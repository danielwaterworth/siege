module Commands where

import Prelude hiding (null)
import Nullable

import qualified Data.ByteString as B
import Control.Monad.Trans
import Control.Monad.Trans.State
import Connection
import DBOperation
import DBNode (Ref, Node)
import qualified Data.Enumerator as E
import StringHelper
import TraceHelper

data Reply =
  ErrorReply String |
  StatusReply String |
  IntegerReply Int |
  BulkReply (Maybe String) |
  MultiReply (Maybe [Reply]) deriving Show

globMatch "*" _ = True
globMatch "" "" = True
globMatch "" _ = False
globMatch _ "" = False
globMatch pattern@('*':p:ps) (m:ms) =
  if p == m then
    globMatch ps ms
  else
    globMatch pattern ms
globMatch (p:ps) (m:ms) =
  if p == m then
    globMatch ps ms
  else
    False

readCommand :: (Nullable r) => String -> [B.ByteString] -> Maybe (r -> DBOperation r (Reply))
readCommand "type" [key] = Just (\head -> do
  ref <- mapLookup head key
  ty <- getType ref
  return $ BulkReply $ fmap show ty)
readCommand "get" [key] = Just (\head -> do
  ref <- mapLookup head key
  val <- getValue ref
  return $ BulkReply $ fmap bToSt val)
readCommand "keys" [pattern] = Just (\head -> do
  items <- mapItems head
    (let op = do
          items <- E.continue return
          case items of
            E.EOF ->
              E.yield [] E.EOF
            E.Chunks items' -> do
              let items''' = filter (globMatch (bToSt pattern) . bToSt) $ map fst items'
              items'' <- op
              return $ items''' ++ items'' in
                op)
  return $ MultiReply $ Just $ map (BulkReply . Just) (map bToSt items))
readCommand "hkeys" [key, pattern] = Just (\head -> do
  ref <- mapLookup head key
  items <- mapItems ref
    (let op = do
          items <- E.continue return
          case items of
            E.EOF ->
              E.yield [] E.EOF
            E.Chunks items' -> do
              let items''' = filter (globMatch (bToSt pattern) . bToSt) $ map fst items'
              items'' <- op
              return $ items''' ++ items'' in
                op)
  return $ MultiReply $ Just $ map (BulkReply . Just) (map bToSt items))
readCommand "hgetall" [key] = Just (\head -> do
  ref <- mapLookup head key
  items <- mapItems ref
    (let op = do
          items <- E.continue return
          case items of
            E.EOF ->
              E.yield [] E.EOF
            E.Chunks items' -> do
              items''' <- mapM (\(key, value) -> do
                value' <- lift $ getValue value
                case value' of
                  Just v ->
                    return [key, v]
                  Nothing ->
                    return []) items'
              items'' <- op
              return $ (concat items''') ++ items'' in
                op)
  return $ MultiReply $ Just $ map (BulkReply . Just) (map bToSt items))
readCommand "hget" [key, field] = Just (\head -> do
  ref <- mapLookup head key
  ref' <- mapLookup ref field
  val <- getValue ref'
  return $ BulkReply $ fmap bToSt val)
readCommand "hexists" [key, field] = Just (\head -> do
  ref <- mapLookup head key
  exists <- mapHas ref field
  return $ IntegerReply (if exists then 1 else 0))
readCommand "sismember" [key, field] = Just (\head -> do
  ref <- mapLookup head key
  exists <- setHas ref field
  return $ IntegerReply (if exists then 1 else 0))
readCommand "smembers" [key] = Just (\head -> do
  r <- mapLookup head key
  items <- setItems r
    (let op = do
          keys <- E.continue return
          case keys of
            E.EOF ->
              E.yield [] E.EOF
            E.Chunks keys' -> do
              keys'' <- op
              return $ keys' ++ keys'' in
                op)
  return $ MultiReply $ Just $ map (BulkReply . Just) (map bToSt items))
readCommand "scard" [key] = Just (\head -> do
  r <- mapLookup head key
  n <- setItems r
    (let op = do
          keys <- E.continue return
          case keys of
            E.EOF ->
              E.yield 0 E.EOF
            E.Chunks keys' -> do
              l <- op
              return $ l + (length keys') in
                op)
  return $ IntegerReply n)
readCommand _ _ = Nothing

writeCommand :: (Nullable r) => String -> [B.ByteString] -> Maybe (r -> DBOperation r (Reply, r))
writeCommand "set" [key, val] = Just (\head -> do
  val' <- createValue val
  head' <- mapInsert head key val'
  return (StatusReply "OK", head'))
--writeCommand "incr" [key] = Just (\head -> do
--  writeCommand "incrby" [key, "1"])
--writeCommand "decr" [key] = Just (\head -> do
--  writeCommand "incrby" [key, "-1"])
--writeCommand "incrby" [key] = Just (\head -> do
--  undefined)
writeCommand "del" [key] = Just (\head -> do
  head' <- mapDelete head key
  return (StatusReply "OK", head'))
writeCommand "hset" [key, field, value] = Just (\head -> do
  value' <- createValue value
  ref <- mapLookup head key
  ref' <- mapInsert ref field value'
  head' <- mapInsert head key ref'
  return (StatusReply "OK", head'))
writeCommand "hdel" [key, field] = Just (\head -> do
  ref <- mapLookup head key
  ref' <- mapDelete ref field
  head' <- mapInsert head key ref'
  return (StatusReply "OK", head'))
writeCommand "sadd" [key, field] = Just (\head -> do
  ref <- mapLookup head key
  ref' <- setInsert ref field
  head' <- mapInsert head key ref'
  return (StatusReply "OK", head'))
writeCommand "srem" [key, field] = Just (\head -> do
  ref <- mapLookup head key
  ref' <- setDelete ref field
  head' <- mapInsert head key ref'
  return (StatusReply "OK", head'))
writeCommand _ _ = Nothing

command c0 c1 =
  case (readCommand c0 c1, writeCommand c0 c1) of
    (Just c, _) -> Just (Left c)
    (_, Just c) -> Just (Right c)
    _ -> Nothing

commandToState :: (Nullable r) =>
  Either (r -> DBOperation r Reply) (r -> DBOperation r (Reply, r)) -> StateT r (DBOperation r) Reply
commandToState (Right c) = StateT c
commandToState (Left c) = do
  head <- get
  lift $ c head

sendReply :: Monad m => Reply -> ConnectionT m ()
sendReply (ErrorReply e) = send $ B.concat $ map stToB ["-", e, "\r\n"]
sendReply (StatusReply s) = send $ B.concat $ map stToB ["+", s, "\r\n"]
sendReply (IntegerReply n) = send $ B.concat $ map stToB [":", show n, "\r\n"]
sendReply (BulkReply Nothing) = send $ stToB "$-1\r\n"
sendReply (BulkReply (Just s)) = send $ B.concat $ map stToB ["$", show $ length s, "\r\n", s, "\r\n"]
sendReply (MultiReply (Nothing)) = send $ stToB "*-1\r\n"
sendReply (MultiReply (Just s)) = do
  send $ B.concat $ map stToB ["*", show $ length s, "\r\n"]
  mapM_ sendReply s
