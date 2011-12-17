{-# LANGUAGE ExistentialQuantification, Rank2Types #-}

module NetworkProtocol where

import Data.Maybe
import Data.Char
import qualified Data.ByteString as B
import qualified Data.Enumerator as E
import Data.Map (Map)
import qualified Data.Map as Map
import Control.Monad
import Control.Monad.Identity
import Control.Monad.Trans
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.State as State
import Connection as C
import SharedState as Sh
import Store (StoreT)
import qualified Store as St
import DBNode (Ref, Node, pullTree)
import qualified DBNode as N
import qualified DBMap as M
import DBOperation
import DoStore
import Commands
import StringHelper

import Network.Socket (Socket)
import Database.Redis.Redis (Redis)
import Flushable (FVar)

import TraceHelper

type NetworkOp a = MaybeT (ConnectionT (SharedStateT Ref (StoreT Ref Node Identity))) a

convert :: NetworkOp a -> Socket -> FVar Ref -> (forall x. (Redis -> IO x) -> IO x) -> IO ()
convert op sock var redis =
  let stage1 = (withSocket sock) . C.monadChange stage2
      stage2 = withFVar var . Sh.monadChange stage3
      stage3 = withRedis redis . St.monadChange stage4
      stage4 = return . runIdentity in
        stage1 $ do
          runMaybeT op
          return ()

nothing :: Monad m => MaybeT m a
nothing = MaybeT $ return $ Nothing

maybeRead :: Read a => String -> Maybe a
maybeRead = fmap fst . listToMaybe . reads

applyReversed fn = reverse . fn . reverse

recvCommand :: NetworkOp [B.ByteString]
recvCommand = do
  line <- lift $ recvLine
  if B.null line || B.head line /= (fromIntegral $ ord '*') then do
    nothing
  else do
    n <- MaybeT $ return $ ((maybeRead . applyReversed (drop 2) . tail . bToSt) line :: Maybe Int)
    c <- replicateM n $ do
      line <- lift $ recvLine
      if B.null line || B.head line /= (fromIntegral $ ord '$') then
        nothing
      else do
        m <- MaybeT $ return $ ((maybeRead . applyReversed (drop 2) . tail. bToSt) line :: Maybe Int)
        if m >= 0 then do
          dat <- lift $ recv m
          lift $ recv 2
          return $ Just dat
        else if m == -1 then
          return Nothing
        else
          nothing
    MaybeT $ return $ sequence c

performAlter :: (Ref -> DBOperation Ref (a, Ref)) -> NetworkOp (Maybe a)
performAlter op =
  lift $ lift $ alter $ (\head -> do
    v <- runMaybeT $ DBOperation.convert $ op head
    case v of
      Just (a, r) -> return (r, Just a)
      Nothing -> return (head, Nothing))

performRead :: (Ref -> DBOperation Ref a) -> NetworkOp (Maybe a)
performRead op = do
  head <- lift $ lift Sh.get
  lift $ lift $ lift $ runMaybeT $ DBOperation.convert $ op head

constructList :: Monad m => m (Either (Maybe a) b) -> m (Either [a] b)
constructList act = do
  v <- act
  case v of
    Right v' -> return $ Right v'
    Left Nothing -> return $ Left []
    Left (Just v') -> do
      rest <- constructList act
      case rest of
        Right v' -> return $ Right v'
        Left arr -> return $ Left $ v':arr

protocol :: NetworkOp ()
protocol = flip (>>) (return ()) $ flip runStateT Map.empty $ forever $ do
  c <- lift $ recvCommand
  let com = (map toLower . bToSt . head) c
  let args = tail c
  case (com, args) of
    ("ping", []) ->
      lift $ lift $ sendReply $ StatusReply "PONG"
    ("show", []) -> lift $ do
      ref <- lift $ lift $ Sh.get
      tree <- lift $ lift $ lift $ pullTree ref
      traceM tree
      lift $ sendReply $ StatusReply "OK"
    ("discard", []) -> do
      lift $ lift $ sendReply $ ErrorReply "ERR DISCARD without MULTI"
    ("exec", []) ->
      lift $ lift $ sendReply $ ErrorReply "ERR EXEC without MULTI"
    ("watch", keys) -> do
      items <- lift $ performRead (\head -> mapM (\key -> do
        val <- mapLookup head key
        return (key, val)) keys)
      items' <- lift $ MaybeT $ return items
      mapM (\(key, val) -> do
        m <- State.get
        if not $ Map.member key m then do
          traceM ("inserting", key, val)
          State.put (Map.insert key val m)
        else
          return ()) items'
      lift $ lift $ sendReply $ StatusReply "OK"
    ("unwatch", []) -> do
      State.put Map.empty
      lift $ lift $ sendReply $ StatusReply "OK"
    ("multi", []) -> do
      lift $ lift $ sendReply $ StatusReply "OK"
      commands <- lift $ constructList $ do
        c <- recvCommand
        let com = (map toLower . bToSt . head) c
        let args = tail c
        case (com, args) of
          ("discard", []) -> do
            return $ Right $ StatusReply "OK"
          ("exec", []) -> 
            return $ Left Nothing
          _ -> do
            let c' = command com args in
              case c' of
                Nothing -> do
                  lift $ sendReply $ ErrorReply "unknown command"
                  nothing
                Just _ -> do
                  lift $ sendReply $ StatusReply "QUEUED"
                  return $ Left c'
      case commands of
        Left commands' -> do
          watches <- State.get
          out <- lift $ performAlter $ runStateT $ do
            head <- State.get
            succeed <- foldr (\(key, val) v -> do
              v' <- v
              if v' then do
                ref <- lift $ mapLookup head key
                return $ ref == val
              else
                return False) (return True) (Map.toList watches)
            if succeed then do
              items <- sequence $ map commandToState commands'
              return $ Just items
            else
              return Nothing
          out' <- lift $ MaybeT $ return out
          State.put Map.empty
          lift $ lift $ sendReply $ MultiReply $ out'
        Right rep -> do
          lift $ lift $ sendReply rep
    _ ->
      case command com args of
        Just (Left c') -> lift $ do
          r <- performRead c'
          r' <- MaybeT $ return r
          lift $ sendReply r'
        Just (Right c') -> lift $ do
          r <- performAlter c'
          r' <- MaybeT $ return r
          lift $ sendReply r'
        Nothing ->
          lift $ nothing
