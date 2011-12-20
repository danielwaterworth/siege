{-# LANGUAGE ScopedTypeVariables #-}

module DBTree where

import Data.Maybe
import Data.Word
import qualified Data.ByteString as B
import Data.List
import qualified Data.Enumerator as E

import Data.Set (Set)
import qualified Data.Set as S
import Data.Map (Map)
import qualified Data.Map as M

import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Error
import Store

import DBNode (Ref, Node(..), RawDBOperation, DBError(..))
import qualified DBNode as N

lookup :: Monad m => Ref -> B.ByteString -> RawDBOperation m Ref
lookup ref h =
  if B.null h then
    return ref
  else if N.null ref then
    return N.empty
  else do
    node <- lift $ get ref
    case node of
      Branch options -> do
        let option = find (\(c, _) -> c == B.head h) options in
          case option of
            Just (_, r) -> DBTree.lookup r $ B.tail h
            Nothing -> return N.empty
      Shortcut h' item' -> do
        if h' == h then
          return item'
        else
          return N.empty
      _ ->
        throwError TypeError

insert :: Monad m => Ref -> B.ByteString -> Ref -> RawDBOperation m Ref
insert ref h item =
  if B.null h then
    return item
  else if N.null ref then do
    lift $ store $ Shortcut h item
  else do
    node <- lift $ get ref
    case node of
      Branch options -> do
        let option = find (\(c, _) -> c == B.head h) options in
          case option of
            Just (_, r) -> do
              ref <- DBTree.insert r (B.tail h) item
              let options' = filter (\(c, _) -> c /= B.head h) options
              let options'' = (B.head h, ref):options'
              lift $ store $ Branch options''
            Nothing -> do
              ref <- DBTree.insert N.empty (B.tail h) item
              let options' = (B.head h, ref):options
              lift $ store $ Branch options'
      Shortcut path item' -> do
        if path == h then
          lift $ store $ Shortcut path item
        else
          let construct ah ai bh bi =
                if B.head ah == B.head bh then do
                  b' <- construct (B.tail ah) ai (B.tail bh) bi
                  lift $ store $ Branch [(B.head ah, b')]
                else do
                  a <- DBTree.insert N.empty (B.tail ah) ai
                  b <- DBTree.insert N.empty (B.tail bh) bi
                  lift $ store $ Branch [(B.head ah, a), (B.head bh, b)] in
                    construct path item' h item
      _ ->
        throwError TypeError

-- TODO: collapse a branch and a shortcut to a single shortcut
delete :: Monad m => Ref -> B.ByteString -> RawDBOperation m Ref
delete ref h =
  if B.null h then
    return N.empty
  else if N.null ref then
    return N.empty
  else do
    node <- lift $ get ref
    case node of
      Branch options -> do
        let option = find (\(c, _) -> c == B.head h) options in
          case option of
            Just (_, r) -> do
              ref <- DBTree.delete r (B.tail h)
              let options' = filter (\(c, _) -> c /= B.head h) options
              if N.null ref then
                createBranch options'
              else do
                let options'' = (B.head h, ref):options'
                createBranch options''
            Nothing ->
              return ref
      Shortcut path item -> do
        if path == h then
          return N.empty
        else
          return ref
      _ ->
        throwError TypeError
 where
  createBranch options = do
    if Prelude.null options then
      return N.empty
    else if length options == 1 then do
      let ref = (snd . head) options
      node <- lift $ get ref
      case node of
        Shortcut path item -> do
          let path' = B.cons (fst $ head options) path
          lift $ store $ Shortcut path' item
        _ -> do
          lift $ store $ Branch options
    else
      lift $ store $ Branch options

iterate :: Monad m => Ref -> E.Enumerator Ref (RawDBOperation m) a
iterate =
  iterate' 20
 where
  iterate' n ref s =
    if N.null ref then
      E.returnI s
    else if n == 0 then
      case s of 
        E.Continue c -> (c . E.Chunks) [ref]
        _ -> E.returnI s
    else do
      node <- lift $ lift $ get ref
      case node of
        Branch options ->
          E.concatEnums (map (iterate' (n-1) . snd) options) s
        Shortcut path item ->
          case s of
            E.Continue c -> (c . E.Chunks) [item]
            _ -> E.returnI s
        _ ->
          lift $ throwError TypeError

type Endo v = v -> v
type MergeFn m v = v -> v -> v -> m (Maybe v)

trivialMerge :: (Monad m, Eq v) => MergeFn m v
trivialMerge a b c = return $ 
  if a == b || c == b then
    Just c
  else if a == c then
    Just b
  else
    Nothing

mergeLists :: (Ord k, Monad m) => MergeFn m (Maybe v) -> MergeFn m [(k, v)]
mergeLists fn a b c = do
  let keys = S.toList $ S.fromList (map fst a) `S.union` S.fromList (map fst b) `S.union` S.fromList (map fst c)
  let a' = M.fromList a
  let b' = M.fromList b
  let c' = M.fromList c
  out <- mapM (\key -> do
    let a'' = M.lookup key a'
    let b'' = M.lookup key b'
    let c'' = M.lookup key c'
    v <- fn a'' b'' c''
    return $ fmap (\v' -> (key, v')) v) keys
  return $ fmap (catMaybes . map (\(k, v) -> fmap (\v' -> (k, v')) v)) $ sequence out

merge :: Monad m => MergeFn (RawDBOperation m) (Maybe Ref) -> MergeFn (RawDBOperation m) Ref
merge fn a b c = do
  out <- merge' 20 fn (Just a) (Just b) (Just c)
  case out of
    Nothing -> return Nothing
    Just Nothing -> return $ Just N.empty
    Just (Just v) -> return $ Just v
 where
  merge' :: Monad m => Int -> Endo (MergeFn (RawDBOperation m) (Maybe Ref))
  merge' n fn a b c = do
    o <- trivialMerge a b c
    case o of
      Just v ->
        return o
      Nothing ->
        if n == 0 then
          fn a b c
        else do
          a' <- case a of
            Just a' -> do
              (Branch a'') <- lift $ get a'
              return a''
            Nothing ->
              return []
          b' <- case b of
            Just b' -> do
              (Branch b'') <- lift $ get b'
              return b''
            Nothing ->
              return []
          c' <- case a of
            Just c' -> do
              (Branch c'') <- lift $ get c'
              return c''
            Nothing ->
              return []
          options <- mergeLists (merge' (n-1) fn) a' b' c'
          case options of
            Just options' ->
              if null options' then
                return $ Just Nothing
              else do
                r <- lift $ store $ Branch options'
                return $ Just $ Just r
            Nothing ->
              return Nothing
  getOptions :: Monad m => Ref -> (RawDBOperation m) [(Word8, Ref)]
  getOptions r = do
    undefined
