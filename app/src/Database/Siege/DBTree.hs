{-# LANGUAGE ScopedTypeVariables, DoAndIfThenElse #-}

module Database.Siege.DBTree where

import Prelude hiding (null, lookup)
import Data.Nullable

import Data.Maybe
import Data.Word
import qualified Data.ByteString as B
import Data.List hiding (null, lookup, delete, insert)
import qualified Data.Enumerator as E

import Data.Set (Set)
import qualified Data.Set as S
import Data.Map (Map)
import qualified Data.Map as M

import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Error

import Database.Siege.Store

import Database.Siege.DBNode (Ref, Node(..), RawDBOperation, DBError(..))
import qualified Database.Siege.DBNode as N

import Database.Siege.Hash

lookup :: (Monad m, Nullable r) => r -> B.ByteString -> RawDBOperation r m r
lookup ref h =
  lookup' ref (B.unpack $ stHash h)
 where
  lookup' ref h =
    -- if the hash is null, then the leaf has been reached
    if null h then
      return ref
    -- if the reference is null, then the map is empty
    else if null ref then
      return empty
    else do
      node <- lift $ get ref
      case node of
        Branch options -> do
          let option = find (\(c, _) -> c == head h) options in
            case option of
              Just (_, r) -> lookup' r $ tail h
              Nothing -> return empty
        Shortcut h' item' -> do
          if h' == (B.pack h) then
            return item'
          else
            return empty
        _ ->
          throwError TypeError

insert :: (Monad m, Nullable r) => r -> B.ByteString -> r -> RawDBOperation r m r
insert ref h item =
  insert' ref (B.unpack $ stHash h) item
 where
  insert' ref h item =
    if null h then
      return item
    else if null ref then do
      lift $ store $ Shortcut (B.pack h) item
    else do
      node <- lift $ get ref
      case node of
        Branch options -> do
          let option = find (\(c, _) -> c == head h) options in
            case option of
              Just (_, r) -> do
                ref <- insert' r (tail h) item
                let options' = filter (\(c, _) -> c /= head h) options
                let options'' = (head h, ref):options'
                lift $ store $ Branch options''
              Nothing -> do
                ref <- insert' empty (tail h) item
                let options' = (head h, ref):options
                lift $ store $ Branch options'
        Shortcut path item' -> do
          if path == (B.pack h) then
            lift $ store $ Shortcut path item
          else
            let construct ah ai bh bi =
                  if head ah == head bh then do
                    b' <- construct (tail ah) ai (tail bh) bi
                    lift $ store $ Branch [(head ah, b')]
                  else do
                    a <- insert' empty (tail ah) ai
                    b <- insert' empty (tail bh) bi
                    lift $ store $ Branch [(head ah, a), (head bh, b)] in
                      construct (B.unpack path) item' h item
        _ ->
          throwError TypeError

delete :: (Monad m, Nullable r) => r -> B.ByteString -> RawDBOperation r m r
delete ref h =
  delete' ref (B.unpack $ stHash h)
 where
  delete' ref h =
    if null h then
      return empty
    else if null ref then
      return empty
    else do
      node <- lift $ get ref
      case node of
        Branch options -> do
          let option = find (\(c, _) -> c == head h) options in
            case option of
              Just (_, r) -> do
                ref <- delete' r (tail h)
                let options' = filter (\(c, _) -> c /= head h) options
                if null ref then
                  createBranch options'
                else do
                  let options'' = (head h, ref):options'
                  createBranch options''
              Nothing ->
                return ref
        Shortcut path item -> do
          if path == (B.pack h) then
            return empty
          else
            return ref
        _ ->
          throwError TypeError
   where
    createBranch options = do
      if null options then
        return empty
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

iterate :: (Monad m, Nullable r) => r -> E.Enumerator r (RawDBOperation r m) a
iterate =
  iterate' 20
 where
  iterate' n ref s =
    if null ref then
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

merge :: (Monad m, Nullable r, Eq r) => MergeFn (RawDBOperation r m) (Maybe r) -> MergeFn (RawDBOperation r m) r
merge fn a b c = do
  out <- merge' 20 fn (Just a) (Just b) (Just c)
  case out of
    Nothing -> return Nothing
    Just Nothing -> return $ Just empty
    Just (Just v) -> return $ Just v
 where
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
  getOptions :: Monad m => Ref -> (RawDBOperation Ref m) [(Word8, Ref)]
  getOptions r = do
    undefined
