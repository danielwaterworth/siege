module DBTree where

import Data.Word
import qualified Data.ByteString as B
import Data.List
import qualified Data.Enumerator as E

import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Maybe
import Store

import DBNode as N

lookup :: Monad m => Ref -> B.ByteString -> MaybeT (StoreT Ref Node m) Ref
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
      _ -> nothing

insert :: Monad m => Ref -> B.ByteString -> Ref -> MaybeT (StoreT Ref Node m) Ref
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
              ref <- DBTree.insert empty (B.tail h) item
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
      _ -> nothing

-- TODO: collapse a branch and a shortcut to a single shortcut
delete :: Monad m => Ref -> B.ByteString -> MaybeT (StoreT Ref Node m) Ref
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
      _ -> nothing
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

iterate :: Monad m => Ref -> E.Enumerator Ref (MaybeT (StoreT Ref Node m)) a
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
        _ -> lift $ nothing
