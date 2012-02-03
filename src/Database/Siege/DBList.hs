module Database.Siege.DBList where

{-
import Data.Nullable as N
import Database.Siege.DBNode as N
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.Store

head :: Monad m => Ref -> MaybeT (StoreT Ref Node m) Ref
head ref = do
  node <- lift $ get ref
  case node of
    Array [h, _] -> return h
    _ -> empty

tail :: Monad m => Ref -> MaybeT (StoreT Ref Node m) Ref
tail ref = do
  node <- lift $ get ref
  case node of
    Array [_, t] -> return t
    _ -> empty

cons :: Monad m => Ref -> Ref -> StoreT Ref Node m Ref
cons a b =
  store $ Array [a, b]

length :: Monad m => Ref -> MaybeT (StoreT Ref Node m) Int
length ref = do
  ref' <- unlabel "List" ref
  length' ref'
 where
  length' ref = do
    if N.null ref
      then
      return 0
      else do
      ref' <- Database.Siege.DBList.tail ref
      l <- Database.Siege.DBList.length ref'
      return $ 1 + l

append :: Monad m => Ref -> Ref -> MaybeT (StoreT Ref Node m) Ref
append a b = do
  a' <- unlabel "List" a
  b' <- unlabel "List" b
  ref <- append' a' b'
  lift $ createLabel "List" ref
 where
  append' a b = do
    if N.null a
      then
      return b
      else do
      h <- Database.Siege.DBList.head a
      t <- Database.Siege.DBList.tail a
      ref <- append' t b
      lift $ cons h ref

create :: Monad m => [Ref] -> StoreT Ref Node m Ref
create items = do
  ref <- create' items
  createLabel "List" ref
 where
  create' [] = return N.empty
  create' (x:xs) = do
    ref <- create' xs
    cons x ref
-}
