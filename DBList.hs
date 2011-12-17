module DBList where

import Store
import DBNode as N
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Maybe

head :: Monad m => Ref -> MaybeT (StoreT Ref Node m) Ref
head ref = do
  node <- lift $ get ref
  case node of
    Array [h, _] -> return h
    _ -> nothing

tail :: Monad m => Ref -> MaybeT (StoreT Ref Node m) Ref
tail ref = do
  node <- lift $ get ref
  case node of
    Array [_, t] -> return t
    _ -> nothing

cons :: Monad m => Ref -> Ref -> StoreT Ref Node m Ref
cons a b =
  store $ Array [a, b]

length :: Monad m => Ref -> MaybeT (StoreT Ref Node m) Int
length ref = do
  ref' <- unlabel "List" ref
  length' ref'
 where
  length' ref = do
    if N.null ref then
      return 0
    else do
      ref' <- DBList.tail ref
      l <- DBList.length ref'
      return $ 1 + l

append :: Monad m => Ref -> Ref -> MaybeT (StoreT Ref Node m) Ref
append a b = do
  a' <- unlabel "List" a
  b' <- unlabel "List" b
  ref <- append' a' b'
  lift $ createLabel "List" ref
 where
  append' a b = do
    if N.null a then
      return b
    else do
      h <- DBList.head a
      t <- DBList.tail a
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
