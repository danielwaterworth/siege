module DBMap where

import Store

import qualified Data.Enumerator as E

import Control.Monad.Trans
import Control.Monad.Trans.Maybe

import Data.Char

import DBNode as N
import DBTree as T
import Hash
import IterateeTrans

insert :: Monad m => Ref -> String -> Ref -> MaybeT (StoreT Ref Node m) Ref
insert ref key item = do
  ref' <- unlabel "Map" ref
  let h = strHash key
  item' <- lift $ createLabel key item
  ref'' <- T.insert ref' (map (fromIntegral . ord) h) item'
  lift $ createLabel "Map" ref''

lookup :: Monad m => Ref -> String -> MaybeT (StoreT Ref Node m) Ref
lookup ref key = do
  ref' <- unlabel "Map" ref
  let h = strHash key
  ref'' <- T.lookup ref' (map (fromIntegral . ord) h)
  if N.null ref'' then
    return N.empty
  else do
    node <- lift $ get ref''
    case node of
      Label key' ref''' ->
        if key == key' then
          return ref'''
        else
          (error . show) ("wooh, key collision ", key, key')
      _ ->
        (error . show) ("this shouldn't be here", node)

delete :: Monad m => Ref -> [Char] -> MaybeT (StoreT Ref Node m) Ref
delete ref key = do
  ref' <- unlabel "Map" ref
  let h = strHash key
  ref'' <- T.delete ref' (map (fromIntegral . ord) h)
  if N.null ref'' then
    return N.empty
  else
    lift $ createLabel "Map" ref''

iterate :: Monad m => Ref -> E.Enumerator (String, Ref) (MaybeT (StoreT Ref Node m)) a
iterate ref i = do
  ref' <- lift $ unlabel "Map" ref
  (T.iterate ref' E.$= (E.mapM N.getLabel)) i
