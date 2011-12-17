module DBSet where

import Store

import qualified Data.Enumerator as E

import Control.Monad.Trans
import Control.Monad.Trans.Maybe

import Data.Char

import DBNode as N
import DBTree as T
import Hash
import IterateeTrans

insert ref item = do
  ref' <- unlabel "Set" ref
  let h = strHash item
  item' <- lift $ createValue item
  ref'' <- T.insert ref' (map (fromIntegral . ord) h) item'
  lift $ createLabel "Set" ref''

delete ref item = do
  ref' <- unlabel "Set" ref
  let h = strHash item
  ref'' <- T.delete ref' (map (fromIntegral . ord) h)
  lift $ createLabel "Set" ref''

exists ref item = do
  ref' <- unlabel "Set" ref
  let h = strHash item
  ref'' <- T.lookup ref' (map (fromIntegral . ord) h)
  if N.null ref'' then
    return False
  else do
    node <- lift $ get ref''
    case node of
      Value item' ->
        if item == item' then
          return True
        else
          (error . show) ("wooh, key collision ", item, item')
      _ ->
        (error . show) ("this shouldn't be here", node)

iterate :: Monad m => Ref -> E.Enumerator String (MaybeT (StoreT Ref Node m)) a
iterate ref i = do
  ref' <- lift $ unlabel "Set" ref
  (T.iterate ref' E.$= (E.mapM (MaybeT . N.getValue))) i
