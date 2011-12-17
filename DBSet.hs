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

import qualified Data.ByteString as B

ident = B.pack $ map (fromIntegral . ord) "Set"

insert ref item = do
  ref' <- unlabel ident ref
  let h = stHash item
  item' <- lift $ createValue item
  ref'' <- T.insert ref' h item'
  lift $ createLabel ident ref''

delete ref item = do
  ref' <- unlabel ident ref
  let h = stHash item
  ref'' <- T.delete ref' h
  lift $ createLabel ident ref''

exists ref item = do
  ref' <- unlabel ident ref
  let h = stHash item
  ref'' <- T.lookup ref' h
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

iterate :: Monad m => Ref -> E.Enumerator B.ByteString (MaybeT (StoreT Ref Node m)) a
iterate ref i = do
  ref' <- lift $ unlabel ident ref
  (T.iterate ref' E.$= (E.mapM (MaybeT . N.getValue))) i
