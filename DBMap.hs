module DBMap where

import Store

import qualified Data.Enumerator as E

import Control.Monad.Trans
import Control.Monad.Trans.Maybe

import Data.Char
import qualified Data.ByteString as B

import DBNode as N
import DBTree as T
import Hash
import IterateeTrans

ident = B.pack $ map (fromIntegral . ord) "Map"

insert :: Monad m => Ref -> B.ByteString -> Ref -> MaybeT (StoreT Ref Node m) Ref
insert ref key item = do
  ref' <- unlabel ident ref
  let h = stHash key
  item' <- lift $ createLabel key item
  ref'' <- T.insert ref' h item'
  lift $ createLabel ident ref''

lookup :: Monad m => Ref -> B.ByteString -> MaybeT (StoreT Ref Node m) Ref
lookup ref key = do
  ref' <- unlabel ident ref
  let h = stHash key
  ref'' <- T.lookup ref' h
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

delete :: Monad m => Ref -> B.ByteString -> MaybeT (StoreT Ref Node m) Ref
delete ref key = do
  ref' <- unlabel ident ref
  let h = stHash key
  ref'' <- T.delete ref' h
  if N.null ref'' then
    return N.empty
  else
    lift $ createLabel ident ref''

iterate :: Monad m => Ref -> E.Enumerator (B.ByteString, Ref) (MaybeT (StoreT Ref Node m)) a
iterate ref i = do
  ref' <- lift $ unlabel ident ref
  (T.iterate ref' E.$= (E.mapM N.getLabel)) i
