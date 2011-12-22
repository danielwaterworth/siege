module DBMap where

import Prelude hiding (null)
import Nullable

import Store

import qualified Data.Enumerator as E
import qualified Data.Enumerator.List as EL

import Control.Monad.Trans
import Control.Monad.Trans.Maybe

import Data.Char
import qualified Data.ByteString as B

import DBNode (Ref, Node(..), RawDBOperation, DBError(..))
import qualified DBNode as N
import DBTree as T
import Hash
import IterateeTrans

ident = B.pack $ map (fromIntegral . ord) "Map"

insert :: (Monad m, Nullable r) => r -> B.ByteString -> r -> RawDBOperation r m r
insert ref key item = do
  ref' <- N.unlabel ident ref
  let h = stHash key
  item' <- N.createLabel key item
  ref'' <- T.insert ref' h item'
  N.createLabel ident ref''

lookup :: (Monad m, Nullable r) => r -> B.ByteString -> RawDBOperation r m r
lookup ref key = do
  ref' <- N.unlabel ident ref
  let h = stHash key
  ref'' <- T.lookup ref' h
  if null ref'' then
    return empty
  else do
    node <- lift $ get ref''
    case node of
      Label key' ref''' ->
        if key == key' then
          return ref'''
        else
          (error . show) ("wooh, key collision ", key, key')
      _ ->
        (error . show) ("this shouldn't be here")

delete :: (Monad m, Nullable r) => r -> B.ByteString -> RawDBOperation r m r
delete ref key = do
  ref' <- N.unlabel ident ref
  let h = stHash key
  ref'' <- T.delete ref' h
  if null ref'' then
    return empty
  else
    N.createLabel ident ref''

iterate :: (Monad m, Nullable r) => r -> E.Enumerator (B.ByteString, r) (RawDBOperation r m) a
iterate ref i = do
  ref' <- lift $ N.unlabel ident ref
  (T.iterate ref' E.$= (EL.mapM N.getLabel)) i
