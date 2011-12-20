module DBMap where

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

insert :: Monad m => Ref -> B.ByteString -> Ref -> RawDBOperation m Ref
insert ref key item = do
  ref' <- N.unlabel ident ref
  let h = stHash key
  item' <- N.createLabel key item
  ref'' <- T.insert ref' h item'
  N.createLabel ident ref''

lookup :: Monad m => Ref -> B.ByteString -> RawDBOperation m Ref
lookup ref key = do
  ref' <- N.unlabel ident ref
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

delete :: Monad m => Ref -> B.ByteString -> RawDBOperation m Ref
delete ref key = do
  ref' <- N.unlabel ident ref
  let h = stHash key
  ref'' <- T.delete ref' h
  if N.null ref'' then
    return N.empty
  else
    N.createLabel ident ref''

iterate :: Monad m => Ref -> E.Enumerator (B.ByteString, Ref) (RawDBOperation m) a
iterate ref i = do
  ref' <- lift $ N.unlabel ident ref
  (T.iterate ref' E.$= (EL.mapM N.getLabel)) i
