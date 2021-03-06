module Database.Siege.DBVector where

import Prelude hiding (null)
import Data.Nullable

import Control.Monad.Trans.Store

import qualified Data.Enumerator as E
import qualified Data.Enumerator.List as EL

import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Maybe

import Data.Int
import Data.Maybe
import Data.Char
import qualified Data.ByteString as B

import Database.Siege.DBNode (Ref, Node(..), RawDBOperation, DBError(..))
import qualified Database.Siege.DBNode as N
import Database.Siege.DBTree as T
import Database.Siege.Hash

import Database.Siege.StringHelper

ident = stToB "Vector"

insert :: (Monad m, Nullable r) => r -> Int64 -> r -> RawDBOperation r m r
insert ref key item = do
  ref' <- N.unlabel ident ref
  let key' = stToB $ show key
  let h = stHash key'
  item' <- N.createLabel key' item
  ref'' <- T.insert ref' h item'
  N.createLabel ident ref''

lookup :: (Monad m, Nullable r) => r -> Int64 -> RawDBOperation r m r
lookup ref key = do
  ref' <- N.unlabel ident ref
  let key' = stToB $ show key
  let h = stHash key'
  ref'' <- T.lookup ref' h
  if null ref''
    then
    return empty
    else do
    node <- lift $ get ref''
    case node of
      Label key'' ref''' ->
        if key' == key''
          then
          return ref'''
          else
          (error . show) ("wooh, key collision ", key, key')
      _ ->
        (error . show) ("this shouldn't be here")

delete :: (Monad m, Nullable r) => r -> Int64 -> RawDBOperation r m r
delete ref key = do
  ref' <- N.unlabel ident ref
  let key' = stToB $ show key
  let h = stHash key'
  ref'' <- T.delete ref' h
  if null ref''
    then
    return empty
    else
    N.createLabel ident ref''

iterate :: (Monad m, Nullable r) => r -> E.Enumerator (Int64, r) (RawDBOperation r m) a
iterate ref i = do
  ref' <- lift $ N.unlabel ident ref
  (T.iterate ref' E.$= (EL.concatMapM $ (\r -> do
    v <- N.getLabel r
    return $ maybeToList $ fmap (\(i, r) -> (read $ bToSt i, r)) v))) i
