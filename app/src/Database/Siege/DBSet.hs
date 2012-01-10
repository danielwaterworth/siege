module Database.Siege.DBSet where

import Prelude hiding (null)
import Data.Nullable

import Control.Monad.Trans.Store

import qualified Data.Enumerator as E
import qualified Data.Enumerator.List as EL

import Data.Maybe
import qualified Data.ByteString as B

import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Error

import Database.Siege.DBNode as N
import Database.Siege.DBTree as T

import Database.Siege.StringHelper

ident :: B.ByteString
ident = stToB "Set"

insert :: Monad m => Maybe r -> B.ByteString -> RawDBOperation r m r
insert Nothing item = do
  item' <- createValue item
  ref <- T.insert Nothing item item'
  createLabel ident ref
insert (Just ref) item = do
  ref' <- unlabel ident ref
  item' <- createValue item
  ref'' <- T.insert (Just ref') item item'
  createLabel ident ref''

delete :: Monad m => Maybe r -> B.ByteString -> RawDBOperation r m (Maybe r)
delete Nothing item = return Nothing
delete (Just ref) item = do
  ref' <- unlabel ident ref
  ref'' <- T.delete (Just ref') item
  case ref'' of
    Just ref''' -> liftM Just $ createLabel ident ref'''
    Nothing -> return Nothing

exists :: Monad m => Maybe r -> B.ByteString -> RawDBOperation r m Bool
exists Nothing item = return False
exists (Just ref) item = do
  ref' <- unlabel ident ref
  ref'' <- T.lookup (Just ref') item
  case ref'' of
    Nothing -> return False
    Just ref''' -> do
      node <- lift $ get ref'''
      case node of
        StringValue item' ->
          if item == item' then
            return True
          else
            (error . show) ("wooh, key collision ", item, item')
        _ ->
          throwError TypeError

iterate :: Monad m => Maybe r -> E.Enumerator B.ByteString (RawDBOperation r m) a
iterate Nothing i = E.returnI i
iterate (Just ref) i = do
  ref' <- lift $ unlabel ident ref
  (T.iterate (Just ref') E.$= (EL.mapM N.getValue)) i
