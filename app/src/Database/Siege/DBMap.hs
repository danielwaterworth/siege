module Database.Siege.DBMap where

import Prelude hiding (null)
import Data.Nullable

import Control.Monad.Trans.Store

import qualified Data.Enumerator as E
import qualified Data.Enumerator.List as EL

import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Error
import Control.Monad.Trans.Maybe

import Data.Maybe
import qualified Data.ByteString as B

import Database.Siege.DBNode (Node(..), RawDBOperation, DBError(..))
import qualified Database.Siege.DBNode as N
import Database.Siege.DBTree as T

import Database.Siege.StringHelper

ident :: B.ByteString
ident = stToB "Map"

insert :: Monad m => Maybe r -> B.ByteString -> r -> RawDBOperation r m r
insert Nothing key item = do
  item' <- N.createLabel key item
  ref <- T.insert Nothing key item'
  N.createLabel ident ref
insert (Just ref) key item = do
  ref' <- N.unlabel ident ref
  item' <- N.createLabel key item
  ref'' <- T.insert (Just ref') key item'
  N.createLabel ident ref''

lookup :: Monad m => Maybe r -> B.ByteString -> RawDBOperation r m (Maybe r)
lookup Nothing key = return Nothing
lookup (Just ref) key = do
  ref' <- N.unlabel ident ref
  ref'' <- T.lookup (Just ref') key
  case ref'' of
    Nothing -> return Nothing
    Just ref''' -> do
      node <- lift $ get ref'''
      case node of
        Label key' ref'''' ->
          if key == key' then
            return $ Just ref''''
          else
            (error . show) ("wooh, key collision ", key, key')
        _ ->
          throwError TypeError

delete :: Monad m => Maybe r -> B.ByteString -> RawDBOperation r m (Maybe r)
delete ref key = runMaybeT $ do
  ref' <- MaybeT $ return ref
  ref'' <- lift $ N.unlabel ident ref'
  ref''' <- MaybeT $ T.delete (Just ref'') key
  lift $ N.createLabel ident ref'''

iterate :: Monad m => Maybe r -> E.Enumerator (B.ByteString, r) (RawDBOperation r m) a
iterate Nothing i = do
  E.returnI i
iterate (Just ref) i = do
  ref' <- lift $ N.unlabel ident ref
  (T.iterate (Just ref') E.$= (EL.mapM N.getLabel)) i
