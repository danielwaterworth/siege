{-# LANGUAGE DoAndIfThenElse #-}

module Database.Siege.DBSet where

import Prelude hiding (null)
import Data.Nullable

import Database.Siege.Store

import qualified Data.Enumerator as E
import qualified Data.Enumerator.List as EL

import Data.Maybe
import Data.Char
import qualified Data.ByteString as B

import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.Error

import Database.Siege.DBNode as N
import Database.Siege.DBTree as T
import Database.Siege.Hash
import Database.Siege.IterateeTrans

import Database.Siege.StringHelper

ident = stToB "Set"

insert ref item = do
  ref' <- unlabel ident ref
  let h = stHash item
  item' <- createValue item
  ref'' <- T.insert ref' h item'
  createLabel ident ref''

delete ref item = do
  ref' <- unlabel ident ref
  let h = stHash item
  ref'' <- T.delete ref' h
  createLabel ident ref''

exists ref item = do
  ref' <- unlabel ident ref
  let h = stHash item
  ref'' <- T.lookup ref' h
  if null ref'' then
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
        throwError TypeError

iterate :: (Monad m, Nullable r) => r -> E.Enumerator B.ByteString (RawDBOperation r m) a
iterate ref i = do
  ref' <- lift $ unlabel ident ref
  (T.iterate ref' E.$= (EL.concatMapM (\r -> liftM maybeToList $ N.getValue r))) i
