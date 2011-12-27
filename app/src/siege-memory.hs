{-# LANGUAGE ExistentialQuantification, Rank2Types, DoAndIfThenElse #-}

-- Experimental RAM based backend

import Prelude hiding (null)
import Data.Nullable

import Data.Maybe

import Control.Concurrent
import Control.Monad
import Control.Monad.Trans.Error
import System.IO

import Database.Siege.StringHelper
import Database.Siege.Flushable
import Database.Siege.NetworkProtocol
import Database.Siege.NetworkHelper

import Database.Siege.Store

import Database.Siege.DBNode (Node)
import Database.Siege.DBNodeBinary

import Data.Int
import Data.Binary as Bin
import qualified Data.ByteString as B

import Database.Siege.StringHelper

newtype MemoryRef = MemoryRef {
  unRef :: Maybe (Node MemoryRef)
}

instance Nullable MemoryRef where
  empty = MemoryRef Nothing
  null = isNothing . unRef

reduceStore :: StoreT MemoryRef (Node MemoryRef) IO a -> IO a
reduceStore op = do
  v <- runStoreT op
  case v of
    Done a -> return a
    Get k c -> do
      reduceStore $ c $ fromJust $ unRef k
    Store v c -> do
      reduceStore $ c $ MemoryRef $ Just v

main = do
  var <- newFVar $ MemoryRef Nothing
  forkIO $ forever $ flushFVar (\head -> do
    print "head changed"
    return ()) var
  listenAt 4050 (\sock -> do
    print "new socket [="
    convert protocol sock var reduceStore)
