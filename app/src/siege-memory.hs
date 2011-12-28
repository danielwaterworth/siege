{-# LANGUAGE ExistentialQuantification, Rank2Types, DoAndIfThenElse #-}

-- Experimental RAM based backend

import Prelude hiding (null)
import Data.Nullable

import Data.Maybe

import Control.Monad
import Control.Concurrent

import Database.Siege.Flushable
import Database.Siege.NetworkProtocol
import Database.Siege.NetworkHelper

import Database.Siege.Store

import Database.Siege.DBNode (Node)

import Data.Int
import qualified Data.ByteString as B

import Database.Siege.StringHelper

import Database.Siege.Memory

main = do
  var <- newFVar $ MemoryRef Nothing
  forkIO $ forever $ flushFVar (\head -> do
    print "head changed"
    return ()) var
  listenAt 4050 (\sock -> do
    print "new socket [="
    convert protocol sock var reduceStore)
