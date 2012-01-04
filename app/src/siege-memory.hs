{-# LANGUAGE ExistentialQuantification, Rank2Types, DoAndIfThenElse #-}

-- Experimental RAM based backend

import Control.Monad
import Control.Concurrent

import Database.Siege.Flushable
import Database.Siege.NetworkProtocol
import Database.Siege.NetworkHelper

import Database.Siege.Memory

main :: IO ()
main = do
  var <- newFVar $ MemoryRef Nothing
  _ <- forkIO $ forever $ flushFVar (const $ print "head changed") var
  listenAt 4050 (\sock -> do
    print "new socket [="
    convert protocol sock var reduceStore)
