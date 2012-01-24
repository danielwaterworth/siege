{-# LANGUAGE Rank2Types #-}

module Database.Siege.EntryPoint where

import Database.Siege.RedisProtocol
import Database.Siege.HeadReference
import Database.Siege.DBNode

import Control.Monad
import Control.Monad.Trans.Store

import Control.Concurrent
import Control.Concurrent.Sync.Channelize

import Network.Socket

listenAt :: Int -> (Socket -> IO ()) -> IO ()
listenAt port act = do
  let port' = toEnum port
  lsock <- socket AF_INET Stream 0
  setSocketOption lsock ReuseAddr 1
  bindSocket lsock $ SockAddrInet port' iNADDR_ANY
  listen lsock 5
  forever $ do
    (sock, _) <- accept lsock
    forkIO $ do
      act sock
      sClose sock

start :: Int -> Maybe r -> (Maybe r -> IO ()) -> (forall a. StoreT r (Node r) IO a -> IO a) -> IO ()
start port start flush store = do
  listenAt port (\sock -> do
    chanpair <- channelize sock
    undefined)
