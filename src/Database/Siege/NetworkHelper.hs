module Database.Siege.NetworkHelper where

import Control.Monad
import Control.Concurrent
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
