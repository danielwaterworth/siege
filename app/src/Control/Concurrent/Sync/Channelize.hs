module Control.Concurrent.Sync.Channelize where

import qualified Data.ByteString as B

import Control.Monad
import Control.Concurrent
import Control.Concurrent.Sync.Closable

import Control.Monad.Trans
import Control.Monad.Trans.Maybe

import Network.Socket (Socket, sClose)
import Network.Socket.ByteString

channelize :: Socket -> IO (ChanIn B.ByteString, ChanOut B.ByteString)
channelize s = do
  (ii, io) <- newChanPair
  (oi, oo) <- newChanPair

  (forkIO . (>> return ()) . runMaybeT . forever) $ do
    d <- lift $ recv s 4096
    if B.null d then do
      lift $ sync $ writeChanIn oi d
     else do
      lift $ sync $ closeChanIn oi
      MaybeT $ return Nothing
  (forkIO . (>> return ()) . runMaybeT . forever) $ do
    d <- lift $ sync $ readChanOut io
    case d of
      Just d' -> do
        -- TODO: look at the output of send
        lift $ send s d'
      Nothing -> do
        lift $ sClose s
        MaybeT $ return Nothing

  return (ii, oo)
