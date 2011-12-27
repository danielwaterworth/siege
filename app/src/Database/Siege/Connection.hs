{-# LANGUAGE ExistentialQuantification, Rank2Types, DoAndIfThenElse #-}

module Database.Siege.Connection where

import Data.Char
import Data.List
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import qualified Data.Enumerator as E
import qualified Database.Siege.IterateeTrans as I
import Control.Monad
import Control.Monad.Hoist
import Control.Concurrent
import Control.Monad.Trans
import Network.Socket hiding (recv, send)
import qualified Network.Socket.ByteString as S

import Debug.Trace.Monad

data Step m a =
  Done a |
  Send B.ByteString (ConnectionT m a) |
  forall x. Recv (E.Iteratee B.ByteString m x) (x -> ConnectionT m a) |
  Close

data ConnectionT m a = ConnectionT {
  runConnectionT :: m (Step m a)
}

instance Monad m => Monad (ConnectionT m) where
  return = ConnectionT . return . Done
  m >>= f = ConnectionT $ do
    v <- runConnectionT m
    case v of
      Done x -> runConnectionT (f x)
      Send s c -> return $ Send s (c >>= f)
      Recv i c -> return $ Recv i ((>>= f) . c)
      Close -> return $ Close

instance MonadTrans ConnectionT where
  lift m = ConnectionT $ liftM Done m

instance MonadIO m => MonadIO (ConnectionT m) where
  liftIO m = ConnectionT $ liftM Done (liftIO m)

instance MonadHoist ConnectionT where
  hoist f m = ConnectionT $ do
    v <- f $ runConnectionT m
    case v of
      Done x -> return $ Done x
      Send dat c -> return $ Send dat $ hoist f c
      Recv n c -> return $ Recv (hoist f n) $ hoist f . c

send :: Monad m => B.ByteString -> ConnectionT m ()
send = ConnectionT . return . flip Send (return ())

recvI :: Monad m => E.Iteratee B.ByteString m x -> ConnectionT m x
recvI = ConnectionT . return . flip Recv return

close :: Monad m => ConnectionT m a
close = ConnectionT $ return $ Close

withSocket :: Socket -> ConnectionT IO () -> IO ()
withSocket sock op =
  withSocket' [] sock op
 where
  withSocket' input sock op = do
    v <- runConnectionT op
    case v of
      Done x ->
        return x
      Send dat c -> do
        S.send sock dat
        withSocket' input sock c
      Recv i c -> do
        (x, dat) <- doRecv input sock i
        withSocket' dat sock $ c x
      Close -> do
        sClose sock
  doRecv dat s i = do
    v <- E.runIteratee $ E.enumList 1 dat E.$$ i
    case v of
      E.Continue c -> do
        o <- S.recv s 4096
        if B.null o then
          doRecv [] s $ c (E.EOF)
        else
          doRecv [] s $ c (E.Chunks [o])
      E.Yield o (E.Chunks d) ->
        return (o, d)
      E.Error err -> (error . show) err -- TODO: fixme properly
