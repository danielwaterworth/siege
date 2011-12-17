{-# LANGUAGE ExistentialQuantification, Rank2Types #-}

module Connection where

import Control.Monad
import Control.Concurrent
import Control.Monad.Trans
import Network.Socket hiding (recv, send)
import qualified Network.Socket as S

data Step m a =
  Done a |
  Send String (ConnectionT m a) |
  Recv Int (String -> ConnectionT m a)

data ConnectionT m a = ConnectionT {
  runConnectionT :: m (Step m a)
}

instance Monad m => Monad (ConnectionT m) where
  return = ConnectionT . return . Done
  m >>= f = ConnectionT $ do
    v <- runConnectionT m
    case v of
      Done x -> (runConnectionT . f) x
      Send s c -> return $ Send s (c >>= f)
      Recv n c -> return $ Recv n ((flip (>>=) f) . c)

instance MonadTrans (ConnectionT) where
  lift m = ConnectionT $ do
    v <- m
    (return . Done) v

instance MonadIO m => MonadIO (ConnectionT m) where
  liftIO m = ConnectionT $ do
    v <- liftIO m
    (return . Done) v

send :: Monad m => String -> ConnectionT m ()
send = ConnectionT . return . flip Send (return ())

recv :: Monad m => Int -> ConnectionT m String
recv = ConnectionT . return . flip Recv return

recvLine :: Monad m => ConnectionT m String
recvLine = do
  st <- recv 1
  if st == "\r" then do
    line <- gotcr
    return $ st ++ line
  else do
    line <- recvLine
    return $ st ++ line
 where
  gotcr = do
    st <- recv 1
    if st == "\n" then
      return st
    else if st == "\r" then do
      line <- gotcr
      return $ st ++ line
    else do
      line <- recvLine
      return $ st ++ line

monadChange :: (Monad m0, Monad m1) => (forall x. m0 x -> m1 x) -> ConnectionT m0 a -> ConnectionT m1 a
monadChange fn op = ConnectionT $ do
  v <- fn $ runConnectionT op
  case v of
    Done x -> return $ Done x
    Send dat c -> return $ Send dat $ monadChange fn c
    Recv n c -> return $ Recv n $ monadChange fn . c

withSocket :: Socket -> ConnectionT IO () -> IO ()
withSocket sock op = do
  v <- runConnectionT op
  case v of
    Done a -> return a
    Send dat c -> do
      S.send sock dat
      withSocket sock c
    Recv n c -> do
      dat <- S.recv sock n
      withSocket sock $ c dat
