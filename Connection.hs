{-# LANGUAGE ExistentialQuantification, Rank2Types #-}

module Connection where

import Data.Char
import Data.List
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import qualified Data.Enumerator as E
import qualified IterateeTrans as I
import Control.Monad
import Control.Concurrent
import Control.Monad.Trans
import Network.Socket hiding (recv, send)
import qualified Network.Socket.ByteString as S
import Network.Socket.Enumerator
import TraceHelper

data Step m a =
  Done a |
  Send B.ByteString (ConnectionT m a) |
  forall x. Recv (E.Iteratee B.ByteString m x) (x -> ConnectionT m a)

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
      Recv i c -> return $ Recv i ((flip (>>=) f) . c)

instance MonadTrans (ConnectionT) where
  lift m = ConnectionT $ do
    v <- m
    (return . Done) v

instance MonadIO m => MonadIO (ConnectionT m) where
  liftIO m = ConnectionT $ do
    v <- liftIO m
    (return . Done) v

send :: Monad m => B.ByteString -> ConnectionT m ()
send = ConnectionT . return . flip Send (return ())

recvI :: Monad m => E.Iteratee B.ByteString m x -> ConnectionT m x
recvI = ConnectionT . return . flip Recv return

retry :: Monad m => m (Maybe a) -> m a
retry m = do
  v <- m
  case v of
    Just v' ->
      return v'
    Nothing ->
      retry m

recvChunks :: Monad m => Int -> ConnectionT m [B.ByteString]
recvChunks n =
  recvI $ recv' n
 where
  recv' n = do
    dat <- E.continue return
    case dat of
      E.EOF ->
        error "unexpected EOF - fix me"
      E.Chunks c -> do
        let l = foldl' (+) 0 $ map B.length c
        if null c then do
          error "fix me"
        else if l == n then
          return c
        else if n < l then do
          let l' = L.fromChunks c
          let n' = fromIntegral n
          E.yield (L.toChunks $ L.take n' l') (E.Chunks $ L.toChunks $ L.drop n' l')
        else do
          dat <- recv' (n - l)
          return (c ++ dat)

recv n = do
  dat <- recvChunks n
  return $ B.concat dat

-- TODO: make this more efficient
recvLine :: Monad m => ConnectionT m B.ByteString
recvLine = do
  st <- recv 1
  let st' = map (chr . fromIntegral) $ B.unpack st
  if st' == "\r" then do
    line <- gotcr
    return $ B.append st line
  else do
    line <- recvLine
    return $ B.append st line
 where
  gotcr = do
    st <- recv 1
    let st' = map (chr . fromIntegral) $ B.unpack st
    if st' == "\n" then
      return st
    else if st' == "\r" then do
      line <- gotcr
      return $ B.append st line
    else do
      line <- recvLine
      return $ B.append st line

monadChange :: (Monad m0, Monad m1) => (forall x. m0 x -> m1 x) -> ConnectionT m0 a -> ConnectionT m1 a
monadChange fn op = ConnectionT $ do
  v <- fn $ runConnectionT op
  case v of
    Done x -> return $ Done x
    Send dat c -> return $ Send dat $ monadChange fn c
    Recv n c -> return $ Recv (I.changeMonad fn n) $ monadChange fn . c

withSocket :: Socket -> ConnectionT IO () -> IO ()
withSocket sock op =
  withSocket' [] sock op
 where
  withSocket' input sock op = do
    v <- runConnectionT op
    case v of
      Done a -> return a
      Send dat c -> do
        S.send sock dat
        withSocket' input sock c
      Recv i c -> do
        (x, dat) <- doRecv input sock i
        withSocket' dat sock $ c x
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
