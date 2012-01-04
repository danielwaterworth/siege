{-# OPTIONS_GHC -Wwarn #-} -- I anticipate rewriting this, no point fixing warnings

module Database.Siege.Flushable where

import Control.Concurrent.MVar
import Data.IORef

data FVar a = FVar (MVar a) (IORef (a, a, IO ())) (MVar ())

-- make it possible to fail a flush

newFVar :: s -> IO (FVar s)
newFVar s = do
  m0 <- newMVar s
  m1 <- newIORef (s, s, return ())
  m2 <- newEmptyMVar
  return $ FVar m0 m1 m2

modifyFVar :: (s -> IO (s, IO (), a)) -> FVar s -> IO a
modifyFVar op (FVar m i b) = do
  v <- takeMVar m
  (v', after, out) <- op v
  atomicModifyIORef i (\(_, c, act) -> ((v', c, act >> after), ()))
  putMVar m v'
  tryPutMVar b ()
  return out

readFVar :: FVar s -> IO s
readFVar (FVar _ i _) = do
  (_, v, _) <- readIORef i
  return v

flushFVar :: (s -> IO a) -> FVar s -> IO a
flushFVar op (FVar m i b) = do
  takeMVar b
  (v, act) <- atomicModifyIORef i (\(v, c, act) -> ((v, c, return ()), (v, act)))
  out <- op v
  atomicModifyIORef i (\(v', c, act) -> ((v', v, act), ()))
  act
  return out
