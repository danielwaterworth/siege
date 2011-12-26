{-# LANGUAGE ExistentialQuantification, Rank2Types #-}

module Database.Siege.SharedState where

import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Hoist
import Control.Monad.Trans

import Database.Siege.Flushable

data Step s m a =
  Done a |
  forall x. Alter (s -> m (s, x)) (x -> SharedStateT s m a) |
  Get (s -> SharedStateT s m a)

data SharedStateT s m a = SharedStateT {
  runSharedStateT :: m (Step s m a)
}

instance Monad m => Monad (SharedStateT s m) where
  return = SharedStateT . return . Done
  m >>= f = SharedStateT $ do
    v <- runSharedStateT m
    case v of
      Done x -> (runSharedStateT . f) x
      Alter m c -> return $ Alter m (\i -> c i >>= f)
      Get c -> return $ Get (\i -> c i >>= f)

instance MonadTrans (SharedStateT s) where
  lift m = SharedStateT $ liftM Done m

instance MonadIO m => MonadIO (SharedStateT s m) where
  liftIO m = SharedStateT $ liftM Done (liftIO m)

instance MonadHoist (SharedStateT s) where
  hoist f m = SharedStateT $ do
    v <- f $ runSharedStateT m
    case v of
      Done x -> return $ Done x
      Alter m c -> return $ Alter (f . m) $ hoist f . c
      Get c -> return $ Get $ hoist f . c

alter :: Monad m => (s -> m (s, x)) -> SharedStateT s m x
alter = SharedStateT . return . flip Alter return

get :: Monad m => SharedStateT s m s
get = SharedStateT $ return $ Get return

withFVar :: FVar s -> SharedStateT s IO a -> IO a
withFVar var op = do
  v <- runSharedStateT op
  case v of
    Done x -> return x
    Get c -> do
      v <- readFVar var
      withFVar var $ c v
    Alter op c -> do
      merge <- newEmptyMVar
      modifyFVar (\v -> do
        (v', out) <- op v
        return (v', putMVar merge out, ())) var
      out <- takeMVar merge
      withFVar var $ c out
