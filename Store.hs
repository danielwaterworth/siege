{-# LANGUAGE ExistentialQuantification, Rank2Types #-}

module Store where

import Control.Monad.Trans
import Database.Redis.Redis as R
import Hash

-- The semantics of store should be that it continues as soon as the reference 
-- has been calculated, but that the entire operation shouldn't complete until 
-- all of the store operations have completed.

data Step k v m a =
  Done a |
  Get k (v -> StoreT k v m a) |
  Store v (k -> StoreT k v m a)

data StoreT k v m a = StoreT {
  runStoreT :: m (Step k v m a)
}

instance Monad m => Monad (StoreT k v m) where
  return = StoreT . return . Done
  m >>= f = StoreT $ do
    v <- runStoreT m
    case v of
      Done x -> runStoreT $ f x
      Get k c -> return $ Get k (\i -> c i >>= f)
      Store v c -> return $ Store v (\i -> c i >>= f)

instance MonadTrans (StoreT k v) where
  lift m = StoreT $ do
    v <- m
    return $ Done v

instance MonadIO m => MonadIO (StoreT k v m) where
  liftIO m = StoreT $ do
    v <- liftIO m
    return $ Done v

get k = StoreT $ return $ Get k return
store v = StoreT $ return $ Store v return

keyChange :: Monad m => (k0 -> k1) -> (k1 -> k0) -> StoreT k0 v m a -> StoreT k1 v m a
keyChange forwards backwards op = StoreT $ do
  step <- runStoreT op
  case step of
    Done a -> return $ Done a
    Get k c -> return $ Get (forwards k) (keyChange forwards backwards . c)
    Store v c -> return $ Store v (keyChange forwards backwards . c . backwards)

valueChange :: Monad m => (v0 -> v1) -> (v1 -> v0) -> StoreT k v0 m a -> StoreT k v1 m a
valueChange forwards backwards op = StoreT $ do
  step <- runStoreT op
  case step of
    Done a -> return $ Done a
    Get k c -> return $ Get k (valueChange forwards backwards . c . backwards)
    Store v c -> return $ Store (forwards v) (valueChange forwards backwards . c)

monadChange :: (Monad m0, Monad m1) => (forall x. m0 x -> m1 x) -> StoreT k v m0 a -> StoreT k v m1 a
monadChange fn op = StoreT $ do
  v <- fn $ runStoreT op
  case v of
    Done a -> return $ Done a
    Get k c -> return $ Get k $ monadChange fn . c
    Store v c -> return $ Store v $ monadChange fn . c