{-# LANGUAGE ExistentialQuantification, Rank2Types #-}

module Control.Monad.Trans.Store where

import qualified Data.Map as M
import Control.Monad
import Control.Monad.Hoist
import Control.Monad.Trans

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
    step <- runStoreT m
    case step of
      Done x -> runStoreT $ f x
      Get k c -> return $ Get k (\i -> c i >>= f)
      Store v c -> return $ Store v (\i -> c i >>= f)

instance MonadTrans (StoreT k v) where
  lift m = StoreT $ liftM Done m

instance MonadIO m => MonadIO (StoreT k v m) where
  liftIO m = StoreT $ liftM Done (liftIO m)

instance MonadHoist (StoreT k v) where
  hoist f m = StoreT $ do
    step <- f $ runStoreT m
    case step of
      Done a -> return $ Done a
      Get k c -> return $ Get k $ hoist f . c
      Store v c -> return $ Store v $ hoist f . c

get :: Monad m => k -> StoreT k a m a
get k = StoreT $ return $ Get k return

store :: Monad m => v -> StoreT a v m a
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

cache :: (Ord k, Monad m) => StoreT k v m a -> StoreT k v m a
cache op = do
  cache' M.empty op
 where
  cache' m op' = do
    step <- lift $ runStoreT op'
    case step of 
      Done a -> return a
      Get k c -> do
        case M.lookup k m of
          Just v ->
            cache' m $ c v
          Nothing -> do
            v <- get k
            cache' (M.insert k v m) $ c v
      Store v c -> do
        k <- store v
        cache' (M.insert k v m) $ c k
