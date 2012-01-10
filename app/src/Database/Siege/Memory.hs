module Database.Siege.Memory where

import Prelude hiding (null)
import Data.Nullable

import Data.Maybe

import Control.Monad.Identity
import Control.Monad.Trans.Error

import Control.Monad.Trans.Store
import Database.Siege.DBNode (Node, RawDBOperation)

data MemoryRef = MemoryRef {
  unRef :: Node MemoryRef
}

reduceStore :: (Monad m) => StoreT MemoryRef (Node MemoryRef) m a -> m a
reduceStore op = do
  step <- runStoreT op
  case step of
    Done a -> return a
    Get k c -> (reduceStore . c . unRef) k
    Store v c -> (reduceStore . c . MemoryRef) v

testRawDBOperation :: RawDBOperation MemoryRef Identity Bool -> Bool
testRawDBOperation = (== Right True) . runIdentity . reduceStore . runErrorT
