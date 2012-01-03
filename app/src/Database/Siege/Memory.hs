module Database.Siege.Memory where

import Prelude hiding (null)
import Data.Nullable

import Data.Maybe

import Control.Monad.Identity
import Control.Monad.Trans.Error

import Database.Siege.Store
import Database.Siege.DBNode (Node, RawDBOperation, DBError)

newtype MemoryRef = MemoryRef {
  unRef :: Maybe (Node MemoryRef)
}

instance Nullable MemoryRef where
  empty = MemoryRef Nothing
  null = isNothing . unRef

reduceStore :: (Monad m) => StoreT MemoryRef (Node MemoryRef) m a -> m a
reduceStore op = do
  v <- runStoreT op
  case v of
    Done a -> return a
    Get k c -> (reduceStore . c . fromJust . unRef) k
    Store v c -> (reduceStore . c . MemoryRef . Just) v

testRawDBOperation :: RawDBOperation MemoryRef Identity Bool -> Bool
testRawDBOperation = (== Right True) . runIdentity . reduceStore . runErrorT
