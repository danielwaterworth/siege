module Data.Nullable where

import Prelude hiding (null)

import Data.Maybe
import qualified Data.ByteString as B
import qualified Data.List as List
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map

class Nullable m where
  empty :: m
  null :: m -> Bool

instance Nullable B.ByteString where
  empty = B.empty
  null = B.null

instance Nullable (Maybe m) where
  empty = Nothing
  null = isNothing

instance Nullable (Map k v) where
  empty = Map.empty
  null = Map.null

instance Nullable (Set k) where
  empty = Set.empty
  null = Set.null

instance Nullable [x] where
  empty = []
  null = List.null

reduceMaybe :: (Nullable x) => Maybe x -> x
reduceMaybe Nothing = empty
reduceMaybe (Just x) = x

constructMaybe :: (Nullable x) => x -> Maybe x
constructMaybe x =
  if null x then
    Nothing
  else
    Just x
