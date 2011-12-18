{-# LANGUAGE ExistentialQuantification, Rank2Types, ImpredicativeTypes #-}

module DoStore where

import Control.Monad
import Control.Monad.Trans
import Data.Word
import Data.Char
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Map (Map)
import qualified Data.Map as M
import Store
import Database.Redis.Redis as R
import Hash
import qualified DBNode as N
import DBNodeBinary
import Data.Binary as Bin
import System.Random
import StringHelper

randRef :: IO N.Ref
randRef = do
  s <- replicateM 20 $ do
    r <- randomIO :: IO Int
    let r' = fromIntegral r :: Word8
    return $ r'
  return $ N.Ref $ B.pack s

refLocation :: N.Ref -> B.ByteString
refLocation arg = B.snoc (N.unRef arg) 0

invertLocation :: N.Ref -> B.ByteString
invertLocation arg = B.snoc (N.unRef arg) 1

cache :: (Ord k, Monad m) => StoreT k v m a -> StoreT k v m a
cache op = do
  cache' M.empty op
 where
  cache' m op = do
    step <- lift $ runStoreT op
    case step of 
      Done a -> return a
      Get k c -> do
        case M.lookup k m of
          Just v ->
            cache' m $ c v
          Nothing -> do
            v <- Store.get k
            cache' (M.insert k v m) $ c v
      Store v c -> do
        k <- Store.store v
        cache' (M.insert k v m) $ c k

get :: [(forall x. (Redis -> IO x) -> IO x)] -> B.ByteString -> IO (Maybe B.ByteString)
get fns k = do
  let fn = head fns
  v <- fn (\redis -> R.get redis k)
  case v of
    RBulk v -> return v
    _ -> return Nothing

store :: [(forall x. (Redis -> IO x) -> IO x)] -> B.ByteString -> B.ByteString -> IO ()
store fns k v = do
  let fn = head fns
  fn (\redis -> R.set redis k v)
  return ()

withRedis :: [(forall x. (Redis -> IO x) -> IO x)] -> StoreT N.Ref N.Node IO a -> IO a
withRedis redisfns op = do
  step <- runStoreT op
  case step of
    Done a -> return a
    Get k c -> do
      let k' = refLocation k
      v <- DoStore.get redisfns k'
      case v of
        (Just v') ->
          (withRedis redisfns . c . Bin.decode . L.fromChunks . (\i -> [i])) v'
        _ ->
          error $ show ("lookup error", k)
    Store v c -> do
      k <- randRef
      out <- withRedis redisfns $ c k
      DoStore.store redisfns (refLocation k) ((B.concat . L.toChunks . Bin.encode) v)
      return out
