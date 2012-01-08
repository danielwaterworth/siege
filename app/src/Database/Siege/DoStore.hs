{-# LANGUAGE ExistentialQuantification, Rank2Types, ImpredicativeTypes #-}

module Database.Siege.DoStore where

import Control.Monad
import Control.Monad.Trans
import Data.Word
import Data.Char
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import qualified Control.Monad.Trans.Store as S
import qualified Database.Redis.Redis as R
import Database.Siege.Hash
import qualified Database.Siege.DBNode as N
import Database.Siege.DBNodeBinary
import qualified Data.Binary as Bin
import System.Random
import Database.Siege.StringHelper

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

get :: [(forall x. (R.Redis -> IO x) -> IO x)] -> B.ByteString -> IO (Maybe B.ByteString)
get fns k = do
  let fn = head fns
  v <- fn (\redis -> R.get redis k)
  case v of
    R.RBulk v -> return v
    _ -> return Nothing

store :: [(forall x. (R.Redis -> IO x) -> IO x)] -> B.ByteString -> B.ByteString -> IO ()
store fns k v = do
  let fn = head fns
  fn (\redis -> R.set redis k v)
  return ()

withRedis :: [(forall x. (R.Redis -> IO x) -> IO x)] -> S.StoreT N.Ref (N.Node N.Ref) IO a -> IO a
withRedis redisfns op = do
  step <- S.runStoreT op
  case step of
    S.Done a -> return a
    S.Get k c -> do
      let k' = refLocation k
      v <- get redisfns k'
      case v of
        (Just v') ->
          (withRedis redisfns . c . Bin.decode . L.fromChunks . (\i -> [i])) v'
        _ ->
          error $ show ("lookup error", k)
    S.Store v c -> do
      k <- randRef
      out <- withRedis redisfns $ c k
      store redisfns (refLocation k) ((B.concat . L.toChunks . Bin.encode) v)
      return out
