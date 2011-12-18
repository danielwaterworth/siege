{-# LANGUAGE ExistentialQuantification, Rank2Types #-}

module DoStore where

import Control.Monad
import Data.Word
import Data.Char
import qualified Data.ByteString as B
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

withRedis :: (forall x. (Redis -> IO x) -> IO x) -> StoreT N.Ref N.Node IO a -> IO a
withRedis redisfn op =
  withRedis' M.empty redisfn op
 where
  withRedis' m redisfn op = do
    step <- runStoreT op
    case step of
      Done a -> return a
      Get k c -> do
        let k' = refLocation k
        v <- case M.lookup k' m of
          Just v ->
            return v
          Nothing -> do
            v <- redisfn (\redis -> R.get redis k')
            case v of
              RBulk (Just v') ->
                return $ (Bin.decode . stToL) v'
              _ ->
                error $ show ("lookup error", k, v)
        withRedis' (M.insert k' v m) redisfn $ c v
      Store v c -> do
        k <- randRef
        let k' = refLocation k
        out <- withRedis' (M.insert k' v m) redisfn $ c k
        let v' = (lToSt . Bin.encode) v
        redisfn (\redis -> do
          R.multi redis
          mapM_ (\ref -> R.sadd redis (invertLocation ref) k') (N.traverse v)
          R.set redis (refLocation k) v'
          R.exec redis :: IO (Reply String))
        return out
