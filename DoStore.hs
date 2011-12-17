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
import System.Random

randRef :: IO N.Ref
randRef = do
  s <- replicateM 20 $ do
    r <- randomIO :: IO Int
    let r' = fromIntegral r :: Word8
    return $ r'
  return $ B.pack s

refLocation :: B.ByteString -> B.ByteString
refLocation arg = B.snoc arg 0

invertLocation :: B.ByteString -> B.ByteString
invertLocation arg = B.snoc arg 1

withRedis :: (forall x. (Redis -> IO x) -> IO x) -> StoreT N.Ref N.Node IO a -> IO a
withRedis redisfn op =
  withRedis' M.empty redisfn op
 where
  withRedis' m redisfn op = do
    step <- runStoreT op
    case step of
      Done a -> return a
      Get k c -> do
        v <- case M.lookup k m of
          Just v ->
            return v
          Nothing -> do
            v <- redisfn (\redis -> R.get redis $ refLocation k)
            case v of
              RBulk (Just v') ->
                return $ read v'
              _ ->
                error $ show ("lookup error", k, v)
        withRedis' (M.insert k v m) redisfn $ c v
      Store v c -> do
        k <- randRef
        out <- withRedis' (M.insert k v m) redisfn $ c k
        let v' = show v
        redisfn (\redis -> do
          R.multi redis
          mapM_ (\ref -> R.sadd redis (invertLocation ref) k) (N.traverse v)
          R.set redis (refLocation k) v'
          R.exec redis :: IO (Reply String))
        return out
