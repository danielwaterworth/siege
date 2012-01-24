module Database.Siege.RedisProtocol where

import Data.Word
import qualified Data.ByteString as B

import Control.Pipe

data RedisMessage =
  RBulk B.ByteString |
  RMulti [RedisMessage] |
  RInteger Int |
  RError B.ByteString |
  RStatus B.ByteString

readByte :: Monad m => Pipe B.ByteString b m (Maybe Word8)
readByte = do
  d <- awaitInput
  case d of
    Just d' -> do
      replaceInput $ B.tail d'
      return $ Just $ B.head d'
    Nothing -> do
      return Nothing

redisSerialize :: Monad m => Pipe B.ByteString RedisMessage m ()
redisSerialize = undefined

redisDeserialize :: Monad m => Pipe RedisMessage B.ByteString m ()
redisDeserialize = undefined
