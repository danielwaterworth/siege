module Database.Siege.Hash where

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Digest.Pure.SHA
import Data.Hex
import Data.Char

hash :: L.ByteString -> L.ByteString
hash = bytestringDigest . sha1

stHash :: B.ByteString -> B.ByteString
stHash = B.concat . L.toChunks . hash . L.fromChunks . (\i -> [i])

lbToStr :: L.ByteString -> String
lbToStr = (map (chr . fromIntegral)) . L.unpack

strToLB :: String -> L.ByteString
strToLB = L.pack . (map (fromIntegral . ord))

strHash :: String -> String
strHash = lbToStr . hash . strToLB

hexStrHash :: String -> String
hexStrHash = hex . strHash
