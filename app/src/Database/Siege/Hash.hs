module Database.Siege.Hash where

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Digest.Pure.SHA
import Data.Hex
import Data.Char

hash = bytestringDigest . sha1

stHash = B.concat . L.toChunks . hash . L.fromChunks . (\i -> [i])

lbToStr = (map (chr . fromIntegral)) . L.unpack
strToLB = L.pack . (map (fromIntegral . ord))

strHash = lbToStr . hash . strToLB

hexStrHash = hex . strHash
