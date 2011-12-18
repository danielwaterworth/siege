module StringHelper where

import Data.Char
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L

stToB :: String -> B.ByteString
stToB = B.pack  . map (fromIntegral . ord)

bToSt :: B.ByteString -> String
bToSt = map (chr . fromIntegral) . B.unpack

stToL :: String -> L.ByteString
stToL = L.fromChunks . (\i -> [i]) . stToB

lToSt :: L.ByteString -> String
lToSt = bToSt . B.concat . L.toChunks
