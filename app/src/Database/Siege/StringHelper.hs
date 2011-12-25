module Database.Siege.StringHelper where

import Data.Char
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L

stToB :: String -> B.ByteString
stToB = B.pack  . map (fromIntegral . ord)

bToSt :: B.ByteString -> String
bToSt = map (chr . fromIntegral) . B.unpack

stToL :: String -> L.ByteString
stToL = bToL . stToB

lToSt :: L.ByteString -> String
lToSt = bToSt . lToB

bToL :: B.ByteString -> L.ByteString
bToL = L.fromChunks . (\i -> [i])

lToB :: L.ByteString -> B.ByteString
lToB = B.concat . L.toChunks
