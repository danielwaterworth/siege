module StringHelper where

import Data.Char
import qualified Data.ByteString as B

stToB :: String -> B.ByteString
stToB = B.pack  . map (fromIntegral . ord)

bToSt :: B.ByteString -> String
bToSt = map (chr . fromIntegral) . B.unpack
