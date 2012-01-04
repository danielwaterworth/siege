module Database.Siege.DBNodeBinary where

import Database.Siege.DBNode

import Data.Binary
import Data.Binary.Put
import Data.Binary.Get
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Control.Monad

instance Binary Ref where
  put r@(Ref v) = 
    if not $ validRef r then
      error $ show ("invalid reference", r)
    else
      putByteString v
  get = do
    v <- getBytes 20
    return $ Ref v

getRemaining :: Get B.ByteString
getRemaining = liftM (B.concat . L.toChunks) getRemainingLazyByteString

instance Binary r => Binary (Node r) where
  put (Branch options) = do
    put 'b'
    put options
  put (Shortcut k r) = do
    put 's'
    put r
    putByteString k
  put (Value v) = do
    put 'v'
    putByteString v
  put (Label l r) = do
    put 'l'
    put r
    putByteString l
  put (Array items) = do
    put 'a'
    put items
  get = do
    v <- get
    case v of
      'b' -> do
        options <- get
        return $ Branch options
      's' -> do
        r <- get
        k <- getRemaining
        return $ Shortcut k r
      'v' -> do
        v <- getRemaining
        return $ Value v
      'l' -> do
        r <- get
        l <- getRemaining
        return $ Label l r
      'a' -> do
        items <- get
        return $ Array items
