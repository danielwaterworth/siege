module DBNodeBinary where

import DBNode
import Data.Binary
import Data.Binary.Put
import Data.Binary.Get

instance Binary Ref where
  put r@(Ref v) = 
    if validRef r then
      error "invalid reference"
    else
      putByteString v
  get = do
    v <- getBytes 20
    return $ Ref v

instance Binary Node where
  put (Branch options) = do
    put 'b'
    put options
  put (Shortcut k r) = do
    put 's'
    put k
    put r
  put (Value v) = do
    put 'v'
    put v
  put (Label l r) = do
    put 'l'
    put l
    put r
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
        k <- get
        r <- get
        return $ Shortcut k r
      'v' -> do
        v <- get
        return $ Value v
      'l' -> do
        l <- get
        r <- get
        return $ Label l r
      'a' -> do
        items <- get
        return $ Array items
