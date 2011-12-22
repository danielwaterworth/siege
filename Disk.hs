-- Experimental Disk based backend
-- Append only to begin with, at some point I'll make it do log structuring

module Disk where

import Prelude hiding (null)
import Nullable

import Store

import DBNode
import DBNodeBinary

import Data.Int
import Data.Binary
import qualified Data.ByteString as B

import System.IO

import StringHelper

type DiskRef = Maybe Word64

getNode :: Handle -> DiskRef -> IO (Node DiskRef)
getNode hnd ref = do
  case ref of
    Just r' -> do
      hSeek hnd AbsoluteSeek $ fromIntegral r'
      sz <- B.hGet hnd 8
      v <- B.hGet hnd $ fromIntegral (decode (bToL sz) :: Word64)
      return $ decode $ bToL v
    Nothing ->
      error "null reference"

putNode :: Handle -> Node DiskRef -> IO DiskRef
putNode hnd node = do
  hSeek hnd SeekFromEnd 0
  pos <- hTell hnd
  let n' = lToB $ encode node
  let sz = B.length n'
  B.hPut hnd $ lToB $ encode sz
  B.hPut hnd n'
  return $ Just $ fromIntegral pos

withHandle :: Handle -> StoreT DiskRef (Node DiskRef) IO a -> IO a
withHandle hnd op = do
  v <- runStoreT op
  case v of
    Done a -> return a
    Get k c -> do
      node <- getNode hnd k
      withHandle hnd $ c node
    Store v c -> do
      ref <- putNode hnd v
      withHandle hnd $ c ref
