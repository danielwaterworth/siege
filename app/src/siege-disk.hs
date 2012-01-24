{-# LANGUAGE ExistentialQuantification, Rank2Types #-}

-- Experimental Disk based backend
-- Append only to begin with, at some point I'll make it do log structuring

import System.IO

import Database.Siege.StringHelper

import Control.Monad
import Control.Monad.Trans.Store

import Control.Concurrent.MVar

import Database.Siege.EntryPoint
import Database.Siege.DBNode
import Database.Siege.DBNodeBinary ()

import Data.Binary as Bin
import qualified Data.ByteString as B

import System.Directory

newtype DiskRef = DiskRef Word64 deriving (Eq, Show, Read)

instance Binary DiskRef where
  get = liftM DiskRef Bin.get
  put (DiskRef r) = Bin.put r

getNode :: Handle -> DiskRef -> IO (Node DiskRef)
getNode hnd (DiskRef r) = do
  hSeek hnd AbsoluteSeek $ fromIntegral r
  sz <- B.hGet hnd 8
  v <- B.hGet hnd $ fromIntegral (decode (bToL sz) :: Word64)
  return $ decode $ bToL v

putNode :: Handle -> Node DiskRef -> IO DiskRef
putNode hnd node = do
  hSeek hnd SeekFromEnd 0
  pos <- hTell hnd
  let n' = lToB $ encode node
  let sz = B.length n'
  B.hPut hnd $ lToB $ encode sz
  B.hPut hnd n'
  return $ DiskRef $ fromIntegral pos

diskStoreTransform :: Handle -> StoreT DiskRef (Node DiskRef) IO a -> IO a
diskStoreTransform hnd op = do
  step <- runStoreT op
  case step of
    Done a -> return a
    Get k c -> do
      node <- getNode hnd k
      diskStoreTransform hnd $ c node
    Store v c -> do
      ref <- putNode hnd v
      diskStoreTransform hnd $ c ref

diskStoreTransform' :: (forall x. (Handle -> IO x) -> IO x) -> StoreT DiskRef (Node DiskRef) IO a -> IO a
diskStoreTransform' hnd op = hnd (\hnd' -> diskStoreTransform hnd' op)

main :: IO ()
main =
  withFile "./test.db" ReadWriteMode (\hnd -> do
    hnd' <- newMVar hnd
    headExists <- doesFileExist "head"
    v <- if headExists then
      liftM read $ readFile "head"
     else do
      writeFile "head" $ show (Nothing :: Maybe DiskRef)
      return (Nothing :: Maybe DiskRef)
    let flush ref = do
          print ("new head", ref)
          withMVar hnd' hFlush
            -- TODO: sync to disk, fsync $ handleToFd hnd
          writeFile "head.new" (show ref)
          renameFile "head" "head.old"
          renameFile "head.new" "head"
          removeFile "head.old"
          return ()
    start 4050 v flush (diskStoreTransform' (withMVar hnd')))
