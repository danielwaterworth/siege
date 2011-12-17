module Main where

import Data.Set (Set)
import qualified Data.Set as S
import Database.Redis.Redis (Redis)
import qualified Database.Redis.Redis as R
import Control.Concurrent
import Control.Monad
import DBNode (Ref, Node)
import qualified DBNode as N
import System.Environment

-- find all unreferenced nodes that aren't safe
-- wait for a reasonable amount of time
-- delete them if they are still unreferenced
-- repeat

keys :: Redis -> String -> IO (Maybe [String])
keys redis pattern = do
  out <- R.keys redis pattern
  case out of
    R.RMulti (Just out') -> do
      return $ sequence $ map (\r ->
        case r of
          R.RBulk r' -> r'
          _ -> Nothing) out'
    _ -> return Nothing

findUnreferenced :: Redis -> IO (Set Ref)
findUnreferenced redis = do
  refs <- keys redis "*:ref"
  let refs' = maybe undefined id refs
  refs' <- filterM (\ref -> do
    R.RInt n <- R.scard redis (((reverse . drop 4 . reverse) ref) ++ ":invert")
    return $ n == 0) (maybe undefined id refs)
  return $ (S.fromList $ map (reverse . drop 4 . reverse) refs')

gcOnce :: Redis -> Set Ref -> IO ()
gcOnce redis safe = do
  before <- findUnreferenced redis
  print ("got", before)
  threadDelay 60000000
  after <- findUnreferenced redis
  print ("got", after)
  let gcSet = (after `S.intersection` before) `S.difference` safe
  print ("deleting", gcSet)
  mapM_ (\ref -> do
    R.RBulk (Just node) <- R.get redis (ref ++ ":ref")
    R.multi redis
    R.del redis (ref ++ ":ref")
    R.del redis (ref ++ ":invert")
    mapM_ (\r -> R.srem redis (r ++ ":invert") ref) (N.traverse $ read node)
    R.exec redis :: IO (R.Reply String)) (S.toList gcSet)

main = do
  args <- getArgs
  redis <- R.connect R.localhost R.defaultPort
  gcOnce redis (S.fromList (map read args))
