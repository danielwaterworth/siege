import Prelude hiding (null)
import Nullable

import Data.Maybe
import Zookeeper.Core as Z
import Database.Redis.Redis
import Database.Redis.ByteStringClass
import Flushable
import DBNode (Ref)
import qualified DBNode as N
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad
import NetworkProtocol
import Connection
import NetworkHelper
import StringHelper
import Store
import DoStore

initZookeeper = do
  Z.setLogLevel Z.Error
  zk <- Z.init "localhost:2181" Nothing 10000 Nothing
  exists <- Z.exists zk "/head" Nothing
  when (isNothing exists) $ do
    (acl, _) <- Z.getAcl zk "/"
    Z.create zk "/head" (bToSt $ N.unRef empty) [] acl
  return zk

initRedis = do
  redis <- connect localhost defaultPort
  redisvar <- newMVar redis
  let redis = (\fn -> do
        redis <- takeMVar redisvar
        out <- fn redis
        putMVar redisvar redis
        return out)
  return redis

main :: IO ()
main = do
  print "starting..."
  zk <- initZookeeper
  redis <- initRedis
  var <- newFVar empty
  forkIO $ forever $ flushFVar (\head -> do
    print ("new head", head)
    Z.set zk "/head" (bToSt $ N.unRef head) Nothing) var
  listenAt 4050 (\sock -> do
    print "new socket [="
    convert protocol sock var (withRedis [redis] . cache))
