import Prelude hiding (null)
import Data.Nullable

import Data.Maybe
import Database.Zookeeper.Core as Z
import Database.Redis.Redis
import Database.Redis.ByteStringClass
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad

import qualified Database.Siege.DBNode as N
import Database.Siege.Flushable
import Database.Siege.DBNode (Ref)
import Database.Siege.NetworkProtocol
import Database.Siege.Connection
import Database.Siege.NetworkHelper
import Database.Siege.StringHelper
import Database.Siege.Store
import Database.Siege.DoStore

initZookeeper = do
  Z.setLogLevel Z.Error
  zk <- Z.init "localhost:2181" Nothing 10000 Nothing
  exists <- Z.exists zk "/head" Nothing
  when (isNothing exists) $ do
    (acl, _) <- Z.getAcl zk "/"
    Z.create zk "/head" (bToSt $ N.unRef empty) [] acl
    return ()
  return zk

initRedis = do
  redis <- connect localhost defaultPort
  newMVar redis

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
    convert protocol sock var (withRedis [withMVar redis] . cache))
