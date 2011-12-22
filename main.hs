-- REFACTOR ME

--ghc --make -rtsopts -threaded main.hs -O2

--import Reactor

--import Store as S
--import Database.Redis.Redis

--import Data.Maybe

--import Control.Concurrent
--import Control.Concurrent.MVar

--import DBNode as N
--import DBList as L

--import Zookeeper.Core as Z

--pushHead zk ref = do
--  putStrLn "updating head..."
--  Z.set zk "/head" ref Nothing

--flushThread head zk = do
--  (ref, actions) <- takeMVar head
--  putMVar head (ref, [])
--  if Prelude.null actions then do
--    threadDelay 1000000
--  else do
--    pushHead zk ref
--    mapM_ Prelude.id $ reverse actions
--  flushThread head zk

--alterMVar mvar fn = do
--  v <- takeMVar mvar
--  (v', out) <- fn v
--  putMVar mvar v'
--  return out

--main = do
--  Z.setLogLevel Z.Error
--  zk <- Z.init "localhost:2181" Nothing 10000 Nothing
--  exists <- Z.exists zk "/head" Nothing
--  if isNothing exists then do
--    (acl, _) <- Z.getAcl zk "/"
--    Z.create zk "/head" N.empty [] acl
--    return ()
--  else
--    return ()
--  head <- newMVar (N.empty, [])
--  let alterHead = alterMVar head
--  forkIO $ flushThread head zk
--  redis <- connect localhost defaultPort
--  select redis 4
--  k <- withRedis redis $ valueChange show read $ do
--    ref <- N.createValue "hello world"
--    L.cons ref empty
--  print k

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

main = do
  print "starting..."
  Z.setLogLevel Z.Error
  zk <- Z.init "localhost:2181" Nothing 10000 Nothing
  exists <- Z.exists zk "/head" Nothing
  if isNothing exists then do
    (acl, _) <- Z.getAcl zk "/"
    Z.create zk "/head" (bToSt $ N.unRef empty) [] acl
    return ()
  else
    return ()
  redis <- connect localhost defaultPort
  redisvar <- newMVar redis
  let redis = (\fn -> do
        redis <- takeMVar redisvar
        out <- fn redis
        putMVar redisvar redis
        return out)
  var <- newFVar empty
  forkIO $ forever $ flushFVar (\head -> do
    print ("new head", head)
    Z.set zk "/head" (bToSt $ N.unRef head) Nothing
    return ()) var
  listenAt 4050 (\sock -> do
    print "new socket [="
    convert protocol sock var (withRedis [redis] . cache))

--  redis <- connect localhost defaultPort
--  ping redis
--  True <- rset redis "hello" "world"
--  test <- rget redis "hello" :: IO (Maybe (Maybe String))
--  print test
