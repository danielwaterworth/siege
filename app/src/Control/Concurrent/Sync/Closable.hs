module Control.Concurrent.Sync.Closable where

import Control.Monad
import Control.Concurrent.STM

import Data.IntMap (IntMap)
import qualified Data.IntMap as I

data Event a = Event { events :: [STM (STM (), STM (Maybe a))] }

instance Functor Event where
  fmap f = Event . map (liftM (\a -> (fst a, liftM (fmap f) (snd a)))) . events

choose :: [Event a] -> Event a
choose = Event . concatMap events

sync :: Event a -> IO a
sync ev =
  (atomically . sequence . events) ev >>= (atomically . sync')
 where
  sync' [] = error "empty list"
  sync' [(_, act)] = do
    out <- act
    case out of
      Just out' ->
        return out'
      Nothing -> retry
  sync' ((cancel, act):xs) = do
    out <- act
    case out of
      Just out' -> do
        sequence $ map fst xs
        return out'
      Nothing -> do
        n <- sync' xs
        cancel
        return n

data ChannelState a =
  Empty |
  Closed |
  Reader (Maybe a -> STM ()) |
  Writer (Bool -> STM ()) a
newtype Channel a = Channel (TVar (ChannelState a))

newChan :: IO (Channel a)
newChan = atomically $ liftM Channel $ newTVar Empty

readChan :: Channel a -> Event (Maybe a)
readChan (Channel chan) = Event . (\i -> [i]) $ do
  state <- readTVar chan
  case state of
    Empty -> do
      s <- newEmptyTMVar
      writeTVar chan (Reader (putTMVar s))
      return (writeTVar chan Empty, tryTakeTMVar s)
    Writer a o ->
      return (return (), a True >> (return $ Just $ Just o))
    Reader _ -> error "already reading in another thread"
    Closed -> return (return (), return $ Just Nothing)

writeChan :: Channel a -> a -> Event Bool
writeChan (Channel chan) item = Event . (\i -> [i]) $ do
  state <- readTVar chan
  case state of
    Empty -> do
      s <- newEmptyTMVar
      writeTVar chan (Writer (putTMVar s) item)
      return (writeTVar chan Empty, tryTakeTMVar s)
    Reader c ->
      return (return (), (c . Just) item >> (return $ Just True))
    Closed ->
      return (return (), return $ Just False)
    Writer _ _ -> error "already writing in anothing thread"

closeChan :: Channel a -> Event ()
closeChan (Channel chan) = Event . (\i -> [i]) $ return (return (), do
  state <- readTVar chan
  case state of
    Empty -> do
      writeTVar chan Closed
      return $ Just ()
    Reader c -> do
      c Nothing
      return $ Just ()
    Closed -> return $ Just ()
    Writer a _ -> a True >> (return $ Just ()))

newtype ChanIn a = ChanIn (Channel a)
newtype ChanOut a = ChanOut (Channel a)

newChanPair :: IO (ChanIn a, ChanOut a)
newChanPair = liftM (\c -> (ChanIn c, ChanOut c)) newChan

readChanOut :: ChanOut a -> Event (Maybe a)
readChanOut (ChanOut c) = readChan c

writeChanIn :: ChanIn a -> a -> Event Bool
writeChanIn (ChanIn c) dat = writeChan c dat

closeChanOut :: ChanOut a -> Event ()
closeChanOut (ChanOut c) = closeChan c

closeChanIn :: ChanIn a -> Event ()
closeChanIn (ChanIn c) = closeChan c
