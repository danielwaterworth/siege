module Control.Pipe.Channelize where

import Control.Pipe
import Control.Concurrent.Sync.Closable

channelize :: Pipe i o IO a -> IO (ChanIn i, ChanOut o)
channelize p = do
  (ii, io) <- newChanPair
  (oi, oo) <- newChanPair

  let run p' = do
    v <- runPipe p'
    case v of
      AwaitInput c -> do
        d <- sync $ readChanOut io
        run $ c d
      AwaitOutput o c -> do
        d <- sync $ writeChanIn o oi
        run $ c d
      _ -> do
        closeChanIn io
        closeChanOut oi
  forkIO $ run p

  return (ii, oo)
