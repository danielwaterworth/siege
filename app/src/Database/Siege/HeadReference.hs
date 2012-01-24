module Database.Siege.HeadReference where

import Control.Concurrent
import Control.Concurrent.Sync.Closable
import Control.Concurrent.SampleVar
import Data.IORef

--headReference :: r -> IO (ChanIn (r -> IO r), SampleVar r)
--headReference start = do
--  (i, o) <- newChanPair
--  out <- newSampleVar start

--  let run r = do
--        op <- sync $ readChanOut o
--        r' <- op r
--        writeSampleVar out r'
--        run r'
--  forkIO $ run start

--  return (i, out)
