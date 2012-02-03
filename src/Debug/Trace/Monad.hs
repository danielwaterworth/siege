module Debug.Trace.Monad where

import Debug.Trace as D

traceM :: (Show s, Monad m) => s -> m ()
traceM st = show st `D.trace` return ()
