module TraceHelper where

import Debug.Trace as D

trace = D.trace

traceM :: (Show s, Monad m) => s -> m ()
traceM st = show st `D.trace` return ()
