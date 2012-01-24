{-# LANGUAGE ExistentialQuantification, Rank2Types #-}

-- Experimental RAM based backend

import Database.Siege.EntryPoint
import Database.Siege.Memory

main :: IO ()
main = do
  start 4050 Nothing (const $ print "head changed") reduceStore
