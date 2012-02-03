{-# OPTIONS_GHC -fno-warn-orphans #-}

module Data.Enumerator.Hoist where

import Control.Monad.Hoist
import qualified Data.Enumerator as E

instance MonadHoist (E.Iteratee a) where
  hoist f m = E.Iteratee $ do
    v <- f $ E.runIteratee m
    case v of 
      E.Continue c -> return $ E.Continue ((hoist f) . c)
      E.Yield b s -> return $ E.Yield b s
      E.Error e -> return $ E.Error e
