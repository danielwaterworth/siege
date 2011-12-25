{-# LANGUAGE ExistentialQuantification, Rank2Types #-}

module Database.Siege.IterateeTrans where

import Control.Monad.Trans
import qualified Data.Enumerator as E

changeMonad :: (Monad m0, Monad m1) => (forall x. m0 x -> m1 x) -> E.Iteratee a m0 b -> E.Iteratee a m1 b
changeMonad fn i = E.Iteratee $ do
  v <- (fn . E.runIteratee) i
  case v of
    E.Continue c -> return $ E.Continue (changeMonad fn . c)
    E.Yield b s -> return $ E.Yield b s
    E.Error e -> return $ E.Error e
