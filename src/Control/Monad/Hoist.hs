{-# LANGUAGE ExistentialQuantification, RankNTypes #-}

module Control.Monad.Hoist where

import Control.Monad.Trans

class (MonadTrans t) => MonadHoist t where
  hoist :: (Monad m, Monad n) => (forall x. m x -> n x) -> t m a -> t n a

