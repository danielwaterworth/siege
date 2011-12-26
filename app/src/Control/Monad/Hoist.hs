{-# LANGUAGE ExistentialQuantification, RankNTypes #-}
module Control.Monad.Hoist where

class MonadHoist t where
  hoist :: (Monad m, Monad n) => (forall a. m a -> n a) -> t m a -> t n a

