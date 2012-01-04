{-# LANGUAGE TemplateHaskell #-}

module Tests.DBTree where

import Control.Monad
import Test.QuickCheck.All
import Test.QuickCheck.Arbitrary
import System.Exit

import Database.Siege.DBNode as N
import Database.Siege.DBTree as T
import Database.Siege.Memory

import qualified Data.ByteString as B

-- TODO: pull these out into a new file
instance Arbitrary B.ByteString where
  arbitrary = fmap B.pack arbitrary

instance Arbitrary Node where
  arbitrary = undefined

instance Arbitrary MemoryRef where
  -- either Nothing or fmap Just arbitrary
  arbitrary = undefined

prop_insert_lookup (ref, key, value) = testRawDBOperation $ do
  ref' <- T.insert ref key value
  value' <- T.lookup ref' key
  return $ value == value'

prop_delete_lookup (ref, key) = testRawDBOperation $ do
  ref' <- T.delete ref key
  value <- T.lookup ref' key
  return $ null value

runTests = do
  succeed <- $quickCheckAll
  when (not succeed) exitFailure
