{-# LANGUAGE TemplateHaskell #-}

module Tests.DBNode where

import Control.Monad
import Test.QuickCheck.All
import Test.QuickCheck.Arbitrary
import System.Exit

import Database.Siege.DBNode
import Database.Siege.Memory

import qualified Data.ByteString as B

instance Arbitrary B.ByteString where
   arbitrary = fmap B.pack arbitrary

prop_create_get_value :: B.ByteString -> Bool
prop_create_get_value a = testRawDBOperation $ do
  a' <- createValue a
  a'' <- getValue a'
  return $ a == a''

runTests :: IO ()
runTests = do
  succeed <- $quickCheckAll
  when (not succeed) exitFailure
