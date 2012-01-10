module Database.Siege.ShowTree where

import Prelude hiding (null)
import Data.Nullable

import Data.Word
import qualified Data.ByteString as B
import Control.Monad.Trans.Store
import Database.Siege.DBNode

data Tree =
  Empty |
  TreeBranch [(Word8, Tree)] |
  TreeShortcut B.ByteString Tree |
  TreeStringValue B.ByteString |
  TreeLabel B.ByteString Tree |
  TreeArray [Tree] deriving (Show)

pullTree :: Monad m => Maybe r -> StoreT r (Node r) m Tree
pullTree ref =
  case ref of
    Nothing -> return Empty
    Just ref' -> pullTree' ref'
 where
  pullTree' ref = do
    node <- get ref
    case node of
      Branch options -> do
        options' <- mapM (\(c, r) -> do
          tree <- pullTree' r
          return (c, tree)) options
        return $ TreeBranch options'
      Shortcut h i -> do
        tree <- pullTree' i
        return $ TreeShortcut h tree
      StringValue st ->
        return $ TreeStringValue st
      Label key ref' -> do
        tree <- pullTree' ref'
        return $ TreeLabel key tree
      Array refs -> do
        trees <- mapM pullTree' refs
        return $ TreeArray trees
