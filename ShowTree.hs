module ShowTree where

import Prelude hiding (null)
import Nullable

import Data.Word
import qualified Data.ByteString as B
import Store
import DBNode

data Tree =
  Empty |
  TreeBranch [(Word8, Tree)] |
  TreeShortcut B.ByteString Tree |
  TreeValue B.ByteString |
  TreeLabel B.ByteString Tree |
  TreeArray [Tree] deriving (Show)

pullTree :: (Monad m, Nullable r) => r -> StoreT r (Node r) m Tree
pullTree ref =
  if null ref then
    return Empty
  else do
    node <- get ref
    case node of
      Branch options -> do
        options' <- mapM (\(c, r) -> do
          tree <- pullTree r
          return (c, tree)) options
        return $ TreeBranch options'
      Shortcut h i -> do
        tree <- pullTree i
        return $ TreeShortcut h tree
      Value st ->
        return $ TreeValue st
      Label key ref -> do
        tree <- pullTree ref
        return $ TreeLabel key tree
      Array refs -> do
        trees <- mapM pullTree refs
        return $ TreeArray trees
