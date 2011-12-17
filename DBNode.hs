module DBNode where

import Store
import Control.Monad.Trans
import Control.Monad.Trans.Maybe
import Data.Word

nothing :: Monad m => MaybeT m a
nothing = MaybeT $ return $ Nothing

type Ref = String

data Node =
  Branch [(Word8, Ref)] |
  Shortcut [Word8] Ref |
  --SequenceNode Int Ref Ref |
  Value String |
  Label String Ref |
  Array [Ref] deriving (Read, Show)

data Tree =
  Empty |
  TreeBranch [(Word8, Tree)] |
  TreeShortcut [Word8] Tree |
  TreeValue String |
  TreeLabel String Tree |
  TreeArray [Tree] deriving (Show)

pullTree :: Monad m => Ref -> StoreT Ref Node m Tree
pullTree ref =
  if DBNode.null ref then
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

validRef :: Ref -> Bool
validRef = (== 20) . length

empty :: Ref
empty = take 20 $ repeat '\x00'

null :: Ref -> Bool
null = (== empty)

createValue :: Monad m => String -> StoreT Ref Node m Ref
createValue dat =
  store $ Value dat

getValue :: Monad m => Ref -> StoreT Ref Node m (Maybe String)
getValue ref =
  if DBNode.null ref then
    return Nothing
  else do
    node <- get ref
    case node of
      Value st -> return $ Just st
      _ -> return Nothing

createLabel :: Monad m => String -> Ref -> StoreT Ref Node m Ref
createLabel label ref =
  store $ Label label ref

unlabel :: Monad m => String -> Ref -> MaybeT (StoreT Ref Node m) Ref
unlabel label ref =
  if DBNode.null ref then
    return ref
  else do
    node <- lift $ get ref
    case node of
      Label label' ref' ->
        if label' == label then
          return ref'
        else
          nothing
      _ -> nothing

getLabel :: Monad m => Ref -> MaybeT (StoreT Ref Node m) (String, Ref)
getLabel ref = do
  if DBNode.null ref then 
    nothing
  else do
    node <- lift $ get ref
    case node of 
      Label l r ->
        return (l, r)
      _ ->
        nothing

traverse :: Node -> [Ref]
traverse (Branch options) = map snd options
traverse (Shortcut _ r) = [r]
traverse (Value _) = []
traverse (Label _ r) = [r]
traverse (Array r) = r
