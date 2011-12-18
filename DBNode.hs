module DBNode where

import Store
import Control.Monad.Trans
import Control.Monad.Trans.Maybe
import Data.Word
import qualified Data.ByteString as B

nothing :: Monad m => MaybeT m a
nothing = MaybeT $ return $ Nothing

newtype Ref = Ref {
  unRef :: B.ByteString
} deriving (Read, Show, Eq, Ord)

data Node =
  Branch [(Word8, Ref)] |
  Shortcut B.ByteString Ref |
  --SequenceNode Int Ref Ref |
  Value B.ByteString |
  Label B.ByteString Ref |
  Array [Ref] deriving (Read, Show)

validRef :: Ref -> Bool
validRef = (== 20) . B.length . unRef

empty :: Ref
empty = Ref . B.pack $ take 20 $ repeat 0

null :: Ref -> Bool
null = (== empty)

createValue :: Monad m => B.ByteString -> StoreT Ref Node m Ref
createValue dat =
  store $ Value dat

getValue :: Monad m => Ref -> StoreT Ref Node m (Maybe B.ByteString)
getValue ref =
  if DBNode.null ref then
    return Nothing
  else do
    node <- get ref
    case node of
      Value st -> return $ Just st
      _ -> return Nothing

createLabel :: Monad m => B.ByteString -> Ref -> StoreT Ref Node m Ref
createLabel label ref =
  store $ Label label ref

unlabel :: Monad m => B.ByteString -> Ref -> MaybeT (StoreT Ref Node m) Ref
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

getLabel :: Monad m => Ref -> MaybeT (StoreT Ref Node m) (B.ByteString, Ref)
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
