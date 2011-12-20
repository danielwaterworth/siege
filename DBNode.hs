module DBNode where

import Store
import Control.Monad.Trans
import Control.Monad.Trans.Error
import Data.Word
import qualified Data.ByteString as B

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

data DBError =
  TypeError |
  NullReference |
  OtherError

instance Error DBError where
  noMsg = OtherError

type RawDBOperation m = ErrorT DBError (StoreT Ref Node m)

validRef :: Ref -> Bool
validRef = (== 20) . B.length . unRef

empty :: Ref
empty = Ref . B.pack $ take 20 $ repeat 0

null :: Ref -> Bool
null = (== empty)

createValue :: Monad m => B.ByteString -> RawDBOperation m Ref
createValue dat =
  lift $ store $ Value dat

getValue :: Monad m => Ref -> RawDBOperation m B.ByteString
getValue ref =
  if DBNode.null ref then
    throwError NullReference
  else do
    node <- lift $ get ref
    case node of
      Value st -> return st
      _ -> throwError TypeError

createLabel :: Monad m => B.ByteString -> Ref -> RawDBOperation m Ref
createLabel label ref =
  lift $ store $ Label label ref

unlabel :: Monad m => B.ByteString -> Ref -> RawDBOperation m Ref
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
          throwError TypeError
      _ ->
        throwError TypeError

getLabel :: Monad m => Ref -> RawDBOperation m (B.ByteString, Ref)
getLabel ref = do
  if DBNode.null ref then 
    throwError NullReference
  else do
    node <- lift $ get ref
    case node of 
      Label l r ->
        return (l, r)
      _ ->
        throwError TypeError

traverse :: Node -> [Ref]
traverse (Branch options) = map snd options
traverse (Shortcut _ r) = [r]
traverse (Value _) = []
traverse (Label _ r) = [r]
traverse (Array r) = r
