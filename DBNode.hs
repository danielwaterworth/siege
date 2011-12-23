module DBNode where

import Prelude hiding (null)

import Store
import Control.Monad.Trans
import Control.Monad.Trans.Error
import Data.Word
import qualified Data.ByteString as B
import Nullable

newtype Ref = Ref {
  unRef :: B.ByteString
} deriving (Read, Show, Eq, Ord)

instance Nullable Ref where
  empty = Ref . B.pack $ take 20 $ repeat 0
  null = (== empty)

data Node r =
  Branch [(Word8, r)] |
  Shortcut B.ByteString r |
  --SequenceNode Int r r |
  Value B.ByteString |
  Label B.ByteString r |
  Array [r] deriving (Read, Show, Eq)

data DBError =
  TypeError |
  OtherError

instance Error DBError where
  noMsg = OtherError

type RawDBOperation r m = ErrorT DBError (StoreT r (Node r) m)

validRef :: Ref -> Bool
validRef = (== 20) . B.length . unRef

createValue :: (Monad m, Nullable r) => B.ByteString -> RawDBOperation r m r
createValue dat =
  lift $ store $ Value dat

getValue :: (Monad m, Nullable r) => r -> RawDBOperation r m (Maybe B.ByteString)
getValue ref =
  if null ref then
    return Nothing
  else do
    node <- lift $ get ref
    case node of
      Value st -> return $ Just st
      _ -> throwError TypeError

createLabel :: (Monad m, Nullable r) => B.ByteString -> r -> RawDBOperation r m r
createLabel label ref =
  lift $ store $ Label label ref

unlabel :: (Monad m, Nullable r) => B.ByteString -> r -> RawDBOperation r m r
unlabel label ref =
  if null ref then
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

getLabel :: (Monad m, Nullable r) => r -> RawDBOperation r m (Maybe (B.ByteString, r))
getLabel ref = do
  if null ref then 
    return Nothing
  else do
    node <- lift $ get ref
    case node of 
      Label l r ->
        return $ Just (l, r)
      _ ->
        throwError TypeError

traverse :: Node r -> [r]
traverse (Branch options) = map snd options
traverse (Shortcut _ r) = [r]
traverse (Value _) = []
traverse (Label _ r) = [r]
traverse (Array r) = r
