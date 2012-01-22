module Database.Siege.DBNode where

import Control.Monad.Trans.Store

import Control.Monad.Trans
import Control.Monad.Trans.Error
--import Data.Int
import Data.Word
import qualified Data.ByteString as B

-- TODO: move this into the distributed backend
newtype Ref = Ref {
  unRef :: B.ByteString
} deriving (Read, Show, Eq, Ord)

validRef :: Ref -> Bool
validRef = (== 20) . B.length . unRef

data Node r =
  Branch [(Word8, r)] |
  Shortcut B.ByteString r |
  StringValue B.ByteString |
  Label B.ByteString r |
  Array [r] deriving (Read, Show, Eq)

data DBError =
  TypeError |
  OtherError deriving (Eq)

instance Error DBError where
  noMsg = OtherError

type RawDBOperation r m = ErrorT DBError (StoreT r (Node r) m)

createValue :: Monad m => B.ByteString -> RawDBOperation r m r
createValue dat =
  lift $ store $ StringValue dat

getValue :: Monad m => r -> RawDBOperation r m B.ByteString
getValue ref = do
  node <- lift $ get ref
  case node of
    StringValue st -> return st
    _ -> throwError TypeError

createLabel :: Monad m => B.ByteString -> r -> RawDBOperation r m r
createLabel label ref =
  lift $ store $ Label label ref

unlabel :: Monad m => B.ByteString -> r -> RawDBOperation r m r
unlabel label ref = do
  node <- lift $ get ref
  case node of
    Label label' ref' ->
      if label' == label then
        return ref'
      else
        throwError TypeError
    _ ->
      throwError TypeError

getLabel :: Monad m => r -> RawDBOperation r m (B.ByteString, r)
getLabel ref = do
  node <- lift $ get ref
  case node of 
    Label l r ->
      return (l, r)
    _ ->
      throwError TypeError

traverse :: Node r -> [r]
traverse (Branch options) = map snd options
traverse (Shortcut _ r) = [r]
traverse (StringValue _) = []
traverse (Label _ r) = [r]
traverse (Array r) = r
