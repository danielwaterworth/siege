{-# LANGUAGE ExistentialQuantification, Rank2Types #-}

module Database.Siege.DBOperation where

import Prelude hiding (null)
import Data.Nullable

import qualified Data.ByteString as B

import Control.Monad
import Control.Monad.Hoist
import Control.Monad.Trans

import qualified Data.Enumerator as E

import qualified Database.Siege.DBMap as Map
import qualified Database.Siege.DBSet as Set
import qualified Database.Siege.Store as S
import Database.Siege.DBNode (Ref, Node, RawDBOperation)
import qualified Database.Siege.DBNode as N

import Database.Siege.IterateeTrans

data NodeType =
  Map |
  Set |
  Value deriving Show

data DBOperation r a =
  Done a |

  GetType r (Maybe NodeType -> DBOperation r a) |

  CreateValue B.ByteString (r -> DBOperation r a) |
  GetValue r (Maybe B.ByteString -> DBOperation r a) |

  MapHas r B.ByteString (Bool -> DBOperation r a) |
  MapLookup r B.ByteString (r -> DBOperation r a) |
  MapInsert r B.ByteString r (r -> DBOperation r a) |
  MapDelete r B.ByteString (r -> DBOperation r a) |
  forall x. MapItems r (E.Iteratee (B.ByteString, r) (DBOperation r) x) (x -> DBOperation r a) |

  SetHas r B.ByteString (Bool -> DBOperation r a) |
  SetInsert r B.ByteString (r -> DBOperation r a) |
  SetDelete r B.ByteString (r -> DBOperation r a) |
  forall x. SetItems r (E.Iteratee B.ByteString (DBOperation r) x) (x -> DBOperation r a)

getType = flip GetType return

createValue = flip CreateValue return
getValue = flip GetValue return

mapHas r k = MapHas r k return
mapLookup r k = MapLookup r k return
mapInsert r k v = MapInsert r k v return
mapDelete r k = MapDelete r k return
mapItems r i = MapItems r i return

setHas r k = SetHas r k return
setInsert r k = SetInsert r k return
setDelete r k = SetDelete r k return
setItems r i = SetItems r i return

instance Monad (DBOperation r) where
  return = Done
  m >>= f = do
    case m of
      Done x -> f x

      GetType r c -> GetType r (\i -> c i >>= f)

      CreateValue v c -> CreateValue v (\i -> c i >>= f)
      GetValue r c -> GetValue r (\i -> c i >>= f)

      MapHas r k c -> MapHas r k (\i -> c i >>= f)
      MapLookup r k c -> MapLookup r k (\i -> c i >>= f)
      MapInsert r k v c -> MapInsert r k v (\i -> c i >>= f)
      MapDelete r k c -> MapDelete r k (\i -> c i >>= f)
      MapItems r i c -> MapItems r i (\i -> c i >>= f)

      SetHas r k c -> SetHas r k (\i -> c i >>= f)
      SetInsert r k c -> SetInsert r k (\i -> c i >>= f)
      SetDelete r k c -> SetDelete r k (\i -> c i >>= f)
      SetItems r i c -> SetItems r i (\i -> c i >>= f)

convert :: (Monad m, Nullable r) => DBOperation r a -> RawDBOperation r m a
convert (Done x) = return x

convert (GetType r c) =
  if null r then
    convert $ c $ Nothing
  else do
    o <- lift $ S.get r
    case o of
      (N.Value _) -> convert $ c $ Just Value
      (N.Label l _) ->
        if l == Map.ident then
          convert $ c $ Just Map
        else if l == Set.ident then
          convert $ c $ Just Set
        else
          undefined

convert (CreateValue v c) = do
  o <- N.createValue v
  convert $ c o
convert (GetValue r c) = do
  o <- N.getValue r
  convert $ c o

convert (MapHas r k c) = do
  v <- Map.lookup r k
  convert $ c $ not $ null v
convert (MapLookup r k c) = do
  v <- Map.lookup r k
  convert $ c v
convert (MapInsert r k v c) = do
  o <- Map.insert r k v
  convert $ c o
convert (MapDelete r k c) = do
  o <- Map.delete r k
  convert $ c o
convert (MapItems r i c) = do
  o <- E.run_ ((Map.iterate r) E.$$ (hoist convert i))
  convert $ c o

convert (SetHas r k c) = do
  o <- Set.exists r k
  convert $ c o
convert (SetInsert r k c) = do
  o <- Set.insert r k
  convert $ c o
convert (SetDelete r k c) = do
  o <- Set.delete r k
  convert $ c o
convert (SetItems r i c) = do
  o <- E.run_ ((Set.iterate r) E.$$ (hoist convert i))
  convert $ c o
