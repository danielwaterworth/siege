{-# LANGUAGE ExistentialQuantification, Rank2Types #-}
{-# OPTIONS_GHC -Wwarn #-} -- FIXME

module Database.Siege.DBOperation where

import Prelude hiding (null)
import Data.Nullable

import qualified Data.ByteString as B

import Control.Monad
import Control.Monad.Hoist
import Control.Monad.Error

import qualified Data.Enumerator as E
import Data.Enumerator.Hoist

import qualified Database.Siege.DBMap as Map
import qualified Database.Siege.DBSet as Set
import qualified Control.Monad.Trans.Store as S
import Database.Siege.DBNode (Ref, Node, RawDBOperation)
import qualified Database.Siege.DBNode as N

data NodeType =
  Map |
  Set |
  Value deriving Show

data DBOperation r a =
  Done a |

  GetType (Maybe r) (Maybe NodeType -> DBOperation r a) |

  CreateValue B.ByteString (Maybe r -> DBOperation r a) |
  GetValue (Maybe r) (Maybe B.ByteString -> DBOperation r a) |

  MapHas (Maybe r) B.ByteString (Bool -> DBOperation r a) |
  MapLookup (Maybe r) B.ByteString (Maybe r -> DBOperation r a) |
  MapInsert (Maybe r) B.ByteString (Maybe r) (Maybe r -> DBOperation r a) |
  MapDelete (Maybe r) B.ByteString (Maybe r -> DBOperation r a) |
  forall x. MapItems (Maybe r) (E.Iteratee (B.ByteString, r) (DBOperation r) x) (x -> DBOperation r a) |

  SetHas (Maybe r) B.ByteString (Bool -> DBOperation r a) |
  SetInsert (Maybe r) B.ByteString (Maybe r -> DBOperation r a) |
  SetDelete (Maybe r) B.ByteString (Maybe r -> DBOperation r a) |
  forall x. SetItems (Maybe r) (E.Iteratee B.ByteString (DBOperation r) x) (x -> DBOperation r a)

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
      MapItems r it c -> MapItems r it (\i -> c i >>= f)

      SetHas r k c -> SetHas r k (\i -> c i >>= f)
      SetInsert r k c -> SetInsert r k (\i -> c i >>= f)
      SetDelete r k c -> SetDelete r k (\i -> c i >>= f)
      SetItems r it c -> SetItems r it (\i -> c i >>= f)

convert :: Monad m => DBOperation r a -> RawDBOperation r m a
convert (Done x) = return x

convert (GetType r c) =
  case r of
    Nothing -> (convert . c) Nothing
    Just ref -> do
      o <- lift $ S.get ref
      case o of
        (N.StringValue _) -> convert $ c $ Just Value
        (N.Label l _) ->
          if l == Map.ident then
            convert $ c $ Just Map
          else if l == Set.ident then
            convert $ c $ Just Set
          else
            throwError N.TypeError
        _ ->
          throwError N.TypeError

convert (CreateValue v c) = do
  o <- N.createValue v
  (convert . c . Just) o
convert (GetValue r c) = do
  case r of
    Just ref -> do
      o <- N.getValue ref
      convert $ c $ Just o
    Nothing -> do
      convert $ c Nothing

convert (MapHas r k c) = do
  v <- Map.lookup r k
  convert $ c $ not $ null v
convert (MapLookup r k c) = do
  v <- Map.lookup r k
  convert $ c v
convert (MapInsert r k v c) = do
  case v of
    Just v' -> do
      o <- Map.insert r k v'
      convert $ c $ Just o
    Nothing -> do
      o <- Map.delete r k
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
  convert $ c $ Just o
convert (SetDelete r k c) = do
  o <- Set.delete r k
  convert $ c o
convert (SetItems r i c) = do
  o <- E.run_ ((Set.iterate r) E.$$ (hoist convert i))
  convert $ c o
