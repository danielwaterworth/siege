{-# INCLUDE <zookeeper.h> #-}
{-# INCLUDE "consts.h" #-}
{-# LINE 1 "Zookeeper/Core.hsc" #-}
{- Copyright 2011, John Billings <john@monkeynut.org>.
{-# LINE 2 "Zookeeper/Core.hsc" #-}
   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are met:

    - Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.

    - Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
   AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
   FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
   DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
   CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
-}

{-# LANGUAGE ForeignFunctionInterface, NamedFieldPuns #-}
{-# LANGUAGE NoImplicitPrelude, TypeSynonymInstances #-}
{-# LANGUAGE EmptyDataDecls, DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ExistentialQuantification #-}

-- | Some documentation talking about the state transitions.
--   <http://hadoop.apache.org/zookeeper/docs/current/zookeeperProgrammers.html#ch_zkSessions>

module Zookeeper.Core
  ( -- * Errors
    ZException(..)
  , SystemError(..)
  , RuntimeInconsistency(..)
  , DataInconsistency(..)
  , ConnectionLoss(..)
  , MarshallingError(..)
  , Unimplemented(..)
  , OperationTimeout(..)
  , BadArguments(..)
  , InvalidState(..)
  , NoNode(..)
  , NoAuth(..)
  , BadVersion(..)
  , NoChildrenForEphemerals(..)
  , NodeExists(..)
  , NotEmpty(..)
  , SessionExpired(..)
  , InvalidCallback(..)
  , InvalidAcl(..)
  , AuthFailed(..)
  , Closing(..)
  , SessionMoved(..)
  , isUnrecoverable
    -- * Logging
  , LogLevel(..), setLogLevel, setLogFile
    -- * Watches
  , State(..), Event(..), Path, Watcher
    -- * Connect
  , Handle, init, setDeterministicConnOrder, getRecvTimeout
    -- * Disconnect
  , close
    -- * Exists
  , Stat(..), exists
    -- * Create
  , CreateFlag(..), create
    -- * Delete
  , delete
    -- * Get
  , get
    -- * Set
  , set, set2
    -- * Children
  , getChildren, getChildren2
    -- * Client IDs
  , ClientId(..), getClientId
    -- * Authentication
  , addAuth
    -- * Access control
  , Id(..)
  , anyoneIdUnsafe, authIds
  , Perm(..), Acl(..)
  , openAclUnsafe, readAclUnsafe, creatorAllAcl
  , getAcl, setAcl
  ) where


{-# LINE 93 "Zookeeper/Core.hsc" #-}

{-# LINE 94 "Zookeeper/Core.hsc" #-}

import Prelude hiding ( id, init )
import Foreign.Ptr
import Foreign.C.String ( CString, withCString, withCStringLen, peekCString
                        , peekCStringLen )
import Foreign.C.Types  ( CInt )
import Foreign.Storable ( pokeByteOff, peekByteOff )
import qualified Foreign.Storable as S
import Foreign.Marshal.Alloc ( allocaBytes )
import Foreign.Marshal.Utils ( copyBytes )
import System.IO.Unsafe ( unsafePerformIO )
import Data.Bits        ( (.|.), (.&.) )
import Data.Int         ( Int32, Int64 )
import Data.Maybe       ( fromMaybe )
import Control.Applicative ( (<$>) )
import Control.Monad ( when, liftM )
import Control.Concurrent.MVar ( newEmptyMVar, putMVar, takeMVar )

import Data.Typeable
import Control.Exception


{-------------------------------------------------------------------------------
  Utilities
 -------------------------------------------------------------------------------}

-- Borrowed from C2HS.hs
cIntConv :: (Integral a, Integral b) => a -> b
cIntConv  = fromIntegral

cToEnum :: (Integral i, Enum e) => i -> e
cToEnum  = toEnum . cIntConv

cFromEnum :: (Enum e, Integral i) => e -> i
cFromEnum  = cIntConv . fromEnum


{-------------------------------------------------------------------------------
  Storable
 -------------------------------------------------------------------------------}

-- TODO Rename to prevent confusion with Foreign.Storable

{- Custom version of storable, where the poke'd pointer must only be used inside
   the supplied IO action.  This allows additional allocation to be performed
   e.g. for variable length strings, with this memory automatically free'ed at
   the end of the action.
 -}
class Storable a where
  sizeOf :: a -> Int
  poke   :: Ptr a -> a -> IO b -> IO b
  peek   :: Ptr a -> IO a

pokeArray :: Storable a => Ptr a -> [a] -> IO b -> IO b
pokeArray _ []     act = act
pokeArray p (x:xs) act =
  poke p x (pokeArray (p `plusPtr` (sizeOf x)) xs act)

peekArray_ :: Storable a => Int -> Ptr a -> [a] -> IO [a]
peekArray_ 0 _ acc = return $ reverse acc
peekArray_ n p acc = do
  x <- peek p
  peekArray_ (pred n) (p `plusPtr` (sizeOf x)) (x:acc)

peekArray :: Storable a => Int -> Ptr a -> IO [a]
peekArray n p = peekArray_ n p []


{-------------------------------------------------------------------------------
  Errors
 -------------------------------------------------------------------------------}

-- | The root of exceptions that may be raised by ZooKeeper.
data ZException = forall e . Exception e => ZException e deriving Typeable

instance Show ZException where
  show (ZException e) = show e

instance Exception ZException

zExceptionToException :: Exception e => e -> SomeException
zExceptionToException = toException . ZException

zExceptionFromException :: Exception e => SomeException -> Maybe e
zExceptionFromException x = do
  ZException a <- fromException x
  cast a

-- | A OS error occurred; check errno.
data SystemError = SystemError deriving (Typeable, Show)
instance Exception SystemError where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | A runtime inconsistency was found.
data RuntimeInconsistency = RuntimeInconsistency deriving (Typeable, Show)
instance Exception RuntimeInconsistency where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | A data inconsistency was found.
data DataInconsistency = DataInconsistency deriving (Typeable, Show)
instance Exception DataInconsistency where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Connection to the server has been lost.
data ConnectionLoss = ConnectionLoss deriving (Typeable, Show)
instance Exception ConnectionLoss where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Error while marshalling or unmarshalling data.
data MarshallingError = MarshallingError deriving (Typeable, Show)
instance Exception MarshallingError where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Operation is unimplemented.
data Unimplemented = Unimplemented deriving (Typeable, Show)
instance Exception Unimplemented where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Operation timeout.
data OperationTimeout = OperationTimeout deriving (Typeable, Show)
instance Exception OperationTimeout where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Invalid arguments.
data BadArguments = BadArguments deriving (Typeable, Show)
instance Exception BadArguments where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Invalid zhandle state.
data InvalidState = InvalidState deriving (Typeable, Show)
instance Exception InvalidState where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Node does not exist.
data NoNode = NoNode deriving (Typeable, Show)
instance Exception NoNode where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Not authenticated.
data NoAuth = NoAuth deriving (Typeable, Show)
instance Exception NoAuth where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Version conflict.
data BadVersion = BadVersion deriving (Typeable, Show)
instance Exception BadVersion where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Ephemeral nodes may not have children.
data NoChildrenForEphemerals = NoChildrenForEphemerals deriving (Typeable, Show)
instance Exception NoChildrenForEphemerals where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | The node already exists.
data NodeExists = NodeExists deriving (Typeable, Show)
instance Exception NodeExists where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | The node has children.
data NotEmpty = NotEmpty deriving (Typeable, Show)
instance Exception NotEmpty where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | The session has been expired by the server.
data SessionExpired = SessionExpired deriving (Typeable, Show)
instance Exception SessionExpired where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Invalid callback specified.
data InvalidCallback = InvalidCallback deriving (Typeable, Show)
instance Exception InvalidCallback where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Invalid ACL specified.
data InvalidAcl = InvalidAcl deriving (Typeable, Show)
instance Exception InvalidAcl where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Client authentication failed.
data AuthFailed = AuthFailed deriving (Typeable, Show)
instance Exception AuthFailed where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | ZooKeeper is closing.
data Closing = Closing deriving (Typeable, Show)
instance Exception Closing where
  toException   = zExceptionToException
  fromException = zExceptionFromException

-- | Session moved to another server, so operation is ignored.
data SessionMoved = SessionMoved deriving (Typeable, Show)
instance Exception SessionMoved where
  toException   = zExceptionToException
  fromException = zExceptionFromException

check :: CInt -> IO ()
check n = case n of
  (0) -> return ()
{-# LINE 311 "Zookeeper/Core.hsc" #-}
  (-1) -> throw SystemError
{-# LINE 312 "Zookeeper/Core.hsc" #-}
  (-2) -> throw RuntimeInconsistency
{-# LINE 313 "Zookeeper/Core.hsc" #-}
  (-3) -> throw DataInconsistency
{-# LINE 314 "Zookeeper/Core.hsc" #-}
  (-4) -> throw ConnectionLoss
{-# LINE 315 "Zookeeper/Core.hsc" #-}
  (-5) -> throw MarshallingError
{-# LINE 316 "Zookeeper/Core.hsc" #-}
  (-6) -> throw Unimplemented
{-# LINE 317 "Zookeeper/Core.hsc" #-}
  (-7) -> throw OperationTimeout
{-# LINE 318 "Zookeeper/Core.hsc" #-}
  (-8) -> throw BadArguments
{-# LINE 319 "Zookeeper/Core.hsc" #-}
  (-9) -> throw InvalidState
{-# LINE 320 "Zookeeper/Core.hsc" #-}
  (-101) -> throw NoNode
{-# LINE 321 "Zookeeper/Core.hsc" #-}
  (-102) -> throw NoAuth
{-# LINE 322 "Zookeeper/Core.hsc" #-}
  (-103) -> throw BadVersion
{-# LINE 323 "Zookeeper/Core.hsc" #-}
  (-108) -> throw NoChildrenForEphemerals
{-# LINE 324 "Zookeeper/Core.hsc" #-}
  (-110) -> throw NodeExists
{-# LINE 325 "Zookeeper/Core.hsc" #-}
  (-111) -> throw NotEmpty
{-# LINE 326 "Zookeeper/Core.hsc" #-}
  (-112) -> throw SessionExpired
{-# LINE 327 "Zookeeper/Core.hsc" #-}
  (-113) -> throw InvalidCallback
{-# LINE 328 "Zookeeper/Core.hsc" #-}
  (-114) -> throw InvalidAcl
{-# LINE 329 "Zookeeper/Core.hsc" #-}
  (-115) -> throw AuthFailed
{-# LINE 330 "Zookeeper/Core.hsc" #-}
  (-116) -> throw Closing
{-# LINE 331 "Zookeeper/Core.hsc" #-}
  (-118) -> throw SessionMoved
{-# LINE 332 "Zookeeper/Core.hsc" #-}
  _                                 -> error $ "Cannot match " ++ show n

ifOk :: CInt -> IO a -> IO a
ifOk cRes actOk =
  check cRes >> actOk


{-------------------------------------------------------------------------------
  Log levels
 -------------------------------------------------------------------------------}

data LogLevel = Error
              | Warn
              | Info
              | Debug
                deriving ( Eq, Show )

instance Enum LogLevel where
  fromEnum Error = 1
{-# LINE 351 "Zookeeper/Core.hsc" #-}
  fromEnum Warn =  2
{-# LINE 352 "Zookeeper/Core.hsc" #-}
  fromEnum Info =  3
{-# LINE 353 "Zookeeper/Core.hsc" #-}
  fromEnum Debug = 4
{-# LINE 354 "Zookeeper/Core.hsc" #-}

  toEnum (1) = Error
{-# LINE 356 "Zookeeper/Core.hsc" #-}
  toEnum (2) = Warn
{-# LINE 357 "Zookeeper/Core.hsc" #-}
  toEnum (3) = Info
{-# LINE 358 "Zookeeper/Core.hsc" #-}
  toEnum (4) = Debug
{-# LINE 359 "Zookeeper/Core.hsc" #-}
  toEnum unmatched = error ("LogLevel.toEnum: Cannot match " ++ show unmatched)


{-------------------------------------------------------------------------------
  Stats
 -------------------------------------------------------------------------------}

-- |Node status information.
data Stat = Stat
  { -- |The zxid of the change that caused this znode to be created.
    czxid          :: Int64
    -- |The zxid of the change that last modified this znode.
  , mzxid          :: Int64
    -- |The time in milliseconds from epoch when this znode was created.
  , ctime          :: Int64
    -- |The time in milliseconds from epoch when this znode was last modified.
  , mtime          :: Int64
    -- |The number of changes to the data of this znode.
  , version        :: Int32
    -- |The number of changes to the children of this znode.
  , cversion       :: Int32
    -- |The number of changes to the ACL of this znode.
  , aversion       :: Int32
    -- |The session id of the owner of this znode if the znode is an ephemeral
    -- node. If it is not an ephemeral node, it will be zero.
  , ephemeralOwner :: Int64
    -- |The length of the data field of this znode.
  , dataLength     :: Int32
    -- |The number of children of this znode.
  , numChildren    :: Int32
    -- |Unknown.
  , pzxid          :: Int64
  } deriving ( Eq, Show )

instance Storable Stat where
  sizeOf _ = (72)
{-# LINE 395 "Zookeeper/Core.hsc" #-}
  poke p (Stat { czxid, mzxid, ctime, mtime, version, cversion, aversion
               , ephemeralOwner, dataLength, numChildren, pzxid }) act = do
    (\hsc_ptr -> pokeByteOff hsc_ptr 0) p czxid
{-# LINE 398 "Zookeeper/Core.hsc" #-}
    (\hsc_ptr -> pokeByteOff hsc_ptr 8) p mzxid
{-# LINE 399 "Zookeeper/Core.hsc" #-}
    (\hsc_ptr -> pokeByteOff hsc_ptr 16) p ctime
{-# LINE 400 "Zookeeper/Core.hsc" #-}
    (\hsc_ptr -> pokeByteOff hsc_ptr 24) p mtime
{-# LINE 401 "Zookeeper/Core.hsc" #-}
    (\hsc_ptr -> pokeByteOff hsc_ptr 32) p version
{-# LINE 402 "Zookeeper/Core.hsc" #-}
    (\hsc_ptr -> pokeByteOff hsc_ptr 36) p cversion
{-# LINE 403 "Zookeeper/Core.hsc" #-}
    (\hsc_ptr -> pokeByteOff hsc_ptr 40) p aversion
{-# LINE 404 "Zookeeper/Core.hsc" #-}
    (\hsc_ptr -> pokeByteOff hsc_ptr 48) p ephemeralOwner
{-# LINE 405 "Zookeeper/Core.hsc" #-}
    (\hsc_ptr -> pokeByteOff hsc_ptr 56) p dataLength
{-# LINE 406 "Zookeeper/Core.hsc" #-}
    (\hsc_ptr -> pokeByteOff hsc_ptr 60) p numChildren
{-# LINE 407 "Zookeeper/Core.hsc" #-}
    (\hsc_ptr -> pokeByteOff hsc_ptr 64) p pzxid
{-# LINE 408 "Zookeeper/Core.hsc" #-}
    act
  peek p = do
    czxid          <- (\hsc_ptr -> peekByteOff hsc_ptr 0) p
{-# LINE 411 "Zookeeper/Core.hsc" #-}
    mzxid          <- (\hsc_ptr -> peekByteOff hsc_ptr 8) p
{-# LINE 412 "Zookeeper/Core.hsc" #-}
    ctime          <- (\hsc_ptr -> peekByteOff hsc_ptr 16) p
{-# LINE 413 "Zookeeper/Core.hsc" #-}
    mtime          <- (\hsc_ptr -> peekByteOff hsc_ptr 24) p
{-# LINE 414 "Zookeeper/Core.hsc" #-}
    version        <- (\hsc_ptr -> peekByteOff hsc_ptr 32) p
{-# LINE 415 "Zookeeper/Core.hsc" #-}
    cversion       <- (\hsc_ptr -> peekByteOff hsc_ptr 36) p
{-# LINE 416 "Zookeeper/Core.hsc" #-}
    aversion       <- (\hsc_ptr -> peekByteOff hsc_ptr 40) p
{-# LINE 417 "Zookeeper/Core.hsc" #-}
    ephemeralOwner <- (\hsc_ptr -> peekByteOff hsc_ptr 48) p
{-# LINE 418 "Zookeeper/Core.hsc" #-}
    dataLength     <- (\hsc_ptr -> peekByteOff hsc_ptr 56) p
{-# LINE 419 "Zookeeper/Core.hsc" #-}
    numChildren    <- (\hsc_ptr -> peekByteOff hsc_ptr 60) p
{-# LINE 420 "Zookeeper/Core.hsc" #-}
    pzxid          <- (\hsc_ptr -> peekByteOff hsc_ptr 64) p
{-# LINE 421 "Zookeeper/Core.hsc" #-}
    return $ Stat { czxid, mzxid, ctime, mtime, version, cversion, aversion
                  , ephemeralOwner, dataLength, numChildren, pzxid }


{-------------------------------------------------------------------------------
  Strings
 -------------------------------------------------------------------------------}

instance Storable String where
  sizeOf _ = (8)
{-# LINE 431 "Zookeeper/Core.hsc" #-}
  poke p str act =
    withCString str $ \q -> do
    S.poke (castPtr p :: Ptr CString) q
    act
  peek p = do
    q <- S.peek (castPtr p :: Ptr CString)
    peekCString q


{-------------------------------------------------------------------------------
  IDs
 -------------------------------------------------------------------------------}

data Id = Id
  { scheme :: String
  , id     :: String
  } deriving ( Eq, Show )

instance Storable Id where
  sizeOf _ = (16)
{-# LINE 451 "Zookeeper/Core.hsc" #-}
  poke p (Id { scheme, id }) act =
    poke ((\hsc_ptr -> hsc_ptr `plusPtr` 0) p) scheme $
{-# LINE 453 "Zookeeper/Core.hsc" #-}
    poke ((\hsc_ptr -> hsc_ptr `plusPtr` 8) p) id     $
{-# LINE 454 "Zookeeper/Core.hsc" #-}
    act
  peek p = do
    scheme <- peek ((\hsc_ptr -> hsc_ptr `plusPtr` 0) p)
{-# LINE 457 "Zookeeper/Core.hsc" #-}
    id     <- peek ((\hsc_ptr -> hsc_ptr `plusPtr` 8) p)
{-# LINE 458 "Zookeeper/Core.hsc" #-}
    return $ Id { scheme, id }

foreign import ccall "consts.h const_anyone_id_unsafe"
  const_anyone_id_unsafe :: Ptr Id

-- | This Id represents anyone.
anyoneIdUnsafe :: Id
anyoneIdUnsafe = unsafePerformIO $ peek (const_anyone_id_unsafe)

foreign import ccall "consts.h const_auth_ids"
  const_auth_ids :: Ptr Id

-- | This Id is only usable to set ACLs. It will get substituted with the
--   Id's the client authenticated with.
authIds :: Id
authIds = unsafePerformIO $ peek (const_auth_ids)


{-------------------------------------------------------------------------------
  ACL permissions
 -------------------------------------------------------------------------------}

data Perm = Read
             | Write
             | Create
             | Delete
             | Admin
             | All
               deriving ( Eq, Show, Bounded )

instance Enum Perm where
  fromEnum Read   = 1
{-# LINE 490 "Zookeeper/Core.hsc" #-}
  fromEnum Write  = 2
{-# LINE 491 "Zookeeper/Core.hsc" #-}
  fromEnum Create = 4
{-# LINE 492 "Zookeeper/Core.hsc" #-}
  fromEnum Delete = 8
{-# LINE 493 "Zookeeper/Core.hsc" #-}
  fromEnum Admin  = 16
{-# LINE 494 "Zookeeper/Core.hsc" #-}
  fromEnum All    = 31
{-# LINE 495 "Zookeeper/Core.hsc" #-}

  toEnum (1) = Read
{-# LINE 497 "Zookeeper/Core.hsc" #-}
  toEnum (2) = Write
{-# LINE 498 "Zookeeper/Core.hsc" #-}
  toEnum (4) = Create
{-# LINE 499 "Zookeeper/Core.hsc" #-}
  toEnum (8) = Delete
{-# LINE 500 "Zookeeper/Core.hsc" #-}
  toEnum (16) = Admin
{-# LINE 501 "Zookeeper/Core.hsc" #-}
  toEnum (31) = All
{-# LINE 502 "Zookeeper/Core.hsc" #-}
  toEnum unmatched = error ("Perm.toEnum: Cannot match " ++ show unmatched)


{-------------------------------------------------------------------------------
  ACLs
 -------------------------------------------------------------------------------}

data Acl = Acl
  { perms :: [Perm]
  , aclId :: Id
  } deriving ( Eq, Show )

instance Storable Acl where
  sizeOf _ = (24)
{-# LINE 516 "Zookeeper/Core.hsc" #-}
  poke p (Acl { perms, aclId }) act = do
    let cPerms = foldr (.|.) 0 $ cFromEnum <$> perms :: CInt
    (\hsc_ptr -> pokeByteOff hsc_ptr 0) p cPerms
{-# LINE 519 "Zookeeper/Core.hsc" #-}
    poke ((\hsc_ptr -> hsc_ptr `plusPtr` 8) p) aclId act
{-# LINE 520 "Zookeeper/Core.hsc" #-}
  peek p = do
    cPerms <- (\hsc_ptr -> peekByteOff hsc_ptr 0) p :: IO Int
{-# LINE 522 "Zookeeper/Core.hsc" #-}
    let allPerms = [ Read, Write, Create
                   , Delete, Admin, All ]
        perms    = filter (\q -> (fromEnum q) .&. cPerms == (fromEnum q)) allPerms
    aclId <- peek $ (\hsc_ptr -> hsc_ptr `plusPtr` 8) p
{-# LINE 526 "Zookeeper/Core.hsc" #-}
    return $ Acl { perms, aclId }


{-------------------------------------------------------------------------------
  ACL vectors
 -------------------------------------------------------------------------------}

instance Storable [Acl] where
  sizeOf _ = (16)
{-# LINE 535 "Zookeeper/Core.hsc" #-}
  poke p acls act = do
    (\hsc_ptr -> pokeByteOff hsc_ptr 0) p (fromIntegral $ length acls :: Int32)
{-# LINE 537 "Zookeeper/Core.hsc" #-}
    allocaBytes (length acls * sizeOf (undefined :: Acl)) $ \q -> do
    (\hsc_ptr -> pokeByteOff hsc_ptr 8) p q
{-# LINE 539 "Zookeeper/Core.hsc" #-}
    pokeArray q acls act
  peek p = do
    count <- (\hsc_ptr -> peekByteOff hsc_ptr 0) p :: IO Int32
{-# LINE 542 "Zookeeper/Core.hsc" #-}
    q <- (\hsc_ptr -> peekByteOff hsc_ptr 8) p
{-# LINE 543 "Zookeeper/Core.hsc" #-}
    peekArray (fromIntegral count) q

foreign import ccall "consts.h const_open_acl_unsafe"
  const_open_acl_unsafe :: Ptr [Acl]

openAclUnsafe :: [Acl]
openAclUnsafe = unsafePerformIO $ peek (const_open_acl_unsafe)

foreign import ccall "consts.h const_read_acl_unsafe"
  const_read_acl_unsafe :: Ptr [Acl]

readAclUnsafe :: [Acl]
readAclUnsafe = unsafePerformIO $ peek (const_read_acl_unsafe)

foreign import ccall "consts.h const_creator_all_acl"
  const_creator_all_acl :: Ptr [Acl]

creatorAllAcl :: [Acl]
creatorAllAcl = unsafePerformIO $ peek (const_creator_all_acl)


{-------------------------------------------------------------------------------
  String vectors
 -------------------------------------------------------------------------------}

instance Storable [String] where
  sizeOf _ = (16)
{-# LINE 570 "Zookeeper/Core.hsc" #-}
  poke p strs act = do
    (\hsc_ptr -> pokeByteOff hsc_ptr 0) p (fromIntegral $ length strs :: Int32)
{-# LINE 572 "Zookeeper/Core.hsc" #-}
    allocaBytes (length strs * S.sizeOf (undefined :: Ptr a)) $ \q -> do
    (\hsc_ptr -> pokeByteOff hsc_ptr 8) p q
{-# LINE 574 "Zookeeper/Core.hsc" #-}
    pokeArray q strs act
  peek p = do
    count <- (\hsc_ptr -> peekByteOff hsc_ptr 0) p :: IO Int32
{-# LINE 577 "Zookeeper/Core.hsc" #-}
    q <- (\hsc_ptr -> peekByteOff hsc_ptr 8) p
{-# LINE 578 "Zookeeper/Core.hsc" #-}
    peekArray (fromIntegral count) q


{-------------------------------------------------------------------------------
  Create flags
 -------------------------------------------------------------------------------}

-- | These flags are used by 'create' to affect node creation.  See
--   <http://hadoop.apache.org/zookeeper/docs/current/zookeeperProgrammers.html>
--   for more details.
data CreateFlag = Ephemeral  -- ^ The node only exists as long as the session that created it is active.
                | Sequence   -- ^ Append a monotonically increasing counter to the end of path.
                  deriving ( Eq, Show, Bounded )

instance Enum CreateFlag where
  fromEnum Ephemeral = 1
{-# LINE 594 "Zookeeper/Core.hsc" #-}
  fromEnum Sequence  = 2
{-# LINE 595 "Zookeeper/Core.hsc" #-}

  toEnum (1) = Ephemeral
{-# LINE 597 "Zookeeper/Core.hsc" #-}
  toEnum (2) = Sequence
{-# LINE 598 "Zookeeper/Core.hsc" #-}
  toEnum unmatched = error ("Create.toEnum: Cannot match " ++ show unmatched)


{-------------------------------------------------------------------------------
  State constants
 -------------------------------------------------------------------------------}

-- | These constants represent the state of a ZooKeeper connection.
data State = ExpiredSession
           | AuthFailedState
           | Connecting
           | Associating
           | Connected
             deriving ( Eq, Show )

instance Enum State where
  fromEnum ExpiredSession  = -112
{-# LINE 615 "Zookeeper/Core.hsc" #-}
  fromEnum AuthFailedState = -113
{-# LINE 616 "Zookeeper/Core.hsc" #-}
  fromEnum Connecting      = 1
{-# LINE 617 "Zookeeper/Core.hsc" #-}
  fromEnum Associating     = 2
{-# LINE 618 "Zookeeper/Core.hsc" #-}
  fromEnum Connected       = 3
{-# LINE 619 "Zookeeper/Core.hsc" #-}

  toEnum (-112) = ExpiredSession
{-# LINE 621 "Zookeeper/Core.hsc" #-}
  toEnum (-113) = AuthFailedState
{-# LINE 622 "Zookeeper/Core.hsc" #-}
  toEnum (1) = Connecting
{-# LINE 623 "Zookeeper/Core.hsc" #-}
  toEnum (2) = Associating
{-# LINE 624 "Zookeeper/Core.hsc" #-}
  toEnum (3) = Connected
{-# LINE 625 "Zookeeper/Core.hsc" #-}
  toEnum unmatched = error ("State.toEnum: Cannot match " ++ show unmatched)


{-------------------------------------------------------------------------------
  Watch types
 -------------------------------------------------------------------------------}

-- |These constants indicate the event that caused a watcher to be executed.
data Event
    -- |A node has been created.  This is only generated by watches on
    -- non-existent nodes. These watches are set using 'exists'.
    = Created
    -- |A node has been deleted.  This is only generated by watches on nodes.
    -- These watches are set using 'exists' and 'get'.
    | Deleted
    -- |A node has changed.  This is only generated by watches on nodes. These
    -- watches are set using 'exists' and 'get'.
    | Changed
    -- |A change as occurred in the list of children.  This is only generated
    -- by watches on the child list of a node. These watches are set using
    -- 'getChildren' or 'getChildren2'.
    | Child
    -- |A session has been lost.  This is generated when a client loses contact
    -- or reconnects with a server.
    | Session
    -- |A watch has been removed. This is generated when the server for
    -- some reason, probably a resource constraint, will no longer watch
    -- a node for a client.
    | NotWatching
      deriving ( Eq, Show )

instance Enum Event where
  fromEnum Created     = 1
{-# LINE 658 "Zookeeper/Core.hsc" #-}
  fromEnum Deleted     = 2
{-# LINE 659 "Zookeeper/Core.hsc" #-}
  fromEnum Changed     = 3
{-# LINE 660 "Zookeeper/Core.hsc" #-}
  fromEnum Child       = 4
{-# LINE 661 "Zookeeper/Core.hsc" #-}
  fromEnum Session     = -1
{-# LINE 662 "Zookeeper/Core.hsc" #-}
  fromEnum NotWatching = -2
{-# LINE 663 "Zookeeper/Core.hsc" #-}

  toEnum (1) = Created
{-# LINE 665 "Zookeeper/Core.hsc" #-}
  toEnum (2) = Deleted
{-# LINE 666 "Zookeeper/Core.hsc" #-}
  toEnum (3) = Changed
{-# LINE 667 "Zookeeper/Core.hsc" #-}
  toEnum (4) = Child
{-# LINE 668 "Zookeeper/Core.hsc" #-}
  toEnum (-1) = Session
{-# LINE 669 "Zookeeper/Core.hsc" #-}
  toEnum (-2) = NotWatching
{-# LINE 670 "Zookeeper/Core.hsc" #-}
  toEnum unmatched = error ("Event.toEnum: Cannot match " ++ show unmatched)


{-------------------------------------------------------------------------------
  Handle
 -------------------------------------------------------------------------------}

-- |ZooKeeper handle.  This is the handle that represents a connection to the
-- ZooKeeper service.  It is needed to invoke any ZooKeeper function. A handle
-- is obtained using 'init'.
newtype Handle = Handle (Ptr Handle)


{-------------------------------------------------------------------------------
  Client ID
 -------------------------------------------------------------------------------}

data ClientId = ClientId
  { clientId :: Int64
  , passwd   :: String
  } deriving ( Eq, Show )

instance Storable ClientId where
  sizeOf _ = (24)
{-# LINE 694 "Zookeeper/Core.hsc" #-}
  poke p (ClientId { clientId, passwd }) act = do
    when (length passwd > 15) $
      error "ClientId.poke: password greater than 15 chars long"
    (\hsc_ptr -> pokeByteOff hsc_ptr 0) p clientId
{-# LINE 698 "Zookeeper/Core.hsc" #-}
    withCStringLen passwd $ \(cPasswd, cPasswdLen) -> do
      copyBytes ((\hsc_ptr -> hsc_ptr `plusPtr` 8) p) cPasswd cPasswdLen
{-# LINE 700 "Zookeeper/Core.hsc" #-}
      act
  peek p = do
    clientId <- (\hsc_ptr -> peekByteOff hsc_ptr 0) p :: IO Int64
{-# LINE 703 "Zookeeper/Core.hsc" #-}
    passwd   <- peekCString $ (\hsc_ptr -> hsc_ptr `plusPtr` 0) p
{-# LINE 704 "Zookeeper/Core.hsc" #-}
    return $ ClientId { clientId, passwd }


{-------------------------------------------------------------------------------
  Watcher
 -------------------------------------------------------------------------------}

type Path = String

type Watcher = Handle -> Event -> State -> Path -> IO ()

type CWatcher = Ptr Handle -> CInt -> CInt -> CString -> Ptr () -> IO ()

foreign import ccall "wrapper"
  wrapWatcher :: CWatcher -> IO (FunPtr CWatcher)

liftWatcher :: Maybe Watcher -> IO (FunPtr CWatcher)
liftWatcher f = do
  case f of
    Nothing -> return nullFunPtr
    Just f  -> do
      let f' h ty st path _ = do p <- peekCString path
                                 f (Handle h) (cToEnum ty) (cToEnum st) p
      wrapWatcher f'


{-------------------------------------------------------------------------------
  Initialise
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zookeeper_init"
  zookeeper_init :: CString -> FunPtr CWatcher -> CInt -> Ptr ClientId -> Ptr ()
                 -> CInt -> IO (Ptr Handle)

{-| The operation may fail with:

    * 'BadArguments'

    * 'MarshallingError'

    * 'OperationTimeout'

    * 'ConnectionLoss'

    * 'SystemError'
-}
init :: String -> Maybe Watcher -> Int -> Maybe ClientId -> IO Handle
init host watcher timeout cid = do
  let cTimeout  = cFromEnum timeout
      cContext  = nullPtr
      cFlags    = cFromEnum 0
      doIt cCid =
        withCString host $ \cHost -> do
        cWatcher <- liftWatcher watcher
        zookeeper_init cHost cWatcher cTimeout cCid cContext cFlags
  h <- case cid of
         Nothing   -> doIt nullPtr
         Just cid' -> do
             allocaBytes (sizeOf cid') $ \cCid -> do
             poke cCid cid' $ doIt cCid
  if h == nullPtr
    then throw SystemError
    else return $ Handle h


{-------------------------------------------------------------------------------
  Close
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zookeeper_close"
  zookeeper_close :: Ptr Handle -> IO CInt

{-| The operation may fail with:

    * 'BadArguments'

    * 'MarshallingError'

    * 'OperationTimeout'

    * 'ConnectionLoss'

    * 'SystemError'
-}
close :: Handle -> IO ()
close (Handle h) = do
  zookeeper_close h >>= flip ifOk (return ())


{-------------------------------------------------------------------------------
  Get client ID
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_client_id"
  zoo_client_id :: Ptr Handle -> IO (Ptr ClientId)

getClientId :: Handle -> IO ClientId
getClientId (Handle h) = do
  cCid <- zoo_client_id h
  if cCid == nullPtr
    then throw SystemError  -- For want of anything better
    else peek cCid


{-------------------------------------------------------------------------------
  Get receive timeout
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_recv_timeout"
  zoo_recv_timeout :: Ptr Handle -> IO CInt

-- Will be zero if not connected
getRecvTimeout :: Handle -> IO Int
getRecvTimeout (Handle h) =
  cToEnum <$> zoo_recv_timeout h


{-------------------------------------------------------------------------------
  Add authentication credentials
 -------------------------------------------------------------------------------}

type CVoidCompletion = CInt -> Ptr () -> IO ()

foreign import ccall "wrapper"
  wrapVoidCompletion :: CVoidCompletion -> IO (FunPtr CVoidCompletion)

foreign import ccall "zookeeper.h zoo_add_auth"
  zoo_add_auth :: Ptr Handle -> CString -> CString -> CInt
               -> FunPtr CVoidCompletion -> Ptr () -> IO CInt

-- TODO Make exception safe.

-- |Synchronous
addAuth :: Handle -> String -> String -> IO ()
addAuth (Handle h) scheme cert = do
  cResErrMVar <- newEmptyMVar
  cb          <- wrapVoidCompletion (\cResErr _ -> putMVar cResErrMVar cResErr)
  withCString    scheme  $ \cScheme           -> do
  withCStringLen cert    $ \(cCert, cCertLen) -> do
  cErr        <- zoo_add_auth h cScheme cCert (cFromEnum cCertLen) cb nullPtr
  ifOk cErr $ do
    cResErr <- takeMVar cResErrMVar
    ifOk cResErr $ return ()


{-------------------------------------------------------------------------------
  Test whether connection is unrecoverable
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h is_unrecoverable"
  is_unrecoverable :: Ptr Handle -> IO CInt

isUnrecoverable :: Handle -> IO Bool
isUnrecoverable (Handle h) = do
  r <- cToEnum <$> is_unrecoverable h
  if r > 0 then return True else return False


{-------------------------------------------------------------------------------
  Set debug level
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_set_debug_level"
  zoo_set_debug_level :: CInt -> IO ()

-- |Set the log level for the library.
setLogLevel :: LogLevel -> IO ()
setLogLevel l = zoo_set_debug_level $ cFromEnum l


{-------------------------------------------------------------------------------
  Set logging stream
 -------------------------------------------------------------------------------}

data Stream

foreign import ccall "zookeeper.h zoo_set_log_stream"
  zoo_set_log_stream :: Ptr Stream -> IO ()

foreign import ccall "stdio.h fopen"
  fopen :: CString -> CString -> IO (Ptr Stream)

{-| Set the file to be used by the library for logging.  By default, zookeeper
    logs to stderr.
-}
setLogFile :: String -> IO ()
setLogFile path = do
  withCString path $ \cPath  -> do
  withCString "a"  $ \cPerms -> do
  st <- fopen cPath cPerms
  zoo_set_log_stream st


{-------------------------------------------------------------------------------
  Set quorum endpoint order randomization
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_deterministic_conn_order"
  zoo_deterministic_conn_order :: CInt -> IO ()

{-| Enable/disable quorum endpoint order randomization

    Note: typically this method should NOT be used outside of testing.

    If passed a non-zero value, will make the client connect to quorum peers
    in the order as specified in 'init'.
    A zero value causes 'init' to permute the peer endpoints
    which is good for more even client connection distribution among the
    quorum peers.
-}
setDeterministicConnOrder :: Bool -> IO ()
setDeterministicConnOrder = zoo_deterministic_conn_order . cFromEnum


{-------------------------------------------------------------------------------
  Create node
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_create"
  zoo_create :: Ptr Handle
             -> CString
             -> CString
             -> CInt
             -> Ptr [Acl]
             -> CInt
             -> CString
             -> CInt
             -> IO CInt

{-| Create a node synchronously.

    This method will create a node in ZooKeeper. A node can only be created if
    it does not already exists. The 'CreateFlag's affect the creation of nodes.
    If 'Ephemeral' is set, the node will automatically get removed if
    the client session goes away. If 'Sequence' is set, a unique
    monotonically increasing sequence number is appended to the path name.

    The operation may fail with:

    * 'NoNode'

    * 'NodeExists'

    * 'NoAuth'

    * 'NoChildrenForEphemerals'

    * 'BadArguments'

    * 'InvalidState'

    * 'MarshallingError'
-}
create :: Handle -> String -> String -> [CreateFlag] -> [Acl] -> IO String
create (Handle h) path value flags aclVector =
  let bufLen = 1024
      cFlags = foldr (.|.) 0 $ cFromEnum <$> flags :: CInt in
  withCString    path   $ \cPath ->
  withCStringLen value  $ \(cValue, cValueLen) ->
  allocaBytes    bufLen $ \cPathBuffer ->
  allocaBytes    (sizeOf aclVector) $ \cAclVector ->
  poke cAclVector aclVector $ do
  cErr <- zoo_create h cPath cValue (cFromEnum cValueLen)
            cAclVector cFlags cPathBuffer (cFromEnum bufLen)
  ifOk cErr $ peekCString cPathBuffer


{-------------------------------------------------------------------------------
  Delete node
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_delete"
  zoo_delete :: Ptr Handle -> CString -> CInt -> IO CInt

{-| Delete a node in zookeeper synchronously.

    The operation may fail with:

    * 'NoNode'

    * 'NoAuth'

    * 'BadVersion'

    * 'NotEmpty'

    * 'BadArguments'

    * 'InvalidState'

    * 'MarshallingError'
-}
delete :: Handle -> Path -> Maybe Int -> IO ()
delete (Handle h) path vers =
  withCString path $ \cPath -> do
  cErr <- zoo_delete h cPath (cFromEnum $ fromMaybe (-1) vers)
  ifOk cErr $ return ()


{-------------------------------------------------------------------------------
  Check for existence of node
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_wexists"
  zoo_wexists :: Ptr Handle -> CString -> FunPtr CWatcher -> Ptr () -> Ptr Stat
              -> IO CInt

{-| Checks the existence of a node in zookeeper synchronously.

    The operation may fail with:

    * 'NoNode'

    * 'NoAuth'

    * 'BadArguments'

    * 'InvalidState'

    * 'MarshallingError'
-}
exists :: Handle -> Path -> Maybe Watcher -> IO (Maybe Stat)
exists (Handle h) path watcher =
  withCString path                $ \cPath   ->
  allocaBytes (72) $ \cStat   -> do
{-# LINE 1029 "Zookeeper/Core.hsc" #-}
  cWatcher <- liftWatcher watcher
  cErr     <- zoo_wexists h cPath cWatcher nullPtr cStat
  if cErr == 0
{-# LINE 1032 "Zookeeper/Core.hsc" #-}
    then Just <$> peek cStat
    else if cErr == -101
{-# LINE 1034 "Zookeeper/Core.hsc" #-}
      then return Nothing
      else check cErr >> undefined


{-------------------------------------------------------------------------------
  Get data from node
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_wget"
  zoo_wget :: Ptr Handle -> CString -> FunPtr CWatcher -> Ptr () -> CString
           -> Ptr CInt -> Ptr Stat -> IO CInt

{-| Gets the data associated with a node synchronously.

    The operation may fail with:

    * 'NoNode'

    * 'NoAuth'

    * 'BadArguments'

    * 'InvalidState'

    * 'MarshallingError'
-}
get :: Handle -> Path -> Maybe Watcher -> IO (String, Stat)
get (Handle h) path watcher =
  let bufLen = 1024 in
  withCString path                $ \cPath   ->
  allocaBytes bufLen              $ \cBuf    ->
  allocaBytes (4)         $ \cBufLen ->
{-# LINE 1066 "Zookeeper/Core.hsc" #-}
  allocaBytes (72) $ \cStat   -> do
{-# LINE 1067 "Zookeeper/Core.hsc" #-}
  S.poke (cBufLen :: Ptr CInt) (cFromEnum bufLen)
  cWatcher <- liftWatcher watcher
  cErr     <- zoo_wget h cPath cWatcher nullPtr cBuf cBufLen cStat
  ifOk cErr $ do
    cBufLen' <- S.peek cBufLen :: IO CInt
    buf      <- peekCStringLen (cBuf, fromIntegral cBufLen')
    stat     <- peek cStat
    return $ (buf, stat)


{-------------------------------------------------------------------------------
  Set data on node
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_set"
  zoo_set :: Ptr Handle -> CString -> CString -> CInt -> CInt -> IO CInt

{-| Sets the data associated with a node. See 'set2' if you require access to the
    stat information associated with the node.

    The operation may fail with:

    * 'NoNode'

    * 'NoAuth'

    * 'BadVersion'

    * 'BadArguments'

    * 'InvalidState'

    * 'MarshallingError'
-}
set :: Handle -> Path -> String -> Maybe Int -> IO ()
set (Handle h) path value vers =
  let cVers = cFromEnum $ fromMaybe (-1) vers in
  withCString     path  $ \cPath               ->
  withCStringLen  value $ \(cValue, cValueLen) -> do
  cErr <- zoo_set h cPath cValue (cFromEnum cValueLen) cVers
  ifOk cErr $ return ()


{-------------------------------------------------------------------------------
  Set and stat on node
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_set2"
  zoo_set2 :: Ptr Handle -> CString -> CString -> CInt -> CInt -> Ptr Stat
           -> IO CInt

{-| Sets the data associated with a node. This function is the same 'set', except
    that it also provides access to 'Stat' information associated with the znode.

    The operation may fail with:

    * 'NoNode'

    * 'NoAuth'

    * 'BadVersion'

    * 'BadArguments'

    * 'InvalidState'

    * 'MarshallingError'
-}
set2 :: Handle -> Path -> String -> Maybe Int -> IO Stat
set2 (Handle h) path value vers =
  let cVers = cFromEnum $ fromMaybe (-1) vers in
  withCString     path  $ \cPath               ->
  withCStringLen  value $ \(cValue, cValueLen) ->
  allocaBytes (72) $ \cStat    -> do
{-# LINE 1141 "Zookeeper/Core.hsc" #-}
  cErr <- zoo_set2 h cPath cValue (cFromEnum cValueLen) cVers cStat
  ifOk cErr $ peek cStat


{-------------------------------------------------------------------------------
  Get children of node
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_wget_children"
  zoo_wget_children :: Ptr Handle -> CString -> FunPtr CWatcher -> Ptr ()
                    -> Ptr [String] -> IO CInt

-- TODO Watcher: must be an immediate child, only notified if created or deleted
-- (but not changed).

{-| Lists the children of a node synchronously.

    The operation may fail with:

    * 'NoNode'

    * 'NoAuth'

    * 'BadArguments'

    * 'InvalidState'

    * 'MarshallingError'
-}
getChildren :: Handle -> Path -> Maybe Watcher -> IO [String]
getChildren (Handle h) path watcher =
  withCString path $ \cPath -> do
  cWatcher <- liftWatcher watcher
  allocaBytes (16) $ \cStringVector -> do
{-# LINE 1175 "Zookeeper/Core.hsc" #-}
  cErr <- zoo_wget_children h cPath cWatcher nullPtr cStringVector
  ifOk cErr $ peek cStringVector


{-------------------------------------------------------------------------------
  Get children and stat node
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_wget_children2"
  zoo_wget_children2 :: Ptr Handle -> CString -> FunPtr CWatcher -> Ptr ()
                     -> Ptr [String] -> Ptr Stat -> IO CInt

{-| Lists the children of a node and get its stat synchronously.

    The operation may fail with:

    * 'NoNode'

    * 'NoAuth'

    * 'BadArguments'

    * 'InvalidState'

    * 'MarshallingError'
-}
getChildren2 :: Handle -> Path -> Maybe Watcher -> IO ([String], Stat)
getChildren2 (Handle h) path watcher =
  withCString path $ \cPath -> do
  cWatcher <- liftWatcher watcher
  allocaBytes (16) $ \cStringVector -> do
{-# LINE 1206 "Zookeeper/Core.hsc" #-}
  allocaBytes (72) $ \cStat -> do
{-# LINE 1207 "Zookeeper/Core.hsc" #-}
  cErr <- zoo_wget_children2 h cPath cWatcher nullPtr cStringVector cStat
  ifOk cErr $ do
    stringVector <- peek cStringVector
    stat         <- peek cStat
    return (stringVector, stat)


{-------------------------------------------------------------------------------
  Get ACL from node
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_get_acl"
  zoo_get_acl :: Ptr Handle -> CString -> Ptr [Acl] -> Ptr Stat -> IO CInt

{-| Gets the acl associated with a node synchronously.

    The operation may fail with:

    * 'NoNode'

    * 'NoAuth'

    * 'BadArguments'

    * 'InvalidState'

    * 'MarshallingError'
-}
getAcl :: Handle -> Path -> IO ([Acl], Stat)
getAcl (Handle h) path =
  withCString path $ \cPath ->
  allocaBytes (16) $ \cAcls ->
{-# LINE 1239 "Zookeeper/Core.hsc" #-}
  allocaBytes (72) $ \cStat -> do
{-# LINE 1240 "Zookeeper/Core.hsc" #-}
  cErr <- zoo_get_acl h cPath cAcls cStat
  ifOk cErr $ do
    acls <- peek cAcls
    stat <- peek cStat
    return (acls, stat)


{-------------------------------------------------------------------------------
  Set ACL on node
 -------------------------------------------------------------------------------}

foreign import ccall "zookeeper.h zoo_set_acl"
  zoo_set_acl :: Ptr Handle -> CString -> CInt -> Ptr [Acl] -> IO CInt

{-| Sets the acl associated with a node synchronously.

    The operation may fail with:

    * 'NoNode'

    * 'NoAuth'

    * 'InvalidAcl'

    * 'BadVersion'

    * 'BadArguments'

    * 'InvalidState'

    * 'MarshallingError'
-}
setAcl :: Handle -> Path -> Maybe Int -> [Acl] -> IO ()
setAcl (Handle h) path vers acls =
  let cVers = cFromEnum $ fromMaybe (-1) vers in
  withCString path $ \cPath ->
  allocaBytes (16) $ \cAcls ->
{-# LINE 1277 "Zookeeper/Core.hsc" #-}
  poke cAcls acls $ do
  cErr <- zoo_set_acl h cPath cVers cAcls
  ifOk cErr $ return ()

