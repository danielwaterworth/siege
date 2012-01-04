{-# OPTIONS_GHC -Wwarn #-}

module Database.Siege.Recv where

import Data.Maybe
import Data.Word
import Data.Char
import qualified Data.ByteString as B
import qualified Data.Enumerator as E
import qualified Data.Enumerator.List as EL

import Control.Monad
import Control.Monad.Trans.Maybe

import Database.Siege.StringHelper

recvByte :: Monad m => E.Iteratee B.ByteString m (Maybe Word8)
recvByte = do
  dat <- EL.head
  case dat of
    Just dat' -> do
      let l = B.length dat'
      if l == 0 then
        recvByte
       else if l == 1 then
        return (Just $ B.head dat')
       else
        E.yield (Just $ B.head dat') (E.Chunks [B.tail dat'])
    Nothing ->
      E.yield Nothing E.EOF

recv :: Monad m => Int -> E.Iteratee B.ByteString m (Maybe [B.ByteString])
recv n = do
  dat <- EL.head
  case dat of
    Just dat' -> do
      let l = B.length dat'
      if l > n then
        E.yield (Just [B.take n dat']) (E.Chunks [B.drop n dat'])
       else if l < n then do
        out <- recv (n - l)
        return $ fmap (\out' -> (dat':out')) out
       else
        return $ Just [dat']
    Nothing -> do
      E.yield Nothing E.EOF

recvLine :: Monad m => E.Iteratee B.ByteString m (Maybe [B.ByteString])
recvLine = do
  dat <- EL.head
  case dat of
    Just dat' -> do
      case B.findSubstring (stToB $ "\r\n") dat' of
        Just n -> do
          let n' = n + 2
          if B.length dat' == n' then
            return $ Just [dat']
           else
            E.yield (Just [B.take n' dat']) (E.Chunks [B.drop n' dat'])
        Nothing ->
          if B.last dat' == (fromIntegral $ ord '\r') then do
            nxt <- EL.head
            case nxt of
              Just nxt' -> 
                if B.head nxt' == (fromIntegral $ ord '\n') then
                  E.yield (Just [dat', stToB "\n"]) (E.Chunks [B.tail nxt'])
                else do
                  E.yield () (E.Chunks [nxt'])
                  out <- recvLine
                  return $ fmap (\out' -> (dat':out')) out
              Nothing ->
                E.yield Nothing E.EOF
          else do
            out <- recvLine
            return $ fmap (\out' -> (dat':out')) out
    Nothing ->
      E.yield Nothing E.EOF

recvCommand :: Monad m => E.Iteratee B.ByteString m (Maybe [Maybe B.ByteString])
recvCommand = runMaybeT $ do
  line <- recvLift recvLine
  expectFirstChar line '*'
  n <- MaybeT $ return $ ((maybeRead . applyReversed (drop 2) . tail . bToSt) line :: Maybe Int)
  replicateM n $ do
    line' <- recvLift recvLine
    expectFirstChar line' '$'
    m <- MaybeT $ return $ ((maybeRead . applyReversed (drop 2) . tail . bToSt) line' :: Maybe Int)
    if m >= 0 then do
      dat <- recvLift $ recv m
      _ <- recvLift $ recv 2
      return $ Just dat
     else if m == -1 then
      return Nothing
     else
      MaybeT $ return Nothing
 where
  expectFirstChar :: Monad m => B.ByteString -> Char -> MaybeT m ()
  expectFirstChar line c =
    when (B.null line || (B.head line /= (fromIntegral $ ord c))) $ do
      MaybeT $ return Nothing

  applyReversed fn = reverse . fn . reverse

  maybeRead :: Read a => String -> Maybe a
  maybeRead = fmap fst . listToMaybe . reads

  recvLift :: (Monad m) => m (Maybe [B.ByteString]) -> (MaybeT m) B.ByteString
  recvLift = MaybeT . liftM (fmap B.concat)
