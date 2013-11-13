-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.Frame.Codec where

import Control.Applicative
import Control.Monad
import Data.Bits
import Data.ByteString (ByteString)
import Data.Int
import Data.Text (Text)
import Data.UUID (UUID)
import Data.Word
import Data.Serialize hiding (decode, encode)
import Database.CQL.Frame.Types
import Network.Socket (SockAddr (..), PortNumber (..))

import qualified Data.ByteString         as B
import qualified Data.ByteString.Lazy    as LB
import qualified Data.Text.Encoding      as T
import qualified Data.Text.Lazy          as LT
import qualified Data.Text.Lazy.Encoding as LT
import qualified Data.UUID               as UUID

class Encoding a where
    encode :: Putter a

class Decoding a where
    decode :: Get a

encWrite :: (Encoding a) => a -> ByteString
encWrite = runPut . encode

encWriteLazy :: (Encoding a) => a -> LB.ByteString
encWriteLazy = runPutLazy . encode

decRead :: (Decoding a) => ByteString -> Either String a
decRead = runGet decode

decReadLazy :: (Decoding a) => LB.ByteString -> Either String a
decReadLazy = runGetLazy decode

-----------------------------------------------------------------------------
-- Byte

instance Encoding Word8 where
    encode = put

instance Decoding Word8 where
    decode = get

-----------------------------------------------------------------------------
-- Signed Byte

instance Encoding Int8 where
    encode = put

instance Decoding Int8 where
    decode = get

-----------------------------------------------------------------------------
-- Short

instance Encoding Word16 where
    encode = put

instance Decoding Word16 where
    decode = get

-----------------------------------------------------------------------------
-- Int

instance Encoding Int32 where
    encode = put

instance Decoding Int32 where
    decode = get

-----------------------------------------------------------------------------
-- String

instance Encoding Text where
    encode = encode . T.encodeUtf8

instance Decoding Text where
    decode = T.decodeUtf8 <$> decode

-----------------------------------------------------------------------------
-- Long String

instance Encoding LT.Text where
    encode = encode . LT.encodeUtf8

instance Decoding LT.Text where
    decode = do
        n <- get :: Get Int32
        LT.decodeUtf8 <$> getLazyByteString (fromIntegral n)

-----------------------------------------------------------------------------
-- Bytes

instance Encoding LB.ByteString where
    encode bs = do
        put (fromIntegral (LB.length bs) :: Int32)
        putLazyByteString bs

instance Decoding (Maybe LB.ByteString) where
    decode = do
        n <- get :: Get Int32
        if n < 0
            then return Nothing
            else Just <$> getLazyByteString (fromIntegral n)

-----------------------------------------------------------------------------
-- Short Bytes

instance Encoding ByteString where
    encode bs = do
        put (fromIntegral (B.length bs) :: Word16)
        putByteString bs

instance Decoding ByteString where
    decode = do
        n <- get :: Get Word16
        getByteString (fromIntegral n)

-----------------------------------------------------------------------------
-- UUID

instance Encoding UUID where
    encode = putLazyByteString . UUID.toByteString

instance Decoding UUID where
    decode = do
        uuid <- UUID.fromByteString <$> getLazyByteString 16
        maybe (fail "Invalid UUID") return uuid

-----------------------------------------------------------------------------
-- String List

instance Encoding [Text] where
    encode sl = do
        put (fromIntegral (length sl) :: Word16)
        mapM_ encode sl

instance Decoding [Text] where
    decode = do
        n <- get :: Get Int16
        replicateM (fromIntegral n) decode

-----------------------------------------------------------------------------
-- String Map

instance Encoding [(Text, Text)] where
    encode m = do
        put (fromIntegral (length m) :: Word16)
        forM_ m $ \(k, v) -> encode k >> encode v

instance Decoding [(Text, Text)] where
    decode = do
        n <- get :: Get Word16
        replicateM (fromIntegral n) ((,) <$> decode <*> decode)

-----------------------------------------------------------------------------
-- String Multi-Map

instance Encoding [(Text, [Text])] where
    encode mm = do
        put (fromIntegral (length mm) :: Word16)
        forM_ mm $ \(k, v) -> encode k >> encode v

instance Decoding [(Text, [Text])] where
    decode = do
        n <- get :: Get Word16
        replicateM (fromIntegral n) ((,) <$> decode <*> decode)

-----------------------------------------------------------------------------
-- Inet Address

instance Encoding SockAddr where
    encode (SockAddrInet (PortNum p) a) =
        putWord8 4 >> put p >> put a
    encode (SockAddrInet6 (PortNum p) _ (a, b, c, d) _) =
        putWord8 16 >> put p >> put a >> put b >> put c >> put d
    encode (SockAddrUnix _) = fail "unix address not supported"

instance Decoding SockAddr where
    decode = do
        n <- getWord8
        case n of
            4  -> SockAddrInet  <$> getPort <*> getIPv4
            16 -> SockAddrInet6 <$> getPort <*> pure 0 <*> getIPv6 <*> pure 0
            _  -> fail $ "Unexpected socket address: " ++ show n
      where
        getPort :: Get PortNumber
        getPort = PortNum <$> get

        getIPv4 :: Get Word32
        getIPv4 = get

        getIPv6 :: Get (Word32, Word32, Word32, Word32)
        getIPv6 = (,,,) <$> get <*> get <*> get <*> get

-----------------------------------------------------------------------------
-- Consistency

instance Encoding Consistency where
    encode Any         = encode (0x00 :: Word16)
    encode One         = encode (0x01 :: Word16)
    encode Two         = encode (0x02 :: Word16)
    encode Three       = encode (0x03 :: Word16)
    encode Quorum      = encode (0x04 :: Word16)
    encode All         = encode (0x05 :: Word16)
    encode LocalQuorum = encode (0x06 :: Word16)
    encode EachQuorum  = encode (0x07 :: Word16)
    encode Serial      = encode (0x08 :: Word16)
    encode LocalSerial = encode (0x09 :: Word16)
    encode LocalOne    = encode (0x10 :: Word16)

instance Decoding Consistency where
    decode = decode >>= mapCode
      where
        mapCode :: Word16 -> Get Consistency
        mapCode 0x00 = return Any
        mapCode 0x01 = return One
        mapCode 0x02 = return Two
        mapCode 0x03 = return Three
        mapCode 0x04 = return Quorum
        mapCode 0x05 = return All
        mapCode 0x06 = return LocalQuorum
        mapCode 0x07 = return EachQuorum
        mapCode 0x08 = return Serial
        mapCode 0x09 = return LocalSerial
        mapCode 0x10 = return LocalOne
        mapCode code = fail $ "Unknown consistency: " ++ show code

-----------------------------------------------------------------------------
-- Binary Version

instance Encoding ProtocolVersion where
    encode ProtocolV2 = putWord8 0x02

instance Decoding ProtocolVersion where
    decode = decode >>= fromByte . (0x7F .&.)
      where
        fromByte :: Word8 -> Get ProtocolVersion
        fromByte 0x02  = return ProtocolV2
        fromByte other = fail $ "decode: unknown version: " ++ show other

-----------------------------------------------------------------------------
-- Paging State

instance Encoding PagingState where
    encode (PagingState s) = encode s

instance Decoding (Maybe PagingState) where
    decode = liftM PagingState <$> decode

-----------------------------------------------------------------------------
-- Helpers

encodeMaybe :: (Encoding a) => Putter (Maybe a)
encodeMaybe Nothing  = return ()
encodeMaybe (Just x) = encode x
