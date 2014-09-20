-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE KindSignatures  #-}
{-# LANGUAGE RecordWildCards #-}

module Database.CQL.Protocol.Header
    ( Header     (..)
    , HeaderType (..)
    , Version    (..)
    , Flags
    , Length     (..)
    , StreamId
    , streamId2
    , streamId3
    , compress
    , tracing
    , isSet
    , encodeHeader2
    , encodeHeader3
    , decodeHeader2
    , decodeHeader3
    ) where

import Control.Applicative
import Data.Bits
import Data.Int
import Data.Monoid
import Data.Serialize
import Data.Singletons.TypeLits (Nat)
import Data.Word
import Database.CQL.Protocol.Codec
import Database.CQL.Protocol.Types

data Header (v :: Nat) where
    H2 :: { h2HeaderType :: !HeaderType
          , h2Version    :: !Version
          , h2Flags      :: !Flags
          , h2StreamId   :: !(StreamId 2)
          , h2OpCode     :: !OpCode
          , h2BodyLength :: !Length
          } -> Header 2
    H3 :: { h3HeaderType :: !HeaderType
          , h3Version    :: !Version
          , h3Flags      :: !Flags
          , h3StreamId   :: !(StreamId 3)
          , h3OpCode     :: !OpCode
          , h3BodyLength :: !Length
          } -> Header 3

data HeaderType = RqHeader | RsHeader deriving Show

encodeHeader2 :: HeaderType -> Flags -> StreamId 2 -> OpCode -> Length -> PutM ()
encodeHeader2 t f i o l = do
    encodeByte $ case t of
        RqHeader -> fromVersion V2
        RsHeader -> fromVersion V2 `setBit` 7
    encodeFlags f
    encodeStreamId2 i
    encodeOpCode o
    encodeLength l

encodeHeader3 :: HeaderType -> Flags -> StreamId 3 -> OpCode -> Length -> PutM ()
encodeHeader3 t f i o l = do
    encodeByte $ case t of
        RqHeader -> fromVersion V3
        RsHeader -> fromVersion V3 `setBit` 7
    encodeFlags f
    encodeStreamId3 i
    encodeOpCode o
    encodeLength l

decodeHeader2 :: Get (Header 2)
decodeHeader2 = do
    b <- getWord8
    H2 (mapHeaderType b)
        <$> toVersion (b .&. 0x7F)
        <*> decodeFlags
        <*> decodeStreamId2
        <*> decodeOpCode
        <*> decodeLength

decodeHeader3 :: Get (Header 3)
decodeHeader3 = do
    b <- getWord8
    H3 (mapHeaderType b)
        <$> toVersion (b .&. 0x7F)
        <*> decodeFlags
        <*> decodeStreamId3
        <*> decodeOpCode
        <*> decodeLength

mapHeaderType :: Word8 -> HeaderType
mapHeaderType b = if b `testBit` 7 then RsHeader else RqHeader

------------------------------------------------------------------------------
-- Version

data Version = V1 | V2 | V3 deriving (Eq, Show)

fromVersion :: Version -> Word8
fromVersion V1 = 1
fromVersion V2 = 2
fromVersion V3 = 3

toVersion :: Word8 -> Get Version
toVersion 1 = return V1
toVersion 2 = return V2
toVersion 3 = return V3
toVersion w = fail $ "decode-version: unknown: " ++ show w

------------------------------------------------------------------------------
-- Length

newtype Length = Length { lengthRepr :: Int32 } deriving (Eq, Show)

encodeLength :: Putter Length
encodeLength (Length x) = encodeInt x

decodeLength :: Get Length
decodeLength = Length <$> decodeInt

------------------------------------------------------------------------------
-- StreamId

newtype StreamId (v :: Nat) = StreamId Int16 deriving (Eq, Show)

streamId2 :: Int8 -> StreamId 2
streamId2 = StreamId . fromIntegral

streamId3 :: Int16 -> StreamId 3
streamId3 = StreamId

encodeStreamId2 :: Putter (StreamId 2)
encodeStreamId2 (StreamId x) = encodeSignedByte (fromIntegral x)

decodeStreamId2 :: Get (StreamId 2)
decodeStreamId2 = StreamId . fromIntegral <$> decodeSignedByte

encodeStreamId3 :: Putter (StreamId 3)
encodeStreamId3 (StreamId x) = encodeSignedShort x

decodeStreamId3 :: Get (StreamId 3)
decodeStreamId3 = StreamId <$> decodeSignedShort

------------------------------------------------------------------------------
-- Flags

newtype Flags = Flags Word8
    deriving (Eq, Show)

instance Monoid Flags where
    mempty = Flags 0
    mappend (Flags a) (Flags b) = Flags (a .|. b)

encodeFlags :: Putter Flags
encodeFlags (Flags x) = encodeByte x

decodeFlags :: Get Flags
decodeFlags = Flags <$> decodeByte

compress :: Flags
compress = Flags 1

tracing :: Flags
tracing = Flags 2

isSet :: Flags -> Flags -> Bool
isSet (Flags a) (Flags b) = a .&. b == a
