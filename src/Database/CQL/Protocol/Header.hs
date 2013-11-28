-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.Protocol.Header
    ( Header     (..)
    , HeaderType (..)
    , Version    (..)
    , Flags
    , StreamId   (..)
    , Length     (..)
    , header
    , compress
    , tracing
    , isSet
    ) where

import Control.Applicative
import Data.Bits
import Data.ByteString.Lazy (ByteString)
import Data.Int
import Data.Monoid
import Data.Serialize hiding (encode, decode)
import Data.Word
import Database.CQL.Protocol.Codec
import Database.CQL.Protocol.Types

data Header = Header
    { headerType :: !HeaderType
    , version    :: !Version
    , flags      :: !Flags
    , streamId   :: !StreamId
    , opCode     :: !OpCode
    , bodyLength :: !Length
    } deriving (Show)

instance Encoding Header where
    encode h = do
        encode $ case headerType h of
            RqHeader -> mapVersion (version h)
            RsHeader -> mapVersion (version h) `setBit` 7
        encode (flags      h)
        encode (streamId   h)
        encode (opCode     h)
        encode (bodyLength h)
     where
        mapVersion :: Version -> Word8
        mapVersion V2 = 2

instance Decoding Header where
    decode = do
        b <- getWord8
        Header (mapHeaderType b)
            <$> decVersion (b .&. 0x7F)
            <*> decode
            <*> decode
            <*> decode
            <*> decode
      where
        mapHeaderType b = if b `testBit` 7 then RsHeader else RqHeader

        decVersion :: Word8 -> Get Version
        decVersion 1 = fail "decode-version: CQL Protocol V1 not supported."
        decVersion 2 = return V2
        decVersion w = fail $ "decode-version: unknown: " ++ show w

data HeaderType
    = RqHeader
    | RsHeader
    deriving (Show)

data Version = V2
    deriving (Eq, Show)

header :: ByteString -> Either String Header
header = decReadLazy

------------------------------------------------------------------------------
-- Length

newtype Length = Length { lengthRepr :: Int32 } deriving (Eq, Show)

instance Encoding Length where
    encode (Length x) = encode x

instance Decoding Length where
    decode = Length <$> decode

------------------------------------------------------------------------------
-- StreamId

newtype StreamId = StreamId { streamRepr :: Int8 } deriving (Eq, Show)

instance Encoding StreamId where
    encode (StreamId x) = encode x

instance Decoding StreamId where
    decode = StreamId <$> decode

------------------------------------------------------------------------------
-- Flags

newtype Flags = Flags Word8
    deriving (Eq, Show)

instance Monoid Flags where
    mempty = Flags 0
    mappend (Flags a) (Flags b) = Flags (a .|. b)

instance Encoding Flags where
    encode (Flags x) = encode x

instance Decoding Flags where
    decode = Flags <$> decode

compress :: Flags
compress = Flags 1

tracing :: Flags
tracing = Flags 2

isSet :: Flags -> Flags -> Bool
isSet (Flags a) (Flags b) = a .&. b == a
