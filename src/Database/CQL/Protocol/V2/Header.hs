-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.Protocol.V2.Header
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
    , encodeHeader
    , decodeHeader
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

encodeHeader :: Putter Header
encodeHeader h = do
    encodeByte $ case headerType h of
        RqHeader -> mapVersion (version h)
        RsHeader -> mapVersion (version h) `setBit` 7
    encodeFlags (flags      h)
    encodeStreamId (streamId   h)
    encodeOpCode (opCode     h)
    encodeLength (bodyLength h)
 where
    mapVersion :: Version -> Word8
    mapVersion V2 = 2

decodeHeader :: Get Header
decodeHeader = do
    b <- getWord8
    Header (mapHeaderType b)
        <$> decVersion (b .&. 0x7F)
        <*> decodeFlags
        <*> decodeStreamId
        <*> decodeOpCode
        <*> decodeLength
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
header = runGetLazy decodeHeader

------------------------------------------------------------------------------
-- Length

newtype Length = Length { lengthRepr :: Int32 } deriving (Eq, Show)

encodeLength :: Putter Length
encodeLength (Length x) = encodeInt x

decodeLength :: Get Length
decodeLength = Length <$> decodeInt

------------------------------------------------------------------------------
-- StreamId

newtype StreamId = StreamId { streamRepr :: Int8 } deriving (Eq, Show)

encodeStreamId :: Putter StreamId
encodeStreamId (StreamId x) = encodeSignedByte x

decodeStreamId :: Get StreamId
decodeStreamId = StreamId <$> decodeSignedByte

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
