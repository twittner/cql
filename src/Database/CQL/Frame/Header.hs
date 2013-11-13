-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.Frame.Header
    ( Header     (..)
    , HeaderData (..)
    , Version    (..)
    , Flag
    , StreamId   (..)
    , Length     (..)

    , compression
    , tracing
    , isSet
    ) where

import Control.Applicative
import Data.Bits
import Data.Int
import Data.Monoid
import Data.Serialize hiding (encode, decode)
import Data.Word
import Database.CQL.Frame.Codec
import Database.CQL.Frame.Types

data Header
    = RequestHeader  !HeaderData
    | ResponseHeader !HeaderData

data HeaderData = HeaderData
    { hdrVersion  :: !Version
    , hdrFlags    :: !Flag
    , hdrStreamId :: !StreamId
    , hdrOpCode   :: !OpCode
    , hdrLength   :: !Length
    }

data Version = V2
    deriving (Eq, Show)

newtype Flag = Flag { unFlag :: Word8 }
    deriving (Eq, Show)

newtype Length = Length { unLength :: Int32 }
    deriving (Eq, Show)

newtype StreamId = StreamId { unStreamId :: Int8 }
    deriving (Eq, Show)

instance Encoding Header where
    encode (RequestHeader h) = do
        encode (version2Byte (hdrVersion h))
        encode (unFlag (hdrFlags h))
        encode (unStreamId (hdrStreamId h))
        encode (hdrOpCode h)
        encode (unLength (hdrLength h))

    encode (ResponseHeader h) = do
        encode (setBit (version2Byte (hdrVersion h)) 7)
        encode (unFlag (hdrFlags h))
        encode (unStreamId (hdrStreamId h))
        encode (hdrOpCode h)
        encode (unLength (hdrLength h))

instance Decoding Header where
    decode = do
        v <- getWord8
        if v `testBit` 7
            then ResponseHeader <$> (byte2Version (v .&. 0x7F) >>= decodeData)
            else RequestHeader  <$> (byte2Version v            >>= decodeData)
      where
        decodeData v = HeaderData v
            <$> (Flag     <$> decode)
            <*> (StreamId <$> decode)
            <*> decode
            <*> (Length   <$> decode)

instance Monoid Flag where
    mempty = Flag 0
    mappend (Flag a) (Flag b) = Flag (a .|. b)

compression :: Flag
compression = Flag 1

tracing :: Flag
tracing = Flag 2

isSet :: Flag -> Flag -> Bool
isSet (Flag a) (Flag b) = a .&. b == a

------------------------------------------------------------------------------
-- Helpers

version2Byte :: Version -> Word8
version2Byte V2 = 2

byte2Version :: Word8 -> Get Version
byte2Version 2 = return V2
byte2Version w = fail $ "decode-version: unknown: " ++ show w
