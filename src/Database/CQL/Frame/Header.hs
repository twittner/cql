-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.Frame.Header where

import Control.Applicative
import Data.Bits
import Data.Int
import Data.Word
import Database.CQL.Frame.Codec

data Header = Header
    { hdrVersion  :: !Word8
    , hdrFlags    :: !Word8
    , hdrStreamId :: !Int8
    , hdrOpCode   :: !Word8
    , hdrLength   :: !Int32
    } deriving (Eq, Show)

instance Encoding Header where
    encode h = do
        encode (hdrVersion h)
        encode (hdrFlags h)
        encode (hdrStreamId h)
        encode (hdrOpCode h)
        encode (hdrLength h)

instance Decoding Header where
    decode = Header
        <$> decode
        <*> decode
        <*> decode
        <*> decode
        <*> decode

isResponse :: Header -> Bool
isResponse h = hdrVersion h `testBit` 7

isCompressed :: Header -> Bool
isCompressed h = hdrFlags h `testBit` 0

isTracing :: Header -> Bool
isTracing h = hdrFlags h `testBit` 1
