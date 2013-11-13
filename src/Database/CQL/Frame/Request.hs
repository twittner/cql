-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.Frame.Request
    ( Request        (..)
    , RequestMessage (..)
    , BatchType      (..)
    , BatchQuery     (..)
    , QueryParams    (..)
    , request
    ) where

import Control.Applicative
import Data.Bits
import Data.ByteString (ByteString)
import Data.Int
import Data.Text (Text)
import Data.Maybe (isJust)
import Data.Serialize hiding (decode, encode)
import Data.Word
import Database.CQL.Frame.Codec
import Database.CQL.Frame.Header
import Database.CQL.Frame.Types

import qualified Data.ByteString      as B
import qualified Data.ByteString.Lazy as LB
import qualified Data.Text.Lazy       as LT

-----------------------------------------------------------------------------
-- Request

data Request = Request
    { rqHeader :: !Header
    , rqBody   :: !ByteString
    }

request :: ProtocolVersion
        -> Maybe Compression
        -> Bool
        -> StreamId
        -> RequestMessage
        -> Request
request v c t (StreamId i) b = do
    let body = compress c (encWrite b)
    let len  = fromIntegral $ B.length body
    let hdr  = Header (version v) flags i (opCode b) len
    Request hdr body
  where
    compress (Just (Snappy f)) a = f a
    compress (Just (ZLib   f)) a = f a
    compress Nothing           a = a

    version ProtocolV2 = 0x02

    flags = 0
        .|. (maybe 0 (const 1) c)
        .|. (if t then 2 else 0)

    opCode (Startup _ _)    = 0x01
    opCode Options          = 0x05
    opCode (Query _ _)      = 0x07
    opCode (Execute _ _)    = 0x0A
    opCode (Prepare _)      = 0x09
    opCode (Register _)     = 0x0B
    opCode (Batch _ _ _)    = 0x0D
    opCode (AuthResponse _) = 0x0F

-----------------------------------------------------------------------------
-- Request Message

data RequestMessage
    = Startup      !CqlVersion (Maybe Compression)
    | AuthResponse !LB.ByteString
    | Options
    | Query        !QueryString !QueryParams
    | Execute      !QueryId !QueryParams
    | Prepare      !LT.Text
    | Register     [EventType]
    | Batch        !BatchType [BatchQuery] !Consistency

data Compression
    = Snappy (ByteString -> ByteString)
    | ZLib   (ByteString -> ByteString)

instance Encoding RequestMessage where
    encode (Query (QueryString s) p) = encode s >> encode p
    encode (Execute (QueryId q) p)   = encode q >> encode p
    encode (Prepare p)      = encode p
    encode Options          = return ()
    encode (AuthResponse b) = encode b
    encode (Startup v c)    = do
        encode $ ("CQL_VERSION", encVersion v) : encCompression c
      where
        encVersion :: CqlVersion -> Text
        encVersion Cqlv300 = "3.0.0"

        encCompression :: Maybe Compression -> [(Text, Text)]
        encCompression (Just (Snappy _)) = [("COMPRESSION", "snappy")]
        encCompression (Just (ZLib _))   = [("COMPRESSION", "zlib")]
        encCompression Nothing           = []

    encode (Register t)  = do
        encode (fromIntegral (length t) :: Word8)
        mapM_ encode t
    encode (Batch t q c) = do
        encode t
        encode (fromIntegral (length q) :: Word16)
        mapM_ encode q
        encode c

data EventType
    = TopologyChangeEvent
    | StatusChangeEvent
    | SchemaChangeEvent
    deriving (Eq, Show)

instance Encoding EventType where
    encode TopologyChangeEvent = encode ("TOPOLOGY_CHANGE" :: Text)
    encode StatusChangeEvent   = encode ("STATUS_CHANGE"   :: Text)
    encode SchemaChangeEvent   = encode ("SCHEMA_CHANGE"   :: Text)

-----------------------------------------------------------------------------
-- Batch Type & Batch Query

data BatchType
    = BatchLogged
    | BatchUnLogged
    | BatchCounter

instance Encoding BatchType where
    encode BatchLogged   = putWord8 0
    encode BatchUnLogged = putWord8 1
    encode BatchCounter  = putWord8 2

data BatchQuery
    = BatchQuery    !QueryString [Value]
    | BatchPrepared !QueryId     [Value]

instance Encoding BatchQuery where
    encode (BatchQuery (QueryString q) vv) = do
        putWord8 0
        encode q
        encodeValues vv
    encode (BatchPrepared (QueryId i) vv)  = do
        putWord8 1
        encode i
        encodeValues vv

-----------------------------------------------------------------------------
-- Query Parameters

data QueryParams = QueryParams
    { qConsistency       :: !Consistency
    , qSkipMetaData      :: !Bool
    , qValues            :: [Value]
    , qPageSize          :: Maybe Int32
    , qPagingState       :: Maybe PagingState
    , qSerialConsistency :: Maybe SerialConsistency
    }

data SerialConsistency
    = SerialConsistency
    | LocalSerialConsistency
    deriving (Eq, Show)

instance Encoding QueryParams where
    encode p = do
        encode . qConsistency $ p
        put flags
        encodeValues . qValues $ p
        encodeMaybe  . qPageSize $ p
        encodeMaybe  . qPagingState $ p
        encodeMaybe  $ mapCons <$> qSerialConsistency p
      where
        flags :: Word8
        flags = (if not (null (qValues p))          then 0x01 else 0x0)
            .|. (if qSkipMetaData p                 then 0x02 else 0x0)
            .|. (if isJust . qPageSize $ p          then 0x04 else 0x0)
            .|. (if isJust . qPagingState $ p       then 0x08 else 0x0)
            .|. (if isJust . qSerialConsistency $ p then 0x10 else 0x0)

        mapCons SerialConsistency = Serial
        mapCons LocalSerialConsistency = LocalSerial

-----------------------------------------------------------------------------
-- Value

data Value where
    Value :: (Encoding a) => a -> Value

encodeValue :: Putter Value
encodeValue (Value a) = encode a

encodeValues :: Putter [Value]
encodeValues [] = return ()
encodeValues vv = do
    put (fromIntegral (length vv) :: Word8)
    mapM_ encodeValue vv
