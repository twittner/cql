-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}

module Database.CQL.Protocol.Request
    ( Request           (..)
    , AuthResponse      (..)
    , Batch             (..)
    , BatchQuery        (..)
    , BatchType         (..)
    , Compression       (..)
    , CqlVersion        (..)
    , EventType         (..)
    , Execute           (..)
    , Options           (..)
    , Prepare           (..)
    , Query             (..)
    , QueryParams       (..)
    , Register          (..)
    , SerialConsistency (..)
    , Startup           (..)
    , pack
    , getOpCode
    ) where

import Control.Applicative
import Data.Bits
import Data.ByteString.Lazy (ByteString)
import Data.Int
import Data.Tagged
import Data.Text (Text)
import Data.Maybe (isJust)
import Data.Monoid
import Data.Serialize hiding (decode, encode)
import Data.Word
import Database.CQL.Protocol.Tuple
import Database.CQL.Protocol.Codec
import Database.CQL.Protocol.Header
import Database.CQL.Protocol.Types

import qualified Data.ByteString.Lazy as LB

------------------------------------------------------------------------------
-- Request

data Request k a b
    = RqStartup  !Startup
    | RqOptions  !Options
    | RqRegister !Register
    | RqBatch    !Batch
    | RqAuthResp !AuthResponse
    | RqPrepare  !(Prepare k a b)
    | RqQuery    !(Query k a b)
    | RqExecute  !(Execute k a b)
    deriving (Show)

instance (Tuple a) => Encoding (Request k a b) where
    encode (RqStartup  r) = encode r
    encode (RqOptions  r) = encode r
    encode (RqRegister r) = encode r
    encode (RqBatch    r) = encode r
    encode (RqAuthResp r) = encode r
    encode (RqPrepare  r) = encode r
    encode (RqQuery    r) = encode r
    encode (RqExecute  r) = encode r

pack :: (Tuple a)
     => Compression
     -> Bool
     -> StreamId
     -> Request k a b
     -> Either String ByteString
pack c t i r = do
    body <- runCompression c (encWriteLazy r)
    let len = Length . fromIntegral $ LB.length body
    let hdr = Header RqHeader V2 mkFlags i (getOpCode r) len
    return . runPutLazy $ encode hdr >> putLazyByteString body
  where
    runCompression f x = maybe compressError return (shrink f $ x)
    compressError      = Left "pack: compression failure"

    mkFlags = (if t then tracing else mempty)
        <> (if algorithm c /= None then compress else mempty)

getOpCode :: Request k a b -> OpCode
getOpCode (RqQuery _)    = OcQuery
getOpCode (RqExecute _)  = OcExecute
getOpCode (RqPrepare _)  = OcPrepare
getOpCode (RqBatch _)    = OcBatch
getOpCode (RqRegister _) = OcRegister
getOpCode (RqOptions _)  = OcOptions
getOpCode (RqStartup _)  = OcStartup
getOpCode (RqAuthResp _) = OcAuthResponse

------------------------------------------------------------------------------
-- STARTUP

data Startup = Startup !CqlVersion !CompressionAlgorithm deriving (Show)

instance Encoding Startup where
    encode (Startup v c) =
        encode $ ("CQL_VERSION", mapVersion v) : mapCompression c
      where
        mapVersion :: CqlVersion -> Text
        mapVersion Cqlv300        = "3.0.0"
        mapVersion (CqlVersion s) = s

        mapCompression :: CompressionAlgorithm -> [(Text, Text)]
        mapCompression Snappy = [("COMPRESSION", "snappy")]
        mapCompression LZ4    = [("COMPRESSION", "lz4")]
        mapCompression None   = []

------------------------------------------------------------------------------
-- AUTH_RESPONSE

newtype AuthResponse = AuthResponse LB.ByteString deriving (Show)

instance Encoding AuthResponse where
    encode (AuthResponse b) = encode b

------------------------------------------------------------------------------
-- OPTIONS

data Options = Options deriving (Show)

instance Encoding Options where
    encode _ = return ()

------------------------------------------------------------------------------
-- QUERY

data Query k a b = Query !(QueryString k a b) !(QueryParams a) deriving (Show)

instance (Tuple a) => Encoding (Query k a b) where
    encode (Query (QueryString s) p) = encode s >> encode p

------------------------------------------------------------------------------
-- EXECUTE

data Execute k a b = Execute !(QueryId k a b) !(QueryParams a) deriving (Show)

instance (Tuple a) => Encoding (Execute k a b) where
    encode (Execute (QueryId q) p) = encode q >> encode p

------------------------------------------------------------------------------
-- PREPARE

newtype Prepare k a b = Prepare (QueryString k a b) deriving (Show)

instance Encoding (Prepare k a b) where
    encode (Prepare (QueryString p)) = encode p

------------------------------------------------------------------------------
-- REGISTER

newtype Register = Register [EventType] deriving (Show)

instance Encoding Register where
    encode (Register t) = do
        encode (fromIntegral (length t) :: Word8)
        mapM_ encode t

data EventType
    = TopologyChangeEvent
    | StatusChangeEvent
    | SchemaChangeEvent
    deriving (Show)

instance Encoding EventType where
    encode TopologyChangeEvent = encode ("TOPOLOGY_CHANGE" :: Text)
    encode StatusChangeEvent   = encode ("STATUS_CHANGE"   :: Text)
    encode SchemaChangeEvent   = encode ("SCHEMA_CHANGE"   :: Text)

------------------------------------------------------------------------------
-- BATCH

data Batch = Batch !BatchType [BatchQuery] !Consistency deriving (Show)

instance Encoding Batch where
    encode (Batch t q c) = do
        encode t
        encode (fromIntegral (length q) :: Word16)
        mapM_ encode q
        encode c

data BatchType
    = BatchLogged
    | BatchUnLogged
    | BatchCounter
    deriving (Show)

instance Encoding BatchType where
    encode BatchLogged   = putWord8 0
    encode BatchUnLogged = putWord8 1
    encode BatchCounter  = putWord8 2

data BatchQuery where
    BatchQuery :: (Show a, Tuple a, Tuple b)
               => !(QueryString W a b)
               -> !a
               -> BatchQuery

    BatchPrepared :: (Show a, Tuple a, Tuple b)
                  => !(QueryId W a b)
                  -> !a
                  -> BatchQuery

deriving instance Show BatchQuery

instance Encoding BatchQuery where
    encode (BatchQuery (QueryString q) v) = do
        putWord8 0
        encode q
        store v
    encode (BatchPrepared (QueryId i) v)  = do
        putWord8 1
        encode i
        store v

------------------------------------------------------------------------------
-- Query Parameters

data QueryParams a = QueryParams
    { consistency       :: !Consistency
    , skipMetaData      :: !Bool
    , values            :: a
    , pageSize          :: Maybe Int32
    , queryPagingState  :: Maybe PagingState
    , serialConsistency :: Maybe SerialConsistency
    } deriving (Show)

data SerialConsistency
    = SerialConsistency
    | LocalSerialConsistency
    deriving (Show)

instance (Tuple a) => Encoding (QueryParams a) where
    encode p = do
        encode (consistency p)
        put queryFlags
        store (values p)
        encodeMaybe (pageSize p)
        encodeMaybe (queryPagingState p)
        encodeMaybe (mapCons <$> serialConsistency p)
      where
        queryFlags :: Word8
        queryFlags =
                (if hasValues                    then 0x01 else 0x0)
            .|. (if skipMetaData p               then 0x02 else 0x0)
            .|. (if isJust (pageSize p)          then 0x04 else 0x0)
            .|. (if isJust (queryPagingState p)  then 0x08 else 0x0)
            .|. (if isJust (serialConsistency p) then 0x10 else 0x0)

        mapCons SerialConsistency      = Serial
        mapCons LocalSerialConsistency = LocalSerial

        hasValues = untag (count :: Tagged a Int) /= 0
