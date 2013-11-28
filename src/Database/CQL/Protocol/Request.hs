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

class (Encoding a) => Request a where
    rqCode :: Tagged a OpCode

instance Request Startup      where rqCode = Tagged OcStartup
instance Request Options      where rqCode = Tagged OcOptions
instance Request Register     where rqCode = Tagged OcRegister
instance Request Batch        where rqCode = Tagged OcBatch
instance Request AuthResponse where rqCode = Tagged OcAuthResponse

instance (Tuple a) => Request (Prepare a) where
    rqCode = Tagged OcPrepare

instance (Tuple a) => Request (Query a) where
    rqCode = Tagged OcQuery

instance (Tuple a) => Request (Execute a) where
    rqCode = Tagged OcExecute

pack :: (Request r)
     => Compression
     -> Bool
     -> StreamId
     -> r
     -> Either String ByteString
pack c t i r = do
    body <- runCompression c (encWriteLazy r)
    let len = Length . fromIntegral $ LB.length body
    let hdr = Header RqHeader V2 mkFlags i (getOpCode r rqCode) len
    return . runPutLazy $ encode hdr >> putLazyByteString body
  where
    runCompression f x = maybe compressError return (shrink f $ x)
    compressError      = Left "pack: compression failure"

    mkFlags = (if t then tracing else mempty)
        <> (if algorithm c /= None then compress else mempty)

getOpCode :: (Request r) => r -> Tagged r OpCode -> OpCode
getOpCode _ = untag

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

data Query a = Query !(QueryString a) !(QueryParams a) deriving (Show)

instance (Tuple a) => Encoding (Query a) where
    encode (Query (QueryString s) p) = encode s >> encode p

------------------------------------------------------------------------------
-- EXECUTE

data Execute a = Execute !(QueryId a) !(QueryParams a) deriving (Show)

instance (Tuple a) => Encoding (Execute a) where
    encode (Execute (QueryId q) p) = encode q >> encode p

------------------------------------------------------------------------------
-- PREPARE

newtype Prepare a = Prepare (QueryString a) deriving (Show)

instance Encoding (Prepare a) where
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
    BatchQuery    :: (Show a, Tuple a) => !(QueryString a) -> !a -> BatchQuery
    BatchPrepared :: (Show a, Tuple a) => !(QueryId a)     -> !a -> BatchQuery

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
