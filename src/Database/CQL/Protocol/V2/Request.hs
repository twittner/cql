-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}

module Database.CQL.Protocol.V2.Request
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
    , encodeRequest
    ) where

import Control.Applicative
import Data.Bits
import Data.ByteString.Lazy (ByteString)
import Data.Foldable (traverse_)
import Data.Int
import Data.Tagged
import Data.Text (Text)
import Data.Maybe (isJust)
import Data.Monoid
import Data.Serialize hiding (decode, encode)
import Data.Word
import Database.CQL.Protocol.Tuple
import Database.CQL.Protocol.Codec
import Database.CQL.Protocol.Types
import Database.CQL.Protocol.V2.Header

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
    deriving Show

encodeRequest :: Tuple a => Putter (Request k a b)
encodeRequest (RqStartup  r) = encodeStartup r
encodeRequest (RqOptions  r) = encodeOptions r
encodeRequest (RqRegister r) = encodeRegister r
encodeRequest (RqBatch    r) = encodeBatch r
encodeRequest (RqAuthResp r) = encodeAuthResponse r
encodeRequest (RqPrepare  r) = encodePrepare r
encodeRequest (RqQuery    r) = encodeQuery r
encodeRequest (RqExecute  r) = encodeExecute r

pack :: (Tuple a)
     => Compression
     -> Bool
     -> StreamId
     -> Request k a b
     -> Either String ByteString
pack c t i r = do
    body <- runCompression c (runPutLazy $ encodeRequest r)
    let len = Length . fromIntegral $ LB.length body
    let hdr = Header RqHeader V2 mkFlags i (getOpCode r) len
    return . runPutLazy $ encodeHeader hdr >> putLazyByteString body
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

data Startup = Startup !CqlVersion !CompressionAlgorithm deriving Show

encodeStartup :: Putter Startup
encodeStartup (Startup v c) =
    encodeMap $ ("CQL_VERSION", mapVersion v) : mapCompression c
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

newtype AuthResponse = AuthResponse LB.ByteString deriving Show

encodeAuthResponse :: Putter AuthResponse
encodeAuthResponse (AuthResponse b) = encodeBytes b

------------------------------------------------------------------------------
-- OPTIONS

data Options = Options deriving Show

encodeOptions :: Putter Options
encodeOptions _ = return ()

------------------------------------------------------------------------------
-- QUERY

data Query k a b = Query !(QueryString k a b) !(QueryParams a) deriving Show

encodeQuery :: Tuple a => Putter (Query k a b)
encodeQuery (Query (QueryString s) p) =
    encodeLongString s >> encodeQueryParams p

------------------------------------------------------------------------------
-- EXECUTE

data Execute k a b = Execute !(QueryId k a b) !(QueryParams a) deriving Show

encodeExecute :: Tuple a => Putter (Execute k a b)
encodeExecute (Execute (QueryId q) p) =
    encodeShortBytes q >> encodeQueryParams p

------------------------------------------------------------------------------
-- PREPARE

newtype Prepare k a b = Prepare (QueryString k a b) deriving Show

encodePrepare :: Putter (Prepare k a b)
encodePrepare (Prepare (QueryString p)) = encodeLongString p

------------------------------------------------------------------------------
-- REGISTER

newtype Register = Register [EventType] deriving Show

encodeRegister :: Putter Register
encodeRegister (Register t) = do
    encodeByte (fromIntegral (length t))
    mapM_ encodeEventType t

data EventType
    = TopologyChangeEvent
    | StatusChangeEvent
    | SchemaChangeEvent
    deriving Show

encodeEventType :: Putter EventType
encodeEventType TopologyChangeEvent = encodeString "TOPOLOGY_CHANGE"
encodeEventType StatusChangeEvent   = encodeString "STATUS_CHANGE"
encodeEventType SchemaChangeEvent   = encodeString "SCHEMA_CHANGE"

------------------------------------------------------------------------------
-- BATCH

data Batch = Batch !BatchType [BatchQuery] !Consistency deriving Show

encodeBatch :: Putter Batch
encodeBatch (Batch t q c) = do
    encodeBatchType t
    encodeShort (fromIntegral (length q))
    mapM_ encodeBatchQuery q
    encodeConsistency c

data BatchType
    = BatchLogged
    | BatchUnLogged
    | BatchCounter
    deriving (Show)

encodeBatchType :: Putter BatchType
encodeBatchType BatchLogged   = putWord8 0
encodeBatchType BatchUnLogged = putWord8 1
encodeBatchType BatchCounter  = putWord8 2

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

encodeBatchQuery :: Putter BatchQuery
encodeBatchQuery (BatchQuery (QueryString q) v) = do
    putWord8 0
    encodeLongString q
    store v
encodeBatchQuery (BatchPrepared (QueryId i) v)  = do
    putWord8 1
    encodeShortBytes i
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
    } deriving Show

data SerialConsistency
    = SerialConsistency
    | LocalSerialConsistency
    deriving Show

encodeQueryParams :: forall a. Tuple a => Putter (QueryParams a)
encodeQueryParams p = do
    encodeConsistency (consistency p)
    put queryFlags
    store (values p)
    traverse_ encodeInt         (pageSize p)
    traverse_ encodePagingState (queryPagingState p)
    traverse_ encodeConsistency (mapCons <$> serialConsistency p)
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
