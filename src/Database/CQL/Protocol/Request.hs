-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeOperators       #-}

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
    , getOpCode
    , pack2
    , pack3
    ) where

import Control.Applicative
import Data.Bits
import Data.ByteString.Lazy (ByteString)
import Data.Foldable (traverse_)
import Data.Int
import Data.Text (Text)
import Data.Maybe (isJust)
import Data.Monoid
import Data.Serialize hiding (decode, encode)
import Data.Word
import Database.CQL.Protocol.Tuple
import Database.CQL.Protocol.Codec
import Database.CQL.Protocol.Types
import Database.CQL.Protocol.Header
import Data.Singletons.TypeLits (Nat)

import qualified Data.ByteString.Lazy as LB

------------------------------------------------------------------------------
-- Request

data Request (v :: Nat) k a b where
    RqStartup  :: Startup       -> Request v k a b
    RqOptions  :: Options       -> Request v k a b
    RqRegister :: Register      -> Request v k a b
    RqBatch    :: Batch v       -> Request v k a b
    RqAuthResp :: AuthResponse  -> Request v k a b
    RqPrepare  :: Prepare k a b -> Request v k a b
    RqQuery    :: Query k a b   -> Request v k a b
    RqExecute  :: Execute k a b -> Request v k a b

encodeRequest :: Tuple v a => Codec v -> Putter (Request v k a b)
encodeRequest _ (RqStartup  r) = encodeStartup r
encodeRequest _ (RqOptions  r) = encodeOptions r
encodeRequest _ (RqRegister r) = encodeRegister r
encodeRequest c (RqBatch    r) = encodeBatch c r
encodeRequest _ (RqAuthResp r) = encodeAuthResponse r
encodeRequest _ (RqPrepare  r) = encodePrepare r
encodeRequest c (RqQuery    r) = encodeQuery c r
encodeRequest c (RqExecute  r) = encodeExecute c r

pack2 :: (v :<: 3, Tuple v a) => Compression -> Bool -> StreamId v -> Request v k a b -> Either String ByteString
pack2 = pack encodeHeader2 codec2

pack3 :: (v :>=: 3, Tuple v a) => Compression -> Bool -> StreamId v -> Request v k a b -> Either String ByteString
pack3 = pack encodeHeader3 codec3

pack :: Tuple v a
     => (HeaderType -> Flags -> StreamId v -> OpCode -> Length -> PutM ())
     -> Codec v
     -> Compression
     -> Bool
     -> StreamId v
     -> Request v k a b
     -> Either String ByteString
pack enc codec c t i r = do
    body <- runCompression c (runPutLazy $ encodeRequest codec r)
    let len = Length . fromIntegral $ LB.length body
    return . runPutLazy $ do
        enc RqHeader mkFlags i (getOpCode r) len
        putLazyByteString body
  where
    runCompression f x = maybe compressError return (shrink f $ x)
    compressError      = Left "pack: compression failure"

    mkFlags = (if t then tracing else mempty)
        <> (if algorithm c /= None then compress else mempty)

getOpCode :: Request v k a b -> OpCode
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

encodeQuery :: Tuple v a => Codec v -> Putter (Query k a b)
encodeQuery c (Query (QueryString s) p) =
    encodeLongString s >> encodeQueryParams c p

------------------------------------------------------------------------------
-- EXECUTE

data Execute k a b = Execute !(QueryId k a b) !(QueryParams a) deriving Show

encodeExecute :: Tuple v a => Codec v -> Putter (Execute k a b)
encodeExecute c (Execute (QueryId q) p) =
    encodeShortBytes q >> encodeQueryParams c p

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

data Batch (v :: Nat) where
    B2 :: (v :<:  3) => BatchType -> [BatchQuery v] -> Consistency -> Batch v
    B3 :: (v :>=: 3) => BatchType -> [BatchQuery v] -> Consistency -> BatchFlags -> Batch v

data BatchType
    = BatchLogged
    | BatchUnLogged
    | BatchCounter
    deriving (Show)

data BatchFlags = BatchFlags
    { batchSerialConsistency :: Maybe SerialConsistency
    } deriving Show

encodeBatch :: Codec v -> Putter (Batch v)
encodeBatch k (B2 t q c) = do
    encodeBatchType t
    encodeShort (fromIntegral (length q))
    mapM_ (encodeBatchQuery k) q
    encodeConsistency c
encodeBatch k (B3 t q c f) = do
    encodeBatchType t
    encodeShort (fromIntegral (length q))
    mapM_ (encodeBatchQuery k) q
    encodeConsistency c
    put (batchFlags f)
    traverse_ encodeConsistency (mapCons <$> batchSerialConsistency f)

batchFlags :: BatchFlags -> Word8
batchFlags b = if isJust (batchSerialConsistency b) then 0x10 else 0x0

encodeBatchType :: Putter BatchType
encodeBatchType BatchLogged   = putWord8 0
encodeBatchType BatchUnLogged = putWord8 1
encodeBatchType BatchCounter  = putWord8 2

data BatchQuery (v :: Nat) where
    BatchQuery :: (Show a, Tuple v a, Tuple v b)
               => !(QueryString W a b)
               -> !a
               -> BatchQuery v

    BatchPrepared :: (Show a, Tuple v a, Tuple v b)
                  => !(QueryId W a b)
                  -> !a
                  -> BatchQuery v

deriving instance Show (BatchQuery v)

encodeBatchQuery :: Codec v -> Putter (BatchQuery v)
encodeBatchQuery c (BatchQuery (QueryString q) v) = do
    putWord8 0
    encodeLongString q
    store c v
encodeBatchQuery c (BatchPrepared (QueryId i) v)  = do
    putWord8 1
    encodeShortBytes i
    store c v

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

encodeQueryParams :: forall a v. Tuple v a => Codec v -> Putter (QueryParams a)
encodeQueryParams c p = do
    encodeConsistency (consistency p)
    put queryFlags
    store c (values p)
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

    hasValues = untag (count :: Tagged v a Int) /= 0

mapCons :: SerialConsistency -> Consistency
mapCons SerialConsistency      = Serial
mapCons LocalSerialConsistency = LocalSerial
