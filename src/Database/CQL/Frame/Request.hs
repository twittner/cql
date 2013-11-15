-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.Frame.Request
    ( Request           (..)
    , BatchType         (..)
    , BatchQuery        (..)
    , Compression       (..)
    , CqlVersion        (..)
    , EventType         (..)
    , QueryParams       (..)
    , SerialConsistency (..)
    , Options           (..)
    , Startup           (..)
    , pack
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
import Database.CQL.Class
import Database.CQL.Frame.Codec
import Database.CQL.Frame.Header
import Database.CQL.Frame.Types

import qualified Data.ByteString.Lazy as LB
import qualified Data.Text.Lazy       as LT

------------------------------------------------------------------------------
-- Request

class (Encoding a) => Request a where
    rqCode :: Tagged a OpCode

instance Request Startup      where rqCode = Tagged OcStartup
instance Request Options      where rqCode = Tagged OcOptions
instance Request Query        where rqCode = Tagged OcQuery
instance Request Execute      where rqCode = Tagged OcExecute
instance Request Prepare      where rqCode = Tagged OcPrepare
instance Request Register     where rqCode = Tagged OcRegister
instance Request Batch        where rqCode = Tagged OcBatch
instance Request AuthResponse where rqCode = Tagged OcAuthResponse

pack :: (Request r) => Compression -> Bool -> StreamId -> r -> ByteString
pack c t i r =
    let body = compress c (encWriteLazy r)
        len  = Length . fromIntegral $ LB.length body
        hdr  = Header RqHeader V2 mkFlags i (getOpCode r rqCode) len
    in runPutLazy $ encode hdr >> putLazyByteString body
  where
    compress (Snappy f) a = f a
    compress (ZLib   f) a = f a
    compress None       a = a

    mkFlags = (if t then tracing else mempty)
        <> (if c /= None then compression else mempty)

    getOpCode :: (Request r) => r -> Tagged r OpCode -> OpCode
    getOpCode _ = unTagged

------------------------------------------------------------------------------
-- STARTUP

data Startup = Startup !CqlVersion !Compression deriving (Show)

instance Encoding Startup where
    encode (Startup v c) =
        encode $ ("CQL_VERSION", mapVersion v) : mapCompression c
      where
        mapVersion :: CqlVersion -> Text
        mapVersion Cqlv300 = "3.0.0"

        mapCompression :: Compression -> [(Text, Text)]
        mapCompression (Snappy _) = [("COMPRESSION", "snappy")]
        mapCompression (ZLib _)   = [("COMPRESSION", "zlib")]
        mapCompression None       = []

data CqlVersion = Cqlv300 deriving (Eq, Show)

data Compression
    = Snappy (ByteString -> ByteString)
    | ZLib   (ByteString -> ByteString)
    | None

instance Show Compression where
    show (Snappy _) = "snappy"
    show (ZLib   _) = "zlib"
    show None       = "none"

instance Eq Compression where
    (Snappy _) == (Snappy _) = True
    (ZLib   _) == (ZLib   _) = True
    None       == None       = True
    _          == _          = False

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

data Query = Query !QueryString !QueryParams deriving (Show)

instance Encoding Query where
    encode (Query (QueryString s) p) = encode s >> encode p

------------------------------------------------------------------------------
-- EXECUTE

data Execute = Execute !QueryId !QueryParams deriving (Show)

instance Encoding Execute where
    encode (Execute (QueryId q) p) = encode q >> encode p

------------------------------------------------------------------------------
-- PREPARE

newtype Prepare = Prepare LT.Text deriving (Show)

instance Encoding Prepare where
    encode (Prepare p) = encode p

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

data BatchQuery
    = BatchQuery    !QueryString [Value]
    | BatchPrepared !QueryId     [Value]
    deriving (Show)

instance Encoding BatchQuery where
    encode (BatchQuery (QueryString q) vv) = do
        putWord8 0
        encode q
        toCql vv
    encode (BatchPrepared (QueryId i) vv)  = do
        putWord8 1
        encode i
        toCql vv

------------------------------------------------------------------------------
-- Query Parameters

data QueryParams = QueryParams
    { consistency       :: !Consistency
    , skipMetaData      :: !Bool
    , values            :: [Value]
    , pageSize          :: Maybe Int32
    , queryPagingState  :: Maybe PagingState
    , serialConsistency :: Maybe SerialConsistency
    } deriving (Show)

data SerialConsistency
    = SerialConsistency
    | LocalSerialConsistency
    deriving (Show)

instance Encoding QueryParams where
    encode p = do
        encode      . consistency $ p
        put queryFlags
        toCql       . values $ p
        encodeMaybe . pageSize $ p
        encodeMaybe . queryPagingState $ p
        encodeMaybe $ mapCons <$> serialConsistency p
      where
        queryFlags :: Word8
        queryFlags =
                (if not (null (values p))        then 0x01 else 0x0)
            .|. (if skipMetaData p               then 0x02 else 0x0)
            .|. (if isJust (pageSize p)          then 0x04 else 0x0)
            .|. (if isJust (queryPagingState p)  then 0x08 else 0x0)
            .|. (if isJust (serialConsistency p) then 0x10 else 0x0)

        mapCons SerialConsistency      = Serial
        mapCons LocalSerialConsistency = LocalSerial
