-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQL.Protocol.V2.Response
    ( Response       (..)
    , Ready          (..)

    , Authenticate   (..)
    , AuthChallenge  (..)
    , AuthSuccess    (..)

    , Result         (..)
    , Supported      (..)

    , Event          (..)
    , TopologyChange (..)
    , SchemaChange   (..)
    , StatusChange   (..)

    , Error          (..)

    , MetaData       (..)
    , ColumnSpec     (..)
    , WriteType      (..)

    , unpack
    ) where

import Control.Applicative
import Control.Exception (Exception)
import Control.Monad
import Data.Bits
import Data.ByteString (ByteString)
import Data.Int
import Data.Maybe (fromMaybe)
import Data.Tagged
import Data.Text (Text)
import Data.Serialize hiding (decode, encode, Result)
import Data.Typeable
import Data.UUID (UUID)
import Data.Word
import Database.CQL.Protocol.Tuple
import Database.CQL.Protocol.Codec
import Database.CQL.Protocol.Types
import Database.CQL.Protocol.V2.Header
import Network.Socket (SockAddr)

import qualified Data.ByteString.Lazy as LB

------------------------------------------------------------------------------
-- Response

data Response k a b
    = RsError         (Maybe UUID) !Error
    | RsReady         (Maybe UUID) !Ready
    | RsAuthenticate  (Maybe UUID) !Authenticate
    | RsAuthChallenge (Maybe UUID) !AuthChallenge
    | RsAuthSuccess   (Maybe UUID) !AuthSuccess
    | RsSupported     (Maybe UUID) !Supported
    | RsResult        (Maybe UUID) !(Result k a b)
    | RsEvent         (Maybe UUID) !Event
    deriving Show

unpack :: (Tuple a, Tuple b)
       => Compression
       -> Header
       -> LB.ByteString
       -> Either String (Response k a b)
unpack c h b = do
    let f = flags h
    x <- if compress `isSet` f then deflate c b else return b
    flip runGetLazy x $ do
        t <- if tracing `isSet` f then Just <$> decodeUUID else return Nothing
        message t (opCode h)
  where
    message t OcError         = RsError         t <$> decodeError
    message t OcReady         = RsReady         t <$> decodeReady
    message t OcAuthenticate  = RsAuthenticate  t <$> decodeAuthenticate
    message t OcSupported     = RsSupported     t <$> decodeSupported
    message t OcResult        = RsResult        t <$> decodeResult
    message t OcEvent         = RsEvent         t <$> decodeEvent
    message t OcAuthChallenge = RsAuthChallenge t <$> decodeAuthChallenge
    message t OcAuthSuccess   = RsAuthSuccess   t <$> decodeAuthSuccess
    message _ other           = fail $
        "decode-response: unknown: " ++ show other

    deflate f x  = maybe deflateError return (expand f x)
    deflateError = Left "unpack: decompression failure"

------------------------------------------------------------------------------
-- AUTHENTICATE

newtype Authenticate = Authenticate Text deriving Show

decodeAuthenticate :: Get Authenticate
decodeAuthenticate = Authenticate <$> decodeString

------------------------------------------------------------------------------
-- AUTH_CHALLENGE

newtype AuthChallenge = AuthChallenge (Maybe LB.ByteString) deriving Show

decodeAuthChallenge :: Get AuthChallenge
decodeAuthChallenge = AuthChallenge <$> decodeBytes

------------------------------------------------------------------------------
-- AUTH_SUCCESS

newtype AuthSuccess = AuthSuccess (Maybe LB.ByteString) deriving Show

decodeAuthSuccess :: Get AuthSuccess
decodeAuthSuccess = AuthSuccess <$> decodeBytes

------------------------------------------------------------------------------
-- READY

data Ready = Ready deriving Show

decodeReady :: Get Ready
decodeReady = return Ready

------------------------------------------------------------------------------
-- SUPPORTED

data Supported = Supported [CompressionAlgorithm] [CqlVersion] deriving Show

decodeSupported :: Get Supported
decodeSupported = do
    opt <- decodeMultiMap
    cmp <- mapM toCompression . fromMaybe [] $ lookup "COMPRESSION" opt
    let v = map toVersion . fromMaybe [] $ lookup "CQL_VERSION" opt
    return $ Supported cmp v
  where
    toCompression "snappy" = return Snappy
    toCompression "lz4"    = return LZ4
    toCompression other    = fail $
        "decode-supported: unknown compression: " ++ show other

    toVersion "3.0.0" = Cqlv300
    toVersion other   = CqlVersion other

------------------------------------------------------------------------------
-- RESULT

data Result k a b
    = VoidResult
    | RowsResult         !MetaData [b]
    | SetKeyspaceResult  !Keyspace
    | PreparedResult     !(QueryId k a b) !MetaData !MetaData
    | SchemaChangeResult !SchemaChange
    deriving (Show)

data MetaData = MetaData
    { columnCount :: !Int32
    , pagingState :: Maybe PagingState
    , columnSpecs :: [ColumnSpec]
    } deriving (Show)

data ColumnSpec = ColumnSpec
    { keyspace   :: !Keyspace
    , table      :: !Table
    , columnName :: !Text
    , columnType :: !ColumnType
    } deriving (Show)

decodeResult :: forall a b k. (Tuple a, Tuple b) => Get (Result k a b)
decodeResult = decodeInt >>= go
  where
    go 0x1 = return VoidResult
    go 0x2 = do
        m <- decodeMetaData
        n <- decodeInt
        let c = untag (count :: Tagged b Int)
        unless (columnCount m == fromIntegral c) $
            fail $ "column count: "
                ++ show (columnCount m)
                ++ " =/= "
                ++ show c
        let typecheck = untag (check :: Tagged b ([ColumnType] -> [ColumnType]))
        let ctypes    = map columnType (columnSpecs m)
        let expected  = typecheck ctypes
        let message   = "expected: " ++ show expected ++ ", but got " ++ show ctypes
        unless (null expected) $
            fail $ "column-type error: " ++ message
        RowsResult m <$> replicateM (fromIntegral n) tuple
    go 0x3 = SetKeyspaceResult <$> decodeKeyspace
    go 0x4 = PreparedResult <$> decodeQueryId <*> decodeMetaData <*> decodeMetaData
    go 0x5 = SchemaChangeResult <$> decodeSchemaChange
    go int = fail $ "decode-result: unknown: " ++ show int

decodeMetaData :: Get MetaData
decodeMetaData = do
    f <- decodeInt
    n <- decodeInt
    p <- if hasMorePages f then decodePagingState else return Nothing
    if hasNoMetaData f
        then return $ MetaData n p []
        else MetaData n p <$> decodeSpecs n (hasGlobalSpec f)
  where
    hasGlobalSpec f = f `testBit` 0
    hasMorePages  f = f `testBit` 1
    hasNoMetaData f = f `testBit` 2

    decodeSpecs n True = do
        k <- decodeKeyspace
        t <- decodeTable
        replicateM (fromIntegral n) $ ColumnSpec k t
            <$> decodeString
            <*> decodeColumnType

    decodeSpecs n False =
        replicateM (fromIntegral n) $ ColumnSpec
            <$> decodeKeyspace
            <*> decodeTable
            <*> decodeString
            <*> decodeColumnType

------------------------------------------------------------------------------
-- SCHEMA_CHANGE

data SchemaChange
    = SchemaCreated !Keyspace !Table
    | SchemaUpdated !Keyspace !Table
    | SchemaDropped !Keyspace !Table
    deriving (Show)

decodeSchemaChange :: Get SchemaChange
decodeSchemaChange = decodeString >>= fromString
  where
    fromString "CREATED" = SchemaCreated <$> decodeKeyspace <*> decodeTable
    fromString "UPDATED" = SchemaUpdated <$> decodeKeyspace <*> decodeTable
    fromString "DROPPED" = SchemaDropped <$> decodeKeyspace <*> decodeTable
    fromString other     = fail $
        "decode-schema-change: unknown: " ++ show other

------------------------------------------------------------------------------
-- EVENT

data Event
    = TopologyEvent !TopologyChange !SockAddr
    | StatusEvent   !StatusChange   !SockAddr
    | SchemaEvent   !SchemaChange
    deriving Show

data TopologyChange = NewNode | RemovedNode deriving Show
data StatusChange   = Up | Down deriving Show

decodeEvent :: Get Event
decodeEvent = decodeString >>= decodeByType
  where
    decodeByType "TOPOLOGY_CHANGE" = TopologyEvent <$> decodeTopologyChange <*> decodeSockAddr
    decodeByType "STATUS_CHANGE"   = StatusEvent   <$> decodeStatusChange <*> decodeSockAddr
    decodeByType "SCHEMA_CHANGE"   = SchemaEvent   <$> decodeSchemaChange
    decodeByType other             = fail $
        "decode-event: unknown: " ++ show other

decodeTopologyChange :: Get TopologyChange
decodeTopologyChange = decodeString >>= fromString
  where
    fromString "NEW_NODE"     = return NewNode
    fromString "REMOVED_NODE" = return RemovedNode
    fromString other          = fail $
        "decode-topology: unknown: "  ++ show other

decodeStatusChange :: Get StatusChange
decodeStatusChange = decodeString >>= fromString
  where
    fromString "UP"   = return Up
    fromString "DOWN" = return Down
    fromString other  = fail $
        "decode-status-change: unknown: " ++ show other

-----------------------------------------------------------------------------
-- ERROR

data Error
    = AlreadyExists   !Text !Keyspace !Table
    | BadCredentials  !Text
    | ConfigError     !Text
    | Invalid         !Text
    | IsBootstrapping !Text
    | Overloaded      !Text
    | ProtocolError   !Text
    | ServerError     !Text
    | SyntaxError     !Text
    | TruncateError   !Text
    | Unauthorized    !Text
    | Unprepared      !Text !ByteString
    | Unavailable
        { unavailMessage     :: !Text
        , unavailConsistency :: !Consistency
        , unavailNumRequired :: !Int32
        , unavailNumAlive    :: !Int32
        }
    | ReadTimeout
        { rTimeoutMessage     :: !Text
        , rTimeoutConsistency :: !Consistency
        , rTimeoutNumAck      :: !Int32
        , rTimeoutNumRequired :: !Int32
        , rTimeoutDataPresent :: !Bool
        }
    | WriteTimeout
        { wTimeoutMessage     :: !Text
        , wTimeoutConsistency :: !Consistency
        , wTimeoutNumAck      :: !Int32
        , wTimeoutNumRequired :: !Int32
        , wTimeoutWriteType   :: !WriteType
        }
    deriving (Eq, Show, Typeable)

instance Exception Error

data WriteType
    = WriteSimple
    | WriteBatch
    | WriteBatchLog
    | WriteUnloggedBatch
    | WriteCounter
    deriving (Eq, Show)

decodeError :: Get Error
decodeError = do
    code <- decodeInt
    msg  <- decodeString
    toError code msg
  where
    toError :: Int32 -> Text -> Get Error
    toError 0x0000 m = return $ ServerError m
    toError 0x000A m = return $ ProtocolError m
    toError 0x0100 m = return $ BadCredentials m
    toError 0x1000 m = Unavailable m <$> decodeConsistency <*> decodeInt <*> decodeInt
    toError 0x1001 m = return $ Overloaded m
    toError 0x1002 m = return $ IsBootstrapping m
    toError 0x1003 m = return $ TruncateError m
    toError 0x1100 m = WriteTimeout m
        <$> decodeConsistency
        <*> decodeInt
        <*> decodeInt
        <*> decodeWriteType
    toError 0x1200 m = ReadTimeout m
        <$> decodeConsistency
        <*> decodeInt
        <*> decodeInt
        <*> (bool <$> decodeByte)
    toError 0x2000 m = return $ SyntaxError m
    toError 0x2100 m = return $ Unauthorized m
    toError 0x2200 m = return $ Invalid m
    toError 0x2300 m = return $ ConfigError m
    toError 0x2400 m = AlreadyExists m <$> decodeKeyspace <*> decodeTable
    toError 0x2500 m = Unprepared m <$> decodeShortBytes
    toError code _   = fail $ "decode-error: unknown: " ++ show code

    bool :: Word8 -> Bool
    bool 0 = False
    bool _ = True

decodeWriteType :: Get WriteType
decodeWriteType = decodeString >>= fromString
  where
    fromString "SIMPLE"          = return WriteSimple
    fromString "BATCH"           = return WriteBatch
    fromString "BATCH_LOG"       = return WriteBatchLog
    fromString "UNLOGGED_BATCH"  = return WriteUnloggedBatch
    fromString "COUNTER"         = return WriteCounter
    fromString unknown           = fail $
        "decode: unknown write-type: " ++ show unknown
