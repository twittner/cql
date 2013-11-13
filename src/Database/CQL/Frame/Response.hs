-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.Frame.Response
    ( Response           (..)
    , ResponseMessage    (..)
    , ColumnSpec         (..)
    , ColumnType         (..)
    , ErrorData          (..)
    , EventData          (..)
    , MetaData           (..)
    , ResultData         (..)
    , SchemaChangeData   (..)
    , StatusChangeType   (..)
    , TopologyChangeType (..)
    , WriteType          (..)
    , response
    ) where

import Control.Applicative
import Control.Monad
import Data.Bits
import Data.Int
import Data.Text (Text)
import Data.Serialize hiding (decode, encode)
import Data.UUID (UUID)
import Data.Word
import Database.CQL.Frame.Codec
import Database.CQL.Frame.Header
import Database.CQL.Frame.Types
import Network.Socket (SockAddr)

import qualified Data.ByteString.Lazy as LB

------------------------------------------------------------------------------
-- Response

data Response = Response
    { rsHeader    :: !Header
    , rsTracingId :: Maybe UUID
    , rsBody      :: !ResponseMessage
    }

response :: (LB.ByteString -> LB.ByteString)
         -> LB.ByteString
         -> Either String Response
response deflate bytes = do
    let (h, b) = LB.splitAt 8 bytes
    unless (LB.length h == 8) $
        Left "input too short"
    hdr <- decReadLazy h
    unless (isResponse hdr) $
        Left "not a response"
    runGetLazy (decodeResponse hdr) (body hdr b)
  where
    decodeResponse h = do
        t <- if isTracing h then Just <$> decode else return Nothing
        Response h t <$> decode

    body h = if isCompressed h then deflate else id
           . LB.take (fromIntegral (hdrLength h))

------------------------------------------------------------------------------
-- Response Message

data ResponseMessage
    = Error         !ErrorData
    | Ready
    | Authenticate  !Text
    | Supported     [(Text, [Text])]
    | Result        !ResultData
    | Event         !EventData
    | AuthChallenge (Maybe LB.ByteString)
    | AuthSuccess   (Maybe LB.ByteString)

instance Decoding ResponseMessage where
    decode = decode >>= fromOpCode
      where
        fromOpCode :: Word8 -> Get ResponseMessage
        fromOpCode 0x00  = Error <$> decode
        fromOpCode 0x02  = return Ready
        fromOpCode 0x03  = Authenticate  <$> decode
        fromOpCode 0x06  = Supported     <$> decode -- TODO: use known options
        fromOpCode 0x08  = Result        <$> decode
        fromOpCode 0x0C  = Event         <$> decode
        fromOpCode 0x0E  = AuthChallenge <$> decode
        fromOpCode 0x10  = AuthSuccess   <$> decode
        fromOpCode other = fail $ "decode-response: unknown: " ++ show other

------------------------------------------------------------------------------
-- Result

data ResultData
    = Void
    | Rows         !MetaData [Row]
    | SetKeyspace  !Keyspace
    | Prepared     !QueryId !MetaData !MetaData
    | SchemaChange !SchemaChangeData

instance Decoding ResultData where
    decode = decode >>= decodeResult
      where
        decodeResult :: Int32 -> Get ResultData
        decodeResult 0x1 = return Void
        decodeResult 0x2 = do
            m <- decode
            r <- decode :: Get Int32
            Rows m <$> decodeRows (mdColumnCount m) r
        decodeResult 0x3 = SetKeyspace . Keyspace <$> decode
        decodeResult 0x4 = Prepared . QueryId
            <$> decode
            <*> decode
            <*> decode
        decodeResult 0x5 = SchemaChange <$> decode
        decodeResult int = fail $ "decode-result: unknown: " ++ show int

        decodeRows c r = replicateM (fromIntegral r)
                       . fmap Row
                       . replicateM (fromIntegral c)
                       . fmap Cell
                       $ decode

------------------------------------------------------------------------------
-- Meta Data

data MetaData = MetaData
    { mdColumnCount :: !Int32
    , mdPagingState :: Maybe PagingState
    , mdColumnSpecs :: [ColumnSpec]
    }

data ColumnSpec = ColumnSpec
    { colKeyspace :: !Keyspace
    , colTable    :: !Table
    , colName     :: !Text
    , colType     :: !ColumnType
    }

instance Decoding MetaData where
    decode = do
        f <- decode :: Get Int32
        n <- decode :: Get Int32
        p <- if hasMorePages f then decode else return Nothing
        if hasNoMetaData f
            then return $ MetaData n p []
            else MetaData n p <$> decodeSpecs n (hasGlobalSpec f)
      where
        hasGlobalSpec f = f `testBit` 0
        hasMorePages  f = f `testBit` 1
        hasNoMetaData f = f `testBit` 2

        decodeSpecs n True = do
            k <- decodeK
            t <- decodeT
            replicateM (fromIntegral n) $ ColumnSpec k t
                <$> decode
                <*> decode

        decodeSpecs n False =
            replicateM (fromIntegral n) $ ColumnSpec
                <$> decodeK
                <*> decodeT
                <*> decode
                <*> decode

------------------------------------------------------------------------------
-- Column Type

data ColumnType
    = TyCustom !Text
    | TyASCII
    | TyBigInt
    | TyBlob
    | TyBoolean
    | TyCounter
    | TyDecimal
    | TyDouble
    | TyFloat
    | TyInt
    | TyTimestamp
    | TyUUID
    | TyVarChar
    | TyVarInt
    | TyTimeUUID
    | TyInet
    | TyList !ColumnType
    | TySet  !ColumnType
    | TyMap  !ColumnType !ColumnType

instance Decoding ColumnType where
    decode = decode >>= toType
      where
        toType :: Word16 -> Get ColumnType
        toType 0x0000 = TyCustom <$> decode
        toType 0x0001 = return TyASCII
        toType 0x0002 = return TyBigInt
        toType 0x0003 = return TyBlob
        toType 0x0004 = return TyBoolean
        toType 0x0005 = return TyCounter
        toType 0x0006 = return TyDecimal
        toType 0x0007 = return TyDouble
        toType 0x0008 = return TyFloat
        toType 0x0009 = return TyInt
        toType 0x000B = return TyTimestamp
        toType 0x000C = return TyUUID
        toType 0x000D = return TyVarChar
        toType 0x000E = return TyVarInt
        toType 0x000F = return TyTimeUUID
        toType 0x0010 = return TyInet
        toType 0x0020 = TyList <$> (decode >>= toType)
        toType 0x0021 = TyMap  <$> (decode >>= toType) <*> (decode >>= toType)
        toType 0x0022 = TySet  <$> (decode >>= toType)
        toType other  = fail $ "decode-type: unknown: " ++ show other

------------------------------------------------------------------------------
-- Schema Change

data SchemaChangeData
    = SchemaCreated !Keyspace !Table
    | SchemaUpdated !Keyspace !Table
    | SchemaDropped !Keyspace !Table

instance Decoding SchemaChangeData where
    decode = decode >>= fromString
      where
        fromString :: Text -> Get SchemaChangeData
        fromString "CREATED" = SchemaCreated <$> decodeK <*> decodeT
        fromString "UPDATED" = SchemaUpdated <$> decodeK <*> decodeT
        fromString "DROPPED" = SchemaDropped <$> decodeK <*> decodeT
        fromString other     = fail $
            "decode-schema-change: unknown: " ++ show other

------------------------------------------------------------------------------
-- Event

data EventData
    = TopologyChanged !TopologyChangeType !SockAddr
    | StatusChanged   !StatusChangeType   !SockAddr
    | SchemaChanged   !SchemaChangeData

data TopologyChangeType
    = NewNode
    | RemovedNode

data StatusChangeType
    = Up
    | Down

instance Decoding EventData where
    decode = decode >>= decodeByType
      where
        decodeByType :: Text -> Get EventData
        decodeByType "TOPOLOGY_CHANGE" = TopologyChanged <$> decode <*> decode
        decodeByType "STATUS_CHANGE"   = StatusChanged   <$> decode <*> decode
        decodeByType "SCHEMA_CHANGE"   = SchemaChanged   <$> decode
        decodeByType other = fail $ "decode-event: unknown: " ++ show other

instance Decoding TopologyChangeType where
    decode = decode >>= fromString
      where
        fromString :: Text -> Get TopologyChangeType
        fromString "NEW_NODE"     = return NewNode
        fromString "REMOVED_NODE" = return RemovedNode
        fromString other          = fail $
            "decode-topology: unknown: "  ++ show other

instance Decoding StatusChangeType where
    decode = decode >>= fromString
      where
        fromString :: Text -> Get StatusChangeType
        fromString "UP"   = return Up
        fromString "DOWN" = return Down
        fromString other  = fail $
            "decode-status-change: unknown: " ++ show other

-----------------------------------------------------------------------------
-- Error

data ErrorData
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
    | Unprepared      !Text !QueryId
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

instance Decoding ErrorData where
    decode = do
        code <- decode
        msg  <- decode
        toError code msg
      where
        toError :: Int32 -> Text -> Get ErrorData
        toError 0x0000 m = return $ ServerError m
        toError 0x000A m = return $ ProtocolError m
        toError 0x0100 m = return $ BadCredentials m
        toError 0x1000 m = Unavailable m <$> decode <*> decode <*> decode
        toError 0x1001 m = return $ Overloaded m
        toError 0x1002 m = return $ IsBootstrapping m
        toError 0x1003 m = return $ TruncateError m
        toError 0x1100 m = WriteTimeout m
            <$> decode
            <*> decode
            <*> decode
            <*> decode
        toError 0x1200 m = ReadTimeout m
            <$> decode
            <*> decode
            <*> decode
            <*> (bool <$> decode)
        toError 0x2000 m = return $ SyntaxError m
        toError 0x2100 m = return $ Unauthorized m
        toError 0x2200 m = return $ Invalid m
        toError 0x2300 m = return $ ConfigError m
        toError 0x2400 m = AlreadyExists m <$> decodeK <*> decodeT
        toError 0x2500 m = Unprepared m <$> (QueryId <$> decode)
        toError code _   = fail $ "decode-error: unknown: " ++ show code

        bool :: Word8 -> Bool
        bool 0 = False
        bool _ = True

data WriteType
    = WriteSimple
    | WriteBatch
    | WriteBatchLog
    | WriteUnloggedBatch
    | WriteCounter
    deriving (Eq)

instance Decoding WriteType where
    decode = decode >>= fromString
      where
        fromString :: Text -> Get WriteType
        fromString "SIMPLE"          = return WriteSimple
        fromString "BATCH"           = return WriteBatch
        fromString "BATCH_LOG"       = return WriteBatchLog
        fromString "UNLOGGED_BATCH"  = return WriteUnloggedBatch
        fromString "COUNTER"         = return WriteCounter
        fromString unknown           = fail $
            "decode: unknown write-type: " ++ show unknown

-----------------------------------------------------------------------------
-- Helpers

decodeK :: Get Keyspace
decodeK = Keyspace <$> decode

decodeT :: Get Table
decodeT = Table <$> decode
