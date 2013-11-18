-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.Frame.Response
    ( Response       (..)
    , ColumnSpec     (..)
    , ColumnType     (..)
    , Error          (..)
    , Event          (..)
    , MetaData       (..)
    , Result         (..)
    , SchemaChange   (..)
    , StatusChange   (..)
    , TopologyChange (..)
    , WriteType      (..)
    , Ready          (..)
    , Supported      (..)
    , unpack
    ) where

import Control.Applicative
import Control.Monad
import Data.Bits
import Data.Int
import Data.Tagged
import Data.Text (Text)
import Data.Serialize hiding (decode, encode, Result)
import Data.UUID (UUID)
import Data.Word
import Database.CQL.Row
import Database.CQL.Frame.Codec
import Database.CQL.Frame.Header
import Database.CQL.Frame.Types
import Network.Socket (SockAddr)

import qualified Data.ByteString.Lazy as LB

------------------------------------------------------------------------------
-- Response

data Response a
    = RsError         (Maybe UUID) !Error
    | RsReady         (Maybe UUID) !Ready
    | RsAuthenticate  (Maybe UUID) !Text
    | RsAuthChallenge (Maybe UUID) (Maybe LB.ByteString)
    | RsAuthSuccess   (Maybe UUID) (Maybe LB.ByteString)
    | RsSupported     (Maybe UUID) !Supported
    | RsResult        (Maybe UUID) !(Result a)
    | RsEvent         (Maybe UUID) !Event
    deriving (Show)

unpack :: (Row a)
       => (LB.ByteString -> LB.ByteString)
       -> Header
       -> LB.ByteString
       -> Either String (Response a)
unpack deflate h b = do
    let f = flags h
    let x = if compression `isSet` f then deflate b else b
    flip runGetLazy x $ do
        t <- if tracing `isSet` f then Just <$> decode else return Nothing
        message t (opCode h)
  where
    message t OcError         = RsError         t <$> decode
    message t OcReady         = RsReady         t <$> decode
    message t OcAuthenticate  = RsAuthenticate  t <$> decode
    message t OcSupported     = RsSupported     t <$> decode -- TODO: use known options
    message t OcResult        = RsResult        t <$> decode
    message t OcEvent         = RsEvent         t <$> decode
    message t OcAuthChallenge = RsAuthChallenge t <$> decode
    message t OcAuthSuccess   = RsAuthSuccess   t <$> decode
    message _ other = fail $ "decode-response: unknown: " ++ show other

------------------------------------------------------------------------------
-- READY

data Ready = Ready deriving Show

instance Decoding Ready where
    decode = return Ready

------------------------------------------------------------------------------
-- SUPPORTED

newtype Supported = Supported [(Text, [Text])] deriving Show

instance Decoding Supported where
    decode = Supported <$> decode

------------------------------------------------------------------------------
-- RESULT

data Result a
    = VoidResult
    | RowsResult         !MetaData [a]
    | SetKeyspaceResult  !Keyspace
    | PreparedResult     !QueryId !MetaData !MetaData
    | SchemaChangeResult !SchemaChange
    deriving (Show)

instance (Row a) => Decoding (Result a) where
    decode = decode >>= decodeResult count
      where
        decodeResult :: (Row a) => Tagged a Int -> Int32 -> Get (Result a)
        decodeResult _ 0x1 = return VoidResult
        decodeResult t 0x2 = do
            m <- decode
            n <- decode :: Get Int32
            unless (columnCount m == fromIntegral (untag t)) $
                fail $ "column count: "
                    ++ show (columnCount m)
                    ++ " =/= "
                    ++ show (untag t)
            RowsResult m <$> replicateM (fromIntegral n) mkRow
        decodeResult _ 0x3 = SetKeyspaceResult <$> decode
        decodeResult _ 0x4 = PreparedResult <$> decode <*> decode <*> decode
        decodeResult _ 0x5 = SchemaChangeResult <$> decode
        decodeResult _ int = fail $ "decode-result: unknown: " ++ show int

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
            k <- decode
            t <- decode
            replicateM (fromIntegral n) $ ColumnSpec k t
                <$> decode
                <*> decode

        decodeSpecs n False =
            replicateM (fromIntegral n) $ ColumnSpec
                <$> decode
                <*> decode
                <*> decode
                <*> decode

------------------------------------------------------------------------------
-- SCHEMA_CHANGE

data SchemaChange
    = SchemaCreated !Keyspace !Table
    | SchemaUpdated !Keyspace !Table
    | SchemaDropped !Keyspace !Table
    deriving (Show)

instance Decoding SchemaChange where
    decode = decode >>= fromString
      where
        fromString :: Text -> Get SchemaChange
        fromString "CREATED" = SchemaCreated <$> decode <*> decode
        fromString "UPDATED" = SchemaUpdated <$> decode <*> decode
        fromString "DROPPED" = SchemaDropped <$> decode <*> decode
        fromString other     = fail $
            "decode-schema-change: unknown: " ++ show other

------------------------------------------------------------------------------
-- EVENT

data Event
    = TopologyEvent !TopologyChange !SockAddr
    | StatusEvent   !StatusChange   !SockAddr
    | SchemaEvent   !SchemaChange
    deriving (Show)

data TopologyChange = NewNode | RemovedNode deriving (Show)
data StatusChange   = Up | Down deriving (Show)

instance Decoding Event where
    decode = decode >>= decodeByType
      where
        decodeByType :: Text -> Get Event
        decodeByType "TOPOLOGY_CHANGE" = TopologyEvent <$> decode <*> decode
        decodeByType "STATUS_CHANGE"   = StatusEvent   <$> decode <*> decode
        decodeByType "SCHEMA_CHANGE"   = SchemaEvent   <$> decode
        decodeByType other             = fail $
            "decode-event: unknown: " ++ show other

instance Decoding TopologyChange where
    decode = decode >>= fromString
      where
        fromString :: Text -> Get TopologyChange
        fromString "NEW_NODE"     = return NewNode
        fromString "REMOVED_NODE" = return RemovedNode
        fromString other          = fail $
            "decode-topology: unknown: "  ++ show other

instance Decoding StatusChange where
    decode = decode >>= fromString
      where
        fromString :: Text -> Get StatusChange
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
    deriving (Show)

instance Decoding Error where
    decode = do
        code <- decode
        msg  <- decode
        toError code msg
      where
        toError :: Int32 -> Text -> Get Error
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
        toError 0x2400 m = AlreadyExists m <$> decode <*> decode
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
    deriving (Show)

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

