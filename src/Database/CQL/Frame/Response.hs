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
import Data.Tagged
import Data.Text (Text)
import Data.Serialize hiding (decode, encode)
import Data.UUID (UUID)
import Data.Word
import Database.CQL.Class
import Database.CQL.Frame.Codec
import Database.CQL.Frame.Header
import Database.CQL.Frame.Types
import Network.Socket (SockAddr)

import qualified Data.ByteString.Lazy as LB

------------------------------------------------------------------------------
-- Response

data Response a = Response
    { rsHeader    :: !Header
    , rsTracingId :: Maybe UUID
    , rsBody      :: !(ResponseMessage a)
    } deriving (Eq, Show)

data ResponseMessage a
    = Error         !ErrorData
    | Ready
    | Authenticate  !Text
    | Supported     [(Text, [Text])]
    | Result        !(ResultData a)
    | Event         !EventData
    | AuthChallenge (Maybe LB.ByteString)
    | AuthSuccess   (Maybe LB.ByteString)
    deriving (Eq, Show)

response :: (FromCQL a)
         => (LB.ByteString -> LB.ByteString)
         -> HeaderData
         -> LB.ByteString
         -> Either String (Response a)
response deflate hdr bytes = do
    let f = hdrFlags hdr
    let b = if compression `isSet` f then deflate bytes else bytes
    flip runGetLazy b $ do
        t <- if tracing `isSet` f then Just <$> decode else return Nothing
        Response (ResponseHeader hdr) t <$> message (hdrOpCode hdr)
  where
    message OcError         = Error <$> decode
    message OcReady         = return Ready
    message OcAuthenticate  = Authenticate  <$> decode
    message OcSupported     = Supported     <$> decode -- TODO: use known options
    message OcResult        = Result        <$> decode
    message OcEvent         = Event         <$> decode
    message OcAuthChallenge = AuthChallenge <$> decode
    message OcAuthSuccess   = AuthSuccess   <$> decode
    message other = fail $ "decode-response: unknown: " ++ show other

------------------------------------------------------------------------------
-- Result

data ResultData a
    = Void
    | Rows         !MetaData [a]
    | SetKeyspace  !Keyspace
    | Prepared     !QueryId !MetaData !MetaData
    | SchemaChange !SchemaChangeData
    deriving (Eq, Show)

instance (FromCQL a) => Decoding (ResultData a) where
    decode = decode >>= decodeResult arity
      where
        decodeResult :: FromCQL a => Tagged a Int -> Int32 -> Get (ResultData a)
        decodeResult _ 0x1 = return Void
        decodeResult a 0x2 = do
            m <- decode
            n <- decode :: Get Int32
            unless (mdColumnCount m == fromIntegral (unTagged a)) $
                fail "decode-result: arity mismatch"
            Rows m <$> replicateM (fromIntegral n) fromCql
        decodeResult _ 0x3 = SetKeyspace . Keyspace <$> decode
        decodeResult _ 0x4 = Prepared . QueryId <$> decode <*> decode <*> decode
        decodeResult _ 0x5 = SchemaChange <$> decode
        decodeResult _ int = fail $ "decode-result: unknown: " ++ show int

------------------------------------------------------------------------------
-- Meta Data

data MetaData = MetaData
    { mdColumnCount :: !Int32
    , mdPagingState :: Maybe PagingState
    , mdColumnSpecs :: [ColumnSpec]
    } deriving (Eq, Show)

data ColumnSpec = ColumnSpec
    { colKeyspace :: !Keyspace
    , colTable    :: !Table
    , colName     :: !Text
    , colType     :: !ColumnType
    } deriving (Eq, Show)

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
-- Schema Change

data SchemaChangeData
    = SchemaCreated !Keyspace !Table
    | SchemaUpdated !Keyspace !Table
    | SchemaDropped !Keyspace !Table
    deriving (Eq, Show)

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
    deriving (Eq, Show)

data TopologyChangeType
    = NewNode
    | RemovedNode
    deriving (Eq, Show)

data StatusChangeType
    = Up
    | Down
    deriving (Eq, Show)

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
    deriving (Eq, Show)

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
    deriving (Eq, Show)

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
