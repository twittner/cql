-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE ConstraintKinds    #-}
{-# LANGUAGE DataKinds          #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE KindSignatures     #-}
{-# LANGUAGE PolyKinds          #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeOperators      #-}

module Database.CQL.Protocol.Types where

import Data.ByteString (ByteString)
import Data.Text (Text, pack, unpack)
import Data.Decimal
import Data.Int
import Data.String
import Data.UUID (UUID)
import Data.Word
import Data.Singletons.Prelude.Ord
import Data.Singletons.TypeLits (Nat)

import qualified Data.ByteString.Lazy as LB
import qualified Data.List            as List
import qualified Data.Text.Lazy       as LT

newtype Keyspace = Keyspace
    { unKeyspace :: Text } deriving (Eq, Show)

newtype Table = Table
    { unTable :: Text } deriving (Eq, Show)

newtype PagingState = PagingState
    { unPagingState :: LB.ByteString } deriving (Eq, Show)

newtype QueryId k a b = QueryId
    { unQueryId :: ByteString } deriving (Eq, Show)

newtype QueryString k a b = QueryString
    { unQueryString :: LT.Text } deriving (Eq, Show)

instance IsString (QueryString k a b) where
    fromString = QueryString . LT.pack

data CqlVersion
    = Cqlv300
    | CqlVersion !Text
    deriving (Eq, Show)

data CompressionAlgorithm
    = Snappy
    | LZ4
    | None
    deriving (Eq, Show)

data Compression = Compression
    { algorithm :: !CompressionAlgorithm
    , shrink    :: LB.ByteString -> Maybe LB.ByteString
    , expand    :: LB.ByteString -> Maybe LB.ByteString
    }

instance Show Compression where
    show = show . algorithm

noCompression :: Compression
noCompression = Compression None Just Just

data Consistency
    = Any
    | One
    | Two
    | Three
    | Quorum
    | All
    | LocalQuorum
    | EachQuorum
    | Serial
    | LocalOne
    | LocalSerial
    deriving (Eq, Show)

data OpCode
    = OcError
    | OcStartup
    | OcReady
    | OcAuthenticate
    | OcOptions
    | OcSupported
    | OcQuery
    | OcResult
    | OcPrepare
    | OcExecute
    | OcRegister
    | OcEvent
    | OcBatch
    | OcAuthChallenge
    | OcAuthResponse
    | OcAuthSuccess
    deriving (Eq, Show)

data ColumnType
    = CustomColumn !Text
    | AsciiColumn
    | BigIntColumn
    | BlobColumn
    | BooleanColumn
    | CounterColumn
    | DecimalColumn
    | DoubleColumn
    | FloatColumn
    | IntColumn
    | TextColumn
    | TimestampColumn
    | UuidColumn
    | VarCharColumn
    | VarIntColumn
    | TimeUuidColumn
    | InetColumn
    | MaybeColumn !ColumnType
    | ListColumn  !ColumnType
    | SetColumn   !ColumnType
    | MapColumn   !ColumnType !ColumnType
    | TupleColumn [ColumnType]
    | UdtColumn   !Keyspace !Text [(Text, ColumnType)]
    deriving (Eq)

instance Show ColumnType where
    show (CustomColumn a)  = unpack a
    show AsciiColumn       = "ascii"
    show BigIntColumn      = "bigint"
    show BlobColumn        = "blob"
    show BooleanColumn     = "boolean"
    show CounterColumn     = "counter"
    show DecimalColumn     = "decimal"
    show DoubleColumn      = "double"
    show FloatColumn       = "float"
    show IntColumn         = "int"
    show TextColumn        = "text"
    show TimestampColumn   = "timestamp"
    show UuidColumn        = "uuid"
    show VarCharColumn     = "varchar"
    show VarIntColumn      = "varint"
    show TimeUuidColumn    = "timeuuid"
    show InetColumn        = "inet"
    show (MaybeColumn a)   = show a ++ "?"
    show (ListColumn a)    = "list<" ++ show a ++ ">"
    show (SetColumn a)     = "set<" ++ show a ++ ">"
    show (MapColumn a b)   = "map<" ++ show a ++ ", " ++ show b ++ ">"
    show (TupleColumn a)   = "tuple<" ++ List.intercalate ", " (map show a) ++ ">"
    show (UdtColumn k n f) = unpack n

newtype Ascii    = Ascii    { fromAscii    :: Text          } deriving (Eq, Ord, Show)
newtype Blob     = Blob     { fromBlob     :: LB.ByteString } deriving (Eq, Ord, Show)
newtype Counter  = Counter  { fromCounter  :: Int64         } deriving (Eq, Ord, Show)
newtype TimeUuid = TimeUuid { fromTimeUuid :: UUID          } deriving (Eq, Ord, Show)
newtype Set a    = Set      { fromSet      :: [a]           } deriving (Show)
newtype Map a b  = Map      { fromMap      :: [(a, b)]      } deriving (Show)

instance IsString Ascii where
    fromString = Ascii . pack

data Inet
    = Inet4 !Word32
    | Inet6 !Word32 !Word32 !Word32 !Word32
    deriving (Eq, Show)

data Value (v :: Nat) where
    CqlCustom    :: LB.ByteString -> Value v
    CqlBoolean   :: Bool          -> Value v
    CqlInt       :: Int32         -> Value v
    CqlBigInt    :: Int64         -> Value v
    CqlVarInt    :: Integer       -> Value v
    CqlFloat     :: Float         -> Value v
    CqlDecimal   :: Decimal       -> Value v
    CqlDouble    :: Double        -> Value v
    CqlText      :: Text          -> Value v
    CqlInet      :: Inet          -> Value v
    CqlUuid      :: UUID          -> Value v
    CqlTimestamp :: Int64         -> Value v
    CqlAscii     :: Text          -> Value v
    CqlBlob      :: LB.ByteString -> Value v
    CqlCounter   :: Int64         -> Value v
    CqlTimeUuid  :: UUID          -> Value v

    -- Collection Types
    CqlMaybe     :: Maybe (Value v)      -> Value v
    CqlList      :: [Value v]            -> Value v
    CqlSet       :: [Value v]            -> Value v
    CqlMap       :: [(Value v, Value v)] -> Value v

    -- Tuple and user-defined types (UDT)
    CqlTuple     :: (v :>=: 3) => [Value v]         -> Value v
    CqlUdt       :: (v :>=: 3) => [(Text, Value v)] -> Value v

deriving instance Eq   (Value v)
deriving instance Show (Value v)

newtype Tagged (v :: Nat) a b = Tagged { untag :: b }

type (x :<: y)  = (x :< y)  ~ True
type (x :>: y)  = (x :> y)  ~ True
type (x :<=: y) = (x :<= y) ~ True
type (x :>=: y) = (x :>= y) ~ True

data R
data W
data S
