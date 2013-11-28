-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.Protocol.Types where

import Data.ByteString (ByteString)
import Data.Text (Text, pack, unpack)
import Data.Decimal
import Data.Int
import Data.String
import Data.UUID (UUID)
import Data.Word

import qualified Data.ByteString.Lazy as LB
import qualified Data.Text.Lazy       as LT

newtype Keyspace      = Keyspace    Text          deriving (Eq, Show)
newtype Table         = Table       Text          deriving (Eq, Show)
newtype PagingState   = PagingState LB.ByteString deriving (Eq, Show)
newtype QueryId a     = QueryId     ByteString    deriving (Eq, Show)
newtype QueryString a = QueryString LT.Text       deriving (Eq, Show)

instance IsString (QueryString a) where
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
    deriving (Eq)

instance Show ColumnType where
    show (CustomColumn a) = unpack a
    show AsciiColumn      = "ascii"
    show BigIntColumn     = "bigint"
    show BlobColumn       = "blob"
    show BooleanColumn    = "boolean"
    show CounterColumn    = "counter"
    show DecimalColumn    = "decimal"
    show DoubleColumn     = "double"
    show FloatColumn      = "float"
    show IntColumn        = "int"
    show TimestampColumn  = "timestamp"
    show UuidColumn       = "uuid"
    show VarCharColumn    = "varchar"
    show VarIntColumn     = "varint"
    show TimeUuidColumn   = "timeuuid"
    show InetColumn       = "inet"
    show (MaybeColumn a)  = show a ++ "?"
    show (ListColumn a)   = "list<" ++ show a ++ ">"
    show (SetColumn a)    = "set<" ++ show a ++ ">"
    show (MapColumn a b)  = "map<" ++ show a ++ ", " ++ show b ++ ">"

newtype Ascii    = Ascii    Text          deriving (Eq, Ord, Show)
newtype Blob     = Blob     LB.ByteString deriving (Eq, Ord, Show)
newtype Counter  = Counter  Int64         deriving (Eq, Ord, Show)
newtype TimeUuid = TimeUuid UUID          deriving (Eq, Ord, Show)
newtype Set a    = Set      [a]           deriving (Show)
newtype Map a b  = Map      [(a, b)]      deriving (Show)

instance IsString Ascii where
    fromString = Ascii . pack

data Inet
    = Inet4 !Word32
    | Inet6 !Word32 !Word32 !Word32 !Word32
    deriving (Eq, Show)

data Value
    = CqlCustom    !LB.ByteString
    | CqlBoolean   !Bool
    | CqlInt       !Int32
    | CqlBigInt    !Int64
    | CqlVarInt    !Integer
    | CqlFloat     !Float
    | CqlDecimal   !Decimal
    | CqlDouble    !Double
    | CqlVarChar   !Text
    | CqlInet      !Inet
    | CqlUuid      !UUID
    | CqlTimestamp !Int64
    | CqlAscii     !Text
    | CqlBlob      !LB.ByteString
    | CqlCounter   !Int64
    | CqlTimeUuid  !UUID
    | CqlMaybe     (Maybe Value)
    | CqlList      [Value]
    | CqlSet       [Value]
    | CqlMap       [(Value, Value)]
    deriving (Eq, Show)
