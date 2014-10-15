-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.Protocol.Types where

import Data.ByteString (ByteString)
import Data.Text (Text, pack, unpack)
import Data.Decimal
import Data.Int
import Data.IP
import Data.String
import Data.UUID (UUID)

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

data Version
    = V2
    | V3
    deriving (Eq, Show)

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
    show (ListColumn a)    = showString "list<" . shows a . showString ">" $ ""
    show (SetColumn a)     = showString "set<" . shows a . showString ">" $ ""
    show (MapColumn a b)   = showString "map<"
                           . shows a
                           . showString ", "
                           . shows b
                           . showString ">"
                           $ ""
    show (TupleColumn a)   = showString "tuple<"
                           . showString (List.intercalate ", " (map show a))
                           . showString ">"
                           $ ""
    show (UdtColumn k n f) = showString (unpack (unKeyspace k))
                           . showString "."
                           . showString (unpack n)
                           . showString "<"
                           . shows (List.intercalate ", " (map show f))
                           . showString ">"
                           $ ""

newtype Ascii    = Ascii    { fromAscii    :: Text          } deriving (Eq, Ord, Show)
newtype Blob     = Blob     { fromBlob     :: LB.ByteString } deriving (Eq, Ord, Show)
newtype Counter  = Counter  { fromCounter  :: Int64         } deriving (Eq, Ord, Show)
newtype TimeUuid = TimeUuid { fromTimeUuid :: UUID          } deriving (Eq, Ord, Show)
newtype Set a    = Set      { fromSet      :: [a]           } deriving (Show)
newtype Map a b  = Map      { fromMap      :: [(a, b)]      } deriving (Show)

instance IsString Ascii where
    fromString = Ascii . pack

data Value
    = CqlCustom    !LB.ByteString
    | CqlBoolean   !Bool
    | CqlInt       !Int32
    | CqlBigInt    !Int64
    | CqlVarInt    !Integer
    | CqlFloat     !Float
    | CqlDecimal   !Decimal
    | CqlDouble    !Double
    | CqlText      !Text
    | CqlInet      !IP
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
    | CqlTuple     [Value]
    | CqlUdt       [(Text, Value)]
    deriving (Eq, Show)

newtype Tagged a b = Tagged { untag :: b }

retag :: Tagged a c -> Tagged b c
retag = Tagged . untag

data R
data W
data S
