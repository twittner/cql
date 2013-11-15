-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GADTs              #-}
{-# LANGUAGE StandaloneDeriving #-}

module Database.CQL.Frame.Types where

import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Int
import Data.Time
import Data.UUID (UUID)
import Network.Socket (SockAddr)

import qualified Data.ByteString.Lazy as LB
import qualified Data.Text.Lazy       as LT

newtype Keyspace    = Keyspace    Text          deriving (Eq, Show)
newtype Table       = Table       Text          deriving (Eq, Show)
newtype QueryId     = QueryId     ByteString    deriving (Eq, Show)
newtype QueryString = QueryString LT.Text       deriving (Eq, Show)
newtype PagingState = PagingState LB.ByteString deriving (Eq, Show)

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
    deriving (Eq, Show)


data Value where
    Value :: (Show a) => CqlValue a -> Value

deriving instance Show Value

data ASCII
data Blob
data Counter
data TimeUUID
data Map a b
data Set a

type Pair a b = (CqlValue a, CqlValue b)

data CqlValue a where
    CqlBool     :: Bool                       -> CqlValue Bool
    CqlInt32    :: Int32                      -> CqlValue Int32
    CqlInt64    :: Int64                      -> CqlValue Int64
    CqlFloat    :: Float                      -> CqlValue Float
    CqlDouble   :: Double                     -> CqlValue Double
    CqlString   :: Text                       -> CqlValue Text
    CqlInet     :: SockAddr                   -> CqlValue SockAddr
    CqlUUID     :: UUID                       -> CqlValue UUID
    CqlTime     :: UTCTime                    -> CqlValue UTCTime
    CqlAscii    :: Text                       -> CqlValue ASCII
    CqlBlob     :: LB.ByteString              -> CqlValue Blob
    CqlCounter  :: Int64                      -> CqlValue Counter
    CqlTimeUUID :: UUID                       -> CqlValue TimeUUID
    CqlMaybe    :: Maybe (CqlValue a)         -> CqlValue (Maybe a)
    CqlList     :: [CqlValue a]               -> CqlValue [a]
    CqlSet      :: [CqlValue a]               -> CqlValue (Set a)
    CqlMap      :: [(CqlValue a, CqlValue b)] -> CqlValue (Map a b)

deriving instance Show (CqlValue a)
