-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.Frame.Types where

import Data.ByteString (ByteString)
import Data.Text (Text)

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
