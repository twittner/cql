-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.Frame.Types where

import Data.ByteString (ByteString)
import Data.Text (Text)

import qualified Data.ByteString.Lazy as LB
import qualified Data.Text.Lazy       as LT

newtype Keyspace    = Keyspace    Text
newtype Table       = Table       Text
newtype Row         = Row         [Cell]
newtype Cell        = Cell        (Maybe LB.ByteString)
newtype QueryId     = QueryId     ByteString
newtype QueryString = QueryString LT.Text
newtype PagingState = PagingState LB.ByteString

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
