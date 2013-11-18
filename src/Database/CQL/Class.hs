-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQL.Class (Cql (..)) where

import Control.Applicative
import Data.Int
import Data.Tagged
import Data.Text (Text)
import Data.Time
import Data.UUID (UUID)
import Database.CQL.Frame.Types
import Network.Socket (SockAddr)

class Cql a where
    ctype   :: Tagged a ColumnType
    toCql   :: a -> Value
    fromCql :: Value -> a

------------------------------------------------------------------------------
-- Bool

instance Cql Bool where
    ctype = Tagged BooleanColumn
    toCql = CqlBoolean
    fromCql (CqlBoolean b) = b

------------------------------------------------------------------------------
-- Int32

instance Cql Int32 where
    ctype = Tagged IntColumn
    toCql = CqlInt
    fromCql (CqlInt i) = i

------------------------------------------------------------------------------
-- Int64

instance Cql Int64 where
    ctype = Tagged BigIntColumn
    toCql = CqlBigInt
    fromCql (CqlBigInt i) = i

------------------------------------------------------------------------------
-- Float

instance Cql Float where
    ctype = Tagged FloatColumn
    toCql = CqlFloat
    fromCql (CqlFloat f) = f

------------------------------------------------------------------------------
-- Double

instance Cql Double where
    ctype = Tagged DoubleColumn
    toCql = CqlDouble
    fromCql (CqlDouble d) = d

------------------------------------------------------------------------------
-- Text

instance Cql Text where
    ctype = Tagged VarCharColumn
    toCql = CqlVarChar
    fromCql (CqlVarChar s) = s

------------------------------------------------------------------------------
-- Ascii

instance Cql Ascii where
    ctype = Tagged AsciiColumn
    toCql (Ascii a) = CqlAscii a
    fromCql (CqlAscii a) = Ascii a

------------------------------------------------------------------------------
-- SockAddr

instance Cql SockAddr where
    ctype = Tagged InetColumn
    toCql = CqlInet
    fromCql (CqlInet s) = s

------------------------------------------------------------------------------
-- UUID

instance Cql UUID where
    ctype = Tagged UuidColumn
    toCql = CqlUuid
    fromCql (CqlUuid u) = u

------------------------------------------------------------------------------
-- UTCTime

instance Cql UTCTime where
    ctype = Tagged TimestampColumn
    toCql = CqlTimestamp
    fromCql (CqlTimestamp t) = t

------------------------------------------------------------------------------
-- Blob

instance Cql Blob where
    ctype = Tagged BlobColumn
    toCql (Blob b) = CqlBlob b
    fromCql (CqlBlob b) = Blob b

------------------------------------------------------------------------------
-- Counter

instance Cql Counter where
    ctype = Tagged CounterColumn
    toCql (Counter c) = CqlCounter c
    fromCql (CqlCounter c) = Counter c

------------------------------------------------------------------------------
-- TimeUuid

instance Cql TimeUuid where
    ctype = Tagged TimeUuidColumn
    toCql (TimeUuid u) = CqlTimeUuid u
    fromCql (CqlTimeUuid t) = TimeUuid t

------------------------------------------------------------------------------
-- [a]

instance (Cql a) => Cql [a] where
    ctype = Tagged (ListColumn (untag (ctype :: Tagged a ColumnType)))
    toCql = CqlList . map toCql
    fromCql (CqlList l) = map fromCql l

------------------------------------------------------------------------------
-- Maybe a

instance (Cql a) => Cql (Maybe a) where
    ctype = Tagged (MaybeColumn (untag (ctype :: Tagged a ColumnType)))
    toCql = CqlMaybe . fmap toCql
    fromCql (CqlMaybe m) = fromCql <$> m

------------------------------------------------------------------------------
-- Map a b

instance (Cql a, Cql b) => Cql [(a, b)] where
    ctype = Tagged $ MapColumn
        (untag (ctype :: Tagged a ColumnType))
        (untag (ctype :: Tagged b ColumnType))
    toCql = CqlMap . map (\(k, v) -> (toCql k, toCql v))
    fromCql (CqlMap m) = map (\(k, v) -> (fromCql k, fromCql v)) m

------------------------------------------------------------------------------
-- Set a

instance (Cql a) => Cql (Set a) where
    ctype = Tagged (SetColumn (untag (ctype :: Tagged a ColumnType)))
    toCql (Set a) = CqlSet $ map toCql a
    fromCql (CqlSet a) = Set $ map fromCql a
