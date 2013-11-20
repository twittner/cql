-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQL.Protocol.Class (Cql (..)) where

import Control.Applicative
import Control.Arrow
import Data.Decimal
import Data.Int
import Data.Tagged
import Data.Text (Text)
import Data.Time
import Data.Time.Clock.POSIX
import Data.UUID (UUID)
import Database.CQL.Protocol.Types
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
    fromCql _              = undefined

------------------------------------------------------------------------------
-- Int32

instance Cql Int32 where
    ctype = Tagged IntColumn
    toCql = CqlInt
    fromCql (CqlInt i) = i
    fromCql _          = undefined

------------------------------------------------------------------------------
-- Int64

instance Cql Int64 where
    ctype = Tagged BigIntColumn
    toCql = CqlBigInt
    fromCql (CqlBigInt i) = i
    fromCql _             = undefined

------------------------------------------------------------------------------
-- Integer

instance Cql Integer where
    ctype = Tagged VarIntColumn
    toCql = CqlVarInt
    fromCql (CqlVarInt i) = i
    fromCql _             = undefined

------------------------------------------------------------------------------
-- Float

instance Cql Float where
    ctype = Tagged FloatColumn
    toCql = CqlFloat
    fromCql (CqlFloat f) = f
    fromCql _            = undefined

------------------------------------------------------------------------------
-- Double

instance Cql Double where
    ctype = Tagged DoubleColumn
    toCql = CqlDouble
    fromCql (CqlDouble d) = d
    fromCql _             = undefined

------------------------------------------------------------------------------
-- Decimal

instance Cql Decimal where
    ctype = Tagged DecimalColumn
    toCql = CqlDecimal
    fromCql (CqlDecimal d) = d
    fromCql _              = undefined

------------------------------------------------------------------------------
-- Text

instance Cql Text where
    ctype = Tagged VarCharColumn
    toCql = CqlVarChar
    fromCql (CqlVarChar s) = s
    fromCql _              = undefined

------------------------------------------------------------------------------
-- Ascii

instance Cql Ascii where
    ctype = Tagged AsciiColumn
    toCql (Ascii a) = CqlAscii a
    fromCql (CqlAscii a) = Ascii a
    fromCql _            = undefined

------------------------------------------------------------------------------
-- SockAddr

instance Cql SockAddr where
    ctype = Tagged InetColumn
    toCql = CqlInet
    fromCql (CqlInet s) = s
    fromCql _           = undefined

------------------------------------------------------------------------------
-- UUID

instance Cql UUID where
    ctype = Tagged UuidColumn
    toCql = CqlUuid
    fromCql (CqlUuid u) = u
    fromCql _           = undefined

------------------------------------------------------------------------------
-- UTCTime

instance Cql UTCTime where
    ctype = Tagged TimestampColumn

    toCql = CqlTimestamp
          . truncate
          . (* 1000)
          . utcTimeToPOSIXSeconds

    fromCql (CqlTimestamp t) =
        let (s, ms)     = t `divMod` 1000
            UTCTime a b = posixSecondsToUTCTime (fromIntegral s)
            ps          = fromIntegral ms * 1000000000
        in UTCTime a (b + picosecondsToDiffTime ps)

    fromCql _                = undefined

------------------------------------------------------------------------------
-- Blob

instance Cql Blob where
    ctype = Tagged BlobColumn
    toCql (Blob b) = CqlBlob b
    fromCql (CqlBlob b) = Blob b
    fromCql _           = undefined

------------------------------------------------------------------------------
-- Counter

instance Cql Counter where
    ctype = Tagged CounterColumn
    toCql (Counter c) = CqlCounter c
    fromCql (CqlCounter c) = Counter c
    fromCql _              = undefined

------------------------------------------------------------------------------
-- TimeUuid

instance Cql TimeUuid where
    ctype = Tagged TimeUuidColumn
    toCql (TimeUuid u) = CqlTimeUuid u
    fromCql (CqlTimeUuid t) = TimeUuid t
    fromCql _               = undefined

------------------------------------------------------------------------------
-- [a]

instance (Cql a) => Cql [a] where
    ctype = Tagged (ListColumn (untag (ctype :: Tagged a ColumnType)))
    toCql = CqlList . map toCql
    fromCql (CqlList l) = map fromCql l
    fromCql _           = undefined

------------------------------------------------------------------------------
-- Maybe a

instance (Cql a) => Cql (Maybe a) where
    ctype = Tagged (MaybeColumn (untag (ctype :: Tagged a ColumnType)))
    toCql = CqlMaybe . fmap toCql
    fromCql (CqlMaybe m) = fromCql <$> m
    fromCql _            = undefined

------------------------------------------------------------------------------
-- Map a b

instance (Cql a, Cql b) => Cql [(a, b)] where
    ctype = Tagged $ MapColumn
        (untag (ctype :: Tagged a ColumnType))
        (untag (ctype :: Tagged b ColumnType))
    toCql = CqlMap . map (toCql *** toCql)
    fromCql (CqlMap m) = map (fromCql *** fromCql) m
    fromCql _          = undefined

------------------------------------------------------------------------------
-- Set a

instance (Cql a) => Cql (Set a) where
    ctype = Tagged (SetColumn (untag (ctype :: Tagged a ColumnType)))
    toCql (Set a) = CqlSet $ map toCql a
    fromCql (CqlSet a) = Set $ map fromCql a
    fromCql _          = undefined
