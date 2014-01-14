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

class Cql a where
    ctype   :: Tagged a ColumnType
    toCql   :: a -> Value
    fromCql :: Value -> Either String a

------------------------------------------------------------------------------
-- Bool

instance Cql Bool where
    ctype = Tagged BooleanColumn
    toCql = CqlBoolean
    fromCql (CqlBoolean b) = Right b
    fromCql _              = Left "Expected CqlBoolean."

------------------------------------------------------------------------------
-- Int32

instance Cql Int32 where
    ctype = Tagged IntColumn
    toCql = CqlInt
    fromCql (CqlInt i) = Right i
    fromCql _          = Left "Expected CqlInt."

------------------------------------------------------------------------------
-- Int64

instance Cql Int64 where
    ctype = Tagged BigIntColumn
    toCql = CqlBigInt
    fromCql (CqlBigInt i) = Right i
    fromCql _             = Left "Expected CqlBigInt."

------------------------------------------------------------------------------
-- Integer

instance Cql Integer where
    ctype = Tagged VarIntColumn
    toCql = CqlVarInt
    fromCql (CqlVarInt i) = Right i
    fromCql _             = Left "Expected CqlVarInt."

------------------------------------------------------------------------------
-- Float

instance Cql Float where
    ctype = Tagged FloatColumn
    toCql = CqlFloat
    fromCql (CqlFloat f) = Right f
    fromCql _            = Left "Expected CqlFloat."

------------------------------------------------------------------------------
-- Double

instance Cql Double where
    ctype = Tagged DoubleColumn
    toCql = CqlDouble
    fromCql (CqlDouble d) = Right d
    fromCql _             = Left "Expected CqlDouble."

------------------------------------------------------------------------------
-- Decimal

instance Cql Decimal where
    ctype = Tagged DecimalColumn
    toCql = CqlDecimal
    fromCql (CqlDecimal d) = Right d
    fromCql _              = Left "Expected CqlDecimal."

------------------------------------------------------------------------------
-- Text

instance Cql Text where
    ctype = Tagged VarCharColumn
    toCql = CqlVarChar
    fromCql (CqlVarChar s) = Right s
    fromCql _              = Left "Expected CqlVarChar."

------------------------------------------------------------------------------
-- Ascii

instance Cql Ascii where
    ctype = Tagged AsciiColumn
    toCql (Ascii a) = CqlAscii a
    fromCql (CqlAscii a) = Right $ Ascii a
    fromCql _            = Left "Expected CqlAscii."

------------------------------------------------------------------------------
-- IP Address

instance Cql Inet where
    ctype = Tagged InetColumn
    toCql = CqlInet
    fromCql (CqlInet i) = Right i
    fromCql _           = Left "Expected CqlInet."

------------------------------------------------------------------------------
-- UUID

instance Cql UUID where
    ctype = Tagged UuidColumn
    toCql = CqlUuid
    fromCql (CqlUuid u) = Right u
    fromCql _           = Left "Expected CqlUuid."

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
        in Right $ UTCTime a (b + picosecondsToDiffTime ps)
    fromCql _                = Left "Expected CqlTimestamp."

------------------------------------------------------------------------------
-- Blob

instance Cql Blob where
    ctype = Tagged BlobColumn
    toCql (Blob b) = CqlBlob b
    fromCql (CqlBlob b) = Right $ Blob b
    fromCql _           = Left "Expected CqlBlob."

------------------------------------------------------------------------------
-- Counter

instance Cql Counter where
    ctype = Tagged CounterColumn
    toCql (Counter c) = CqlCounter c
    fromCql (CqlCounter c) = Right $ Counter c
    fromCql _              = Left "Expected CqlCounter."

------------------------------------------------------------------------------
-- TimeUuid

instance Cql TimeUuid where
    ctype = Tagged TimeUuidColumn
    toCql (TimeUuid u) = CqlTimeUuid u
    fromCql (CqlTimeUuid t) = Right $ TimeUuid t
    fromCql _               = Left "Expected TimeUuid."

------------------------------------------------------------------------------
-- [a]

instance (Cql a) => Cql [a] where
    ctype = Tagged (ListColumn (untag (ctype :: Tagged a ColumnType)))
    toCql = CqlList . map toCql
    fromCql (CqlList l) = mapM fromCql l
    fromCql _           = Left "Expected CqlList."

------------------------------------------------------------------------------
-- Maybe a

instance (Cql a) => Cql (Maybe a) where
    ctype = Tagged (MaybeColumn (untag (ctype :: Tagged a ColumnType)))
    toCql = CqlMaybe . fmap toCql
    fromCql (CqlMaybe (Just m)) = Just <$> fromCql m
    fromCql (CqlMaybe Nothing)  = Right Nothing
    fromCql _                   = Left "Expected CqlMaybe."

------------------------------------------------------------------------------
-- Map a b

instance (Cql a, Cql b) => Cql (Map a b) where
    ctype = Tagged $ MapColumn
        (untag (ctype :: Tagged a ColumnType))
        (untag (ctype :: Tagged b ColumnType))
    toCql (Map m)      = CqlMap $ map (toCql *** toCql) m
    fromCql (CqlMap m) = Map <$> mapM (\(k, v) -> (,) <$> fromCql k <*> fromCql v) m
    fromCql _          = Left "Expected CqlMap."

------------------------------------------------------------------------------
-- Set a

instance (Cql a) => Cql (Set a) where
    ctype = Tagged (SetColumn (untag (ctype :: Tagged a ColumnType)))
    toCql (Set a) = CqlSet $ map toCql a
    fromCql (CqlSet a) = Set <$> mapM fromCql a
    fromCql _          = Left "Expected CqlSet."
