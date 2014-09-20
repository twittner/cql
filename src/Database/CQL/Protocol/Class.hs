-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module Database.CQL.Protocol.Class (Cql (..)) where

import Control.Applicative
import Control.Arrow
import Data.Decimal
import Data.Int
import Data.Singletons.TypeLits (Nat)
import Data.Text (Text)
import Data.Time
import Data.Time.Clock.POSIX
import Data.UUID (UUID)
import Database.CQL.Protocol.Types

class Cql (v :: Nat) a where
    ctype   :: Tagged v a ColumnType
    toCql   :: a -> Value v
    fromCql :: Value v -> Either String a

------------------------------------------------------------------------------
-- Bool

instance Cql v Bool where
    ctype = Tagged BooleanColumn
    toCql = CqlBoolean
    fromCql (CqlBoolean b) = Right b
    fromCql _              = Left "Expected CqlBoolean."

------------------------------------------------------------------------------
-- Int32

instance Cql v Int32 where
    ctype = Tagged IntColumn
    toCql = CqlInt
    fromCql (CqlInt i) = Right i
    fromCql _          = Left "Expected CqlInt."

------------------------------------------------------------------------------
-- Int64

instance Cql v Int64 where
    ctype = Tagged BigIntColumn
    toCql = CqlBigInt
    fromCql (CqlBigInt i) = Right i
    fromCql _             = Left "Expected CqlBigInt."

------------------------------------------------------------------------------
-- Integer

instance Cql v Integer where
    ctype = Tagged VarIntColumn
    toCql = CqlVarInt
    fromCql (CqlVarInt i) = Right i
    fromCql _             = Left "Expected CqlVarInt."

------------------------------------------------------------------------------
-- Float

instance Cql v Float where
    ctype = Tagged FloatColumn
    toCql = CqlFloat
    fromCql (CqlFloat f) = Right f
    fromCql _            = Left "Expected CqlFloat."

------------------------------------------------------------------------------
-- Double

instance Cql v Double where
    ctype = Tagged DoubleColumn
    toCql = CqlDouble
    fromCql (CqlDouble d) = Right d
    fromCql _             = Left "Expected CqlDouble."

------------------------------------------------------------------------------
-- Decimal

instance Cql v Decimal where
    ctype = Tagged DecimalColumn
    toCql = CqlDecimal
    fromCql (CqlDecimal d) = Right d
    fromCql _              = Left "Expected CqlDecimal."

------------------------------------------------------------------------------
-- Text

instance Cql v Text where
    ctype = Tagged TextColumn
    toCql = CqlText
    fromCql (CqlText s) = Right s
    fromCql _           = Left "Expected CqlText."

------------------------------------------------------------------------------
-- Ascii

instance Cql v Ascii where
    ctype = Tagged AsciiColumn
    toCql (Ascii a) = CqlAscii a
    fromCql (CqlAscii a) = Right $ Ascii a
    fromCql _            = Left "Expected CqlAscii."

------------------------------------------------------------------------------
-- IP Address

instance Cql v Inet where
    ctype = Tagged InetColumn
    toCql = CqlInet
    fromCql (CqlInet i) = Right i
    fromCql _           = Left "Expected CqlInet."

------------------------------------------------------------------------------
-- UUID

instance Cql v UUID where
    ctype = Tagged UuidColumn
    toCql = CqlUuid
    fromCql (CqlUuid u) = Right u
    fromCql _           = Left "Expected CqlUuid."

------------------------------------------------------------------------------
-- UTCTime

instance Cql v UTCTime where
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

instance Cql v Blob where
    ctype = Tagged BlobColumn
    toCql (Blob b) = CqlBlob b
    fromCql (CqlBlob b) = Right $ Blob b
    fromCql _           = Left "Expected CqlBlob."

------------------------------------------------------------------------------
-- Counter

instance Cql v Counter where
    ctype = Tagged CounterColumn
    toCql (Counter c) = CqlCounter c
    fromCql (CqlCounter c) = Right $ Counter c
    fromCql _              = Left "Expected CqlCounter."

------------------------------------------------------------------------------
-- TimeUuid

instance Cql v TimeUuid where
    ctype = Tagged TimeUuidColumn
    toCql (TimeUuid u) = CqlTimeUuid u
    fromCql (CqlTimeUuid t) = Right $ TimeUuid t
    fromCql _               = Left "Expected TimeUuid."

------------------------------------------------------------------------------
-- [a]

instance Cql v a => Cql v [a] where
    ctype = Tagged (ListColumn (untag (ctype :: Tagged v a ColumnType)))
    toCql = CqlList . map toCql
    fromCql (CqlList l) = mapM fromCql l
    fromCql _           = Left "Expected CqlList."

------------------------------------------------------------------------------
-- Maybe a

-- | Please note that due to the fact that Cassandra internally represents
-- empty collection type values (i.e. lists, maps and sets) as @null@, we
-- can not distinguish @Just []@ from @Nothing@ on response decoding.
instance Cql v a => Cql v (Maybe a) where
    ctype = Tagged (MaybeColumn (untag (ctype :: Tagged v a ColumnType)))
    toCql = CqlMaybe . fmap toCql
    fromCql (CqlMaybe (Just m)) = Just <$> fromCql m
    fromCql (CqlMaybe Nothing)  = Right Nothing
    fromCql _                   = Left "Expected CqlMaybe."

------------------------------------------------------------------------------
-- Map a b

instance (Cql v a, Cql v b) => Cql v (Map a b) where
    ctype = Tagged $ MapColumn
        (untag (ctype :: Tagged v a ColumnType))
        (untag (ctype :: Tagged v b ColumnType))
    toCql (Map m)      = CqlMap $ map (toCql *** toCql) m
    fromCql (CqlMap m) = Map <$> mapM (\(k, v) -> (,) <$> fromCql k <*> fromCql v) m
    fromCql _          = Left "Expected CqlMap."

------------------------------------------------------------------------------
-- Set a

instance Cql v a => Cql v (Set a) where
    ctype = Tagged (SetColumn (untag (ctype :: Tagged v a ColumnType)))
    toCql (Set a) = CqlSet $ map toCql a
    fromCql (CqlSet a) = Set <$> mapM fromCql a
    fromCql _          = Left "Expected CqlSet."
