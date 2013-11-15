-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Database.CQL.Class where

import Control.Applicative
import Data.ByteString.Lazy (ByteString)
import Data.Int
import Data.Serialize hiding (encode, decode)
import Data.Tagged
import Data.Text (Text)
import Data.Time
import Data.UUID (UUID)
import Database.CQL.Frame.Types
import Database.CQL.Frame.Codec
import Network.Socket (SockAddr)

class ToCQL a b where
    toCql :: a -> CqlValue b

class FromCQL a b where
    fromCql :: CqlValue a -> b

------------------------------------------------------------------------------
-- Bool

instance ToCQL Bool Bool where
    toCql = CqlBool

instance FromCQL Bool Bool where
    fromCql (CqlBool b) = b

------------------------------------------------------------------------------
-- Int32

instance ToCQL Int32 Int32 where
    toCql = CqlInt32

instance FromCQL Int32 Int32 where
    fromCql (CqlInt32 i) = i

------------------------------------------------------------------------------
-- Int64

instance ToCQL Int64 Int64 where
    toCql = CqlInt64

instance FromCQL Int64 Int64 where
    fromCql (CqlInt64 i) = i

------------------------------------------------------------------------------
-- Float

instance ToCQL Float Float where
    toCql = CqlFloat

instance FromCQL Float Float where
    fromCql (CqlFloat f) = f

------------------------------------------------------------------------------
-- Double

instance ToCQL Double Double where
    toCql = CqlDouble

instance FromCQL Double Double where
    fromCql (CqlDouble d) = d

------------------------------------------------------------------------------
-- Text

instance ToCQL Text Text where
    toCql = CqlString

instance FromCQL Text Text where
    fromCql (CqlString s) = s

------------------------------------------------------------------------------
-- SockAddr

instance ToCQL SockAddr SockAddr where
    toCql = CqlInet

instance FromCQL SockAddr SockAddr where
    fromCql (CqlInet s) = s

------------------------------------------------------------------------------
-- UUID

instance ToCQL UUID UUID where
    toCql = CqlUUID

instance FromCQL UUID UUID where
    fromCql (CqlUUID u) = u

------------------------------------------------------------------------------
-- UTCTime

instance ToCQL UTCTime UTCTime where
    toCql = CqlTime

instance FromCQL UTCTime UTCTime where
    fromCql (CqlTime t) = t

------------------------------------------------------------------------------
-- ASCII

instance ToCQL Text ASCII where
    toCql = CqlAscii

instance FromCQL ASCII Text where
    fromCql (CqlAscii a) = a

------------------------------------------------------------------------------
-- Blob

instance ToCQL ByteString Blob where
    toCql = CqlBlob

instance FromCQL Blob ByteString where
    fromCql (CqlBlob b) = b

------------------------------------------------------------------------------
-- Counter

instance ToCQL Int64 Counter where
    toCql = CqlCounter

instance FromCQL Counter Int64 where
    fromCql (CqlCounter c) = c

------------------------------------------------------------------------------
-- TimeUUID

instance ToCQL UUID TimeUUID where
    toCql = CqlTimeUUID

instance FromCQL TimeUUID UUID where
    fromCql (CqlTimeUUID t) = t

------------------------------------------------------------------------------
-- [a]

instance (ToCQL a b) => ToCQL [a] [b] where
    toCql = CqlList . map toCql

instance (FromCQL a b) => FromCQL [a] [b] where
    fromCql (CqlList l) = map fromCql l

------------------------------------------------------------------------------
-- Maybe a

instance (ToCQL a b) => ToCQL (Maybe a) (Maybe b) where
    toCql = CqlMaybe . fmap toCql

instance (FromCQL a b) => FromCQL (Maybe a) (Maybe b) where
    fromCql (CqlMaybe m) = fromCql <$> m

------------------------------------------------------------------------------
-- Map a b

instance (ToCQL a c, ToCQL b d) => ToCQL [(a, b)] (Map c d) where
    toCql = CqlMap . map (\(k, v) -> (toCql k, toCql v))

instance (FromCQL a c, FromCQL b d) => FromCQL (Map a b) [(c, d)] where
    fromCql (CqlMap m) = map (\(k, v) -> (fromCql k, fromCql v)) m

------------------------------------------------------------------------------
-- Set a

instance (ToCQL a b) => ToCQL [a] (Set b) where
    toCql = CqlSet . map toCql

instance (FromCQL a b) => FromCQL (Set a) [b] where
    fromCql (CqlSet a) = map fromCql a

------------------------------------------------------------------------------
-- Row

class Row a where
    mkRow :: Get a
    arity :: Tagged a Int

instance Row () where
    mkRow = return ()
    arity = Tagged 0

instance Row (CqlValue Bool) where
    mkRow = decode
    arity = Tagged 1

instance Row (CqlValue Int32) where
    mkRow = decode
    arity = Tagged 1

instance Row (CqlValue Int64) where
    mkRow = decode
    arity = Tagged 1

instance Row (CqlValue Float) where
    mkRow = decode
    arity = Tagged 1

instance Row (CqlValue Double) where
    mkRow = decode
    arity = Tagged 1

instance Row (CqlValue Text) where
    mkRow = decode
    arity = Tagged 1

instance Row (CqlValue ASCII) where
    mkRow = decode
    arity = Tagged 1

instance Row (CqlValue Blob) where
    mkRow = decode
    arity = Tagged 1

instance Row (CqlValue SockAddr) where
    mkRow = decode
    arity = Tagged 1

instance Row (CqlValue UUID) where
    mkRow = decode
    arity = Tagged 1

instance Row (CqlValue TimeUUID) where
    mkRow = decode
    arity = Tagged 1

instance Row (CqlValue Counter) where
    mkRow = decode
    arity = Tagged 1

instance (Decoding (CqlValue a)) => Row (CqlValue (Maybe a)) where
    mkRow = decode
    arity = Tagged 1

instance (Decoding (CqlValue a)) => Row (CqlValue [a]) where
    mkRow = decode
    arity = Tagged 1

instance (Decoding (CqlValue a)) => Row (CqlValue (Set a)) where
    mkRow = decode
    arity = Tagged 1

instance (Decoding (CqlValue a), Decoding (CqlValue b))
    => Row (CqlValue (Map a b))
  where
    mkRow = decode
    arity = Tagged 1

instance (Row a, Row b) => Row (a, b) where
    mkRow = (,) <$> mkRow <*> mkRow
    arity = Tagged 2

instance (Row a, Row b, Row c) => Row (a, b, c) where
    mkRow = (,,) <$> mkRow <*> mkRow <*> mkRow
    arity = Tagged 3

instance (Row a, Row b, Row c, Row d) => Row (a, b, c, d) where
    mkRow = (,,,) <$> mkRow <*> mkRow <*> mkRow <*> mkRow
    arity = Tagged 4

instance (Row a, Row b, Row c, Row d, Row e) => Row (a, b, c, d, e) where
    mkRow = (,,,,) <$> mkRow <*> mkRow <*> mkRow <*> mkRow <*> mkRow
    arity = Tagged 5
