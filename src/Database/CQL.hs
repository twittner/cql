-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL where

import Control.Applicative
import Control.Monad
import Data.Int
import Data.Serialize hiding (encode, decode)
import Data.Text (Text)
import Data.Time
import Data.Time.Clock.POSIX
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import Data.UUID (UUID)
import Data.Word
import Database.CQL.Frame.Codec
import Network.Socket (SockAddr)

import qualified Data.ByteString.Lazy as LB

class ToCQL a where
    toCql :: Putter a

class FromCQL a where
    fromCql :: Get a

------------------------------------------------------------------------------
-- Bool

instance ToCQL Bool where
    toCql b = putWord8 $ if b then 1 else 0

instance FromCQL Bool where
    fromCql = (/= 0) <$> getWord8

------------------------------------------------------------------------------
-- Int32

instance ToCQL Int32 where
    toCql = put

instance FromCQL Int32 where
    fromCql = get

------------------------------------------------------------------------------
-- Int64

instance ToCQL Int64 where
    toCql = put

instance FromCQL Int64 where
    fromCql = get

------------------------------------------------------------------------------
-- Float

instance ToCQL Float where
    toCql = putFloat32be

instance FromCQL Float where
    fromCql = getFloat32be

------------------------------------------------------------------------------
-- Double

instance ToCQL Double where
    toCql = putFloat64be

instance FromCQL Double where
    fromCql = getFloat64be

------------------------------------------------------------------------------
-- Text

instance ToCQL Text where
    toCql = put . encodeUtf8

instance FromCQL Text where
    fromCql = decodeUtf8 <$> get

------------------------------------------------------------------------------
-- SockAddr

instance ToCQL SockAddr where
    toCql = encode

instance FromCQL SockAddr where
    fromCql = decode

------------------------------------------------------------------------------
-- UUID

instance ToCQL UUID where
    toCql = encode

instance FromCQL UUID where
    fromCql = decode

------------------------------------------------------------------------------
-- UTCTime

instance ToCQL UTCTime where
    toCql = toCql . toTimestamp
      where
        toTimestamp :: UTCTime -> Int64
        toTimestamp = truncate
                    . (* (1000 :: Double))
                    . realToFrac
                    . utcTimeToPOSIXSeconds

instance FromCQL UTCTime where
    fromCql = fromTimestamp <$> fromCql
      where
        fromTimestamp :: Int64 -> UTCTime
        fromTimestamp = posixSecondsToUTCTime . fromIntegral

------------------------------------------------------------------------------
-- ASCII

newtype ASCII = ASCII
    { ascii :: Text
    } deriving (Eq, Ord, Show)

instance ToCQL ASCII where
    toCql (ASCII a) = toCql a

instance FromCQL ASCII where
    fromCql = ASCII <$> fromCql

------------------------------------------------------------------------------
-- Blob

newtype Blob = Blob
    { blob :: LB.ByteString
    } deriving (Eq, Ord, Show)

instance ToCQL Blob where
    toCql (Blob b) = putLazyByteString b

instance FromCQL Blob where
    fromCql = Blob <$> (getLazyByteString . fromIntegral =<< remaining)

------------------------------------------------------------------------------
-- Counter

newtype Counter = Counter
    { counter :: Int64
    } deriving (Eq, Ord, Show)

instance ToCQL Counter where
    toCql (Counter c) = put c

instance FromCQL Counter where
    fromCql = Counter <$> get

------------------------------------------------------------------------------
-- TimeUUID

newtype TimeUUID = TimeUUID
    { uuid :: UUID
    } deriving (Eq, Ord, Show)

instance ToCQL TimeUUID where
    toCql (TimeUUID u) = toCql u

instance FromCQL TimeUUID where
    fromCql = TimeUUID <$> fromCql

------------------------------------------------------------------------------
-- [a]

instance (ToCQL a) => ToCQL [a] where
    toCql a = do
        put (fromIntegral (length a) :: Word16)
        mapM_ toCql a

instance (FromCQL a) => FromCQL [a] where
    fromCql = do
        n <- get :: Get Word16
        replicateM (fromIntegral n) fromCql

------------------------------------------------------------------------------
-- Maybe a

instance (ToCQL a) => ToCQL (Maybe a) where
    toCql Nothing  = return ()
    toCql (Just a) = toCql a

instance (FromCQL a) => FromCQL (Maybe a) where
    fromCql = do
        bytes <- decode :: Get (Maybe LB.ByteString)
        maybe (return Nothing) readBytes bytes
      where
        readBytes b = case runGetLazy fromCql b of
            Left e  -> fail e
            Right a -> return a

------------------------------------------------------------------------------
-- Map a b

newtype Map a b = Map
    { fromMap :: [(a, b)]
    } deriving (Eq, Show)

instance (ToCQL a, ToCQL b) => ToCQL (Map a b) where
    toCql (Map m) = do
        put (fromIntegral (length m) :: Word16)
        forM_ m $ \(k, v) -> toCql k >> toCql v

instance (FromCQL a, FromCQL b) => FromCQL (Map a b) where
    fromCql = do
        n <- get :: Get Word16
        Map <$> replicateM (fromIntegral n) ((,) <$> fromCql <*> fromCql)

------------------------------------------------------------------------------
-- Set a

newtype Set a = Set
    { fromSet :: [a]
    } deriving (Eq, Show)

instance (ToCQL a) => ToCQL (Set a) where
    toCql (Set a) = toCql a

instance (FromCQL a) => FromCQL (Set a) where
    fromCql = Set <$> fromCql
