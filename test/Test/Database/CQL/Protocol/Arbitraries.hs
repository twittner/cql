-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# OPTIONS_GHC -fno-warn-orphans  #-}
{-# LANGUAGE OverloadedStrings     #-}

module Test.Database.CQL.Protocol.Arbitraries where

import Control.Applicative hiding (many)
import Database.CQL.Protocol
import Database.CQL.Protocol.Internal
import Data.Int
import Data.Maybe
import Data.Serialize
import Data.Time
import Data.Time.Clock.POSIX
import Data.UUID
import Network.Socket (SockAddr (..), PortNumber (..))
import Test.QuickCheck (Gen)
import Test.Tasty
import Test.Tasty.QuickCheck

import qualified Data.ByteString.Lazy as LB
import qualified Data.Text            as T

tests :: TestTree
tests = testGroup "Arbitraries"
    [ testProperty "getValue . putValue = id" getPutIdentity
    , testProperty "toCql . fromCql = id" toCqlFromCqlIdentity
    ]

getPutIdentity :: Value -> Bool
getPutIdentity x =
    let t = typeof x
        y = runGet (getValue t) (runPut (putValue x))
    in Right x == y

toCqlFromCqlIdentity :: Value -> Bool
toCqlFromCqlIdentity x@(CqlBoolean _)   = toCql (fromCql x :: Bool) == x
toCqlFromCqlIdentity x@(CqlInt _)       = toCql (fromCql x :: Int32) == x
toCqlFromCqlIdentity x@(CqlBigInt _)    = toCql (fromCql x :: Int64) == x
toCqlFromCqlIdentity x@(CqlFloat _)     = toCql (fromCql x :: Float) == x
toCqlFromCqlIdentity x@(CqlDouble _)    = toCql (fromCql x :: Double) == x
toCqlFromCqlIdentity x@(CqlVarChar _)   = toCql (fromCql x :: T.Text) == x
toCqlFromCqlIdentity x@(CqlInet _)      = toCql (fromCql x :: SockAddr) == x
toCqlFromCqlIdentity x@(CqlUuid _)      = toCql (fromCql x :: UUID) == x
toCqlFromCqlIdentity x@(CqlTimestamp _) = toCql (fromCql x :: UTCTime) == x
toCqlFromCqlIdentity x@(CqlAscii _)     = toCql (fromCql x :: Ascii) == x
toCqlFromCqlIdentity x@(CqlBlob _)      = toCql (fromCql x :: Blob) == x
toCqlFromCqlIdentity x@(CqlCounter _)   = toCql (fromCql x :: Counter) == x
toCqlFromCqlIdentity x@(CqlTimeUuid _)  = toCql (fromCql x :: TimeUuid) == x
toCqlFromCqlIdentity _                  = True

typeof :: Value -> ColumnType
typeof (CqlBoolean _)      = BooleanColumn
typeof (CqlInt _)          = IntColumn
typeof (CqlBigInt _)       = BigIntColumn
typeof (CqlVarInt _)       = VarIntColumn
typeof (CqlFloat _)        = FloatColumn
typeof (CqlDecimal _)      = DecimalColumn
typeof (CqlDouble _)       = DoubleColumn
typeof (CqlVarChar _)      = VarCharColumn
typeof (CqlInet _)         = InetColumn
typeof (CqlUuid _)         = UuidColumn
typeof (CqlTimestamp _)    = TimestampColumn
typeof (CqlAscii _)        = AsciiColumn
typeof (CqlBlob _)         = BlobColumn
typeof (CqlCounter _)      = CounterColumn
typeof (CqlTimeUuid _)     = TimeUuidColumn
typeof (CqlMaybe Nothing)  = MaybeColumn (CustomColumn "a")
typeof (CqlMaybe (Just a)) = MaybeColumn (typeof a)
typeof (CqlList [])        = ListColumn  (CustomColumn "a")
typeof (CqlList (x:_))     = ListColumn  (typeof x)
typeof (CqlSet  [])        = SetColumn (CustomColumn "a")
typeof (CqlSet  (x:_))     = SetColumn (typeof x)
typeof (CqlMap  [])        = MapColumn (CustomColumn "a") (CustomColumn "b")
typeof (CqlMap  ((x,y):_)) = MapColumn (typeof x) (typeof y)
typeof (CqlCustom _)       = CustomColumn "a"

instance Arbitrary Value where
    arbitrary = oneof
        [ simple
        , CqlMaybe <$> oneof [Just <$> simple, return Nothing]
        , CqlList  <$> many
        , CqlSet   <$> many
        , CqlMap   <$> (zip <$> many <*> many)
        ]
      where
        many   = simple >>= listOf . return
        simple = oneof
            [ CqlAscii     <$> arbitrary
            , CqlBigInt    <$> arbitrary
            , CqlBlob      <$> arbitrary
            , CqlBoolean   <$> arbitrary
            , CqlCounter   <$> arbitrary
            , CqlCustom    <$> arbitrary
            , CqlDouble    <$> arbitrary
            , CqlFloat     <$> arbitrary
            , CqlInet      <$> arbitrary
            , CqlInt       <$> arbitrary
            , CqlTimeUuid  <$> arbitrary
            , CqlTimestamp <$> arbitrary
            , CqlUuid      <$> arbitrary
            , CqlVarChar   <$> arbitrary
            -- , CqlDecimal   <$> arbitrary
            -- , CqlVarInt    <$> arbitrary
            ]

instance Arbitrary LB.ByteString where
    arbitrary = LB.pack <$> arbitrary

instance Arbitrary T.Text where
    arbitrary = T.pack <$> arbitrary

instance Arbitrary PortNumber where
    arbitrary = PortNum <$> choose (1, 65535)

instance Arbitrary SockAddr where
    arbitrary = oneof
        [ SockAddrInet  <$> arbitrary <*> arbitrary
        , SockAddrInet6 <$> arbitrary <*> pure 0 <*> arbitrary <*> pure 0
        ]

instance Bounded UUID where
    minBound = nil
    maxBound = fromJust $ fromString "ffffffff-ffff-4fff-bfff-ffffffffffff"

instance Arbitrary UUID where
    arbitrary = arbitraryBoundedRandom

instance Arbitrary UTCTime where
    arbitrary =
        posixSecondsToUTCTime . fromIntegral <$> (arbitrary :: Gen Int64)
