-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQL.Protocol.Tuple (Tuple (..), Some (..)) where

import Control.Applicative
import Data.Serialize
import Data.Tagged
import Data.Word
import Database.CQL.Protocol.Class
import Database.CQL.Protocol.Codec
import Database.CQL.Protocol.Types

------------------------------------------------------------------------------
-- Tuple

class Tuple a where
    count :: Tagged a Int
    check :: Tagged a ([ColumnType] -> [ColumnType])
    tuple :: Get a
    store :: Putter a

instance Tuple () where
    count = Tagged 0
    check = Tagged $ const []
    tuple = return ()
    store = const $ return ()

newtype Some a = Some a deriving (Eq, Show)

instance (Cql a) => Tuple (Some a) where
    count = Tagged 1
    check = Tagged $ typecheck [untag (ctype :: Tagged a ColumnType)]
    tuple = Some <$> element ctype
    store (Some a) = do
        put (1 :: Word16)
        putValue (toCql a)

instance (Cql a, Cql b) => Tuple (a, b) where
    count = Tagged 2
    check = Tagged $ typecheck
        [ untag (ctype :: Tagged a ColumnType)
        , untag (ctype :: Tagged b ColumnType)
        ]
    tuple = (,)
        <$> element ctype
        <*> element ctype
    store (a, b) = do
        put (2 :: Word16)
        putValue (toCql a)
        putValue (toCql b)

instance (Cql a, Cql b, Cql c) => Tuple (a, b, c) where
    count = Tagged 3
    check = Tagged $ typecheck
        [ untag (ctype :: Tagged a ColumnType)
        , untag (ctype :: Tagged b ColumnType)
        , untag (ctype :: Tagged c ColumnType)
        ]
    tuple = (,,)
        <$> element ctype
        <*> element ctype
        <*> element ctype
    store (a, b, c) = do
        put (3 :: Word16)
        putValue (toCql a)
        putValue (toCql b)
        putValue (toCql c)

instance (Cql a, Cql b, Cql c, Cql d) => Tuple (a, b, c, d) where
    count = Tagged 4
    check = Tagged $ typecheck
        [ untag (ctype :: Tagged a ColumnType)
        , untag (ctype :: Tagged b ColumnType)
        , untag (ctype :: Tagged c ColumnType)
        , untag (ctype :: Tagged d ColumnType)
        ]
    tuple = (,,,)
        <$> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype
    store (a, b, c, d) = do
        put (4 :: Word16)
        putValue (toCql a)
        putValue (toCql b)
        putValue (toCql c)
        putValue (toCql d)

instance (Cql a, Cql b, Cql c, Cql d, Cql e) => Tuple (a, b, c, d, e) where
    count = Tagged 5
    check = Tagged $ typecheck
        [ untag (ctype :: Tagged a ColumnType)
        , untag (ctype :: Tagged b ColumnType)
        , untag (ctype :: Tagged c ColumnType)
        , untag (ctype :: Tagged d ColumnType)
        , untag (ctype :: Tagged e ColumnType)
        ]
    tuple = (,,,,)
        <$> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype
    store (a, b, c, d, e) = do
        put (5 :: Word16)
        putValue (toCql a)
        putValue (toCql b)
        putValue (toCql c)
        putValue (toCql d)
        putValue (toCql e)

instance (Cql a, Cql b, Cql c, Cql d, Cql e, Cql f) => Tuple (a, b, c, d, e, f) where
    count = Tagged 6
    check = Tagged $ typecheck
        [ untag (ctype :: Tagged a ColumnType)
        , untag (ctype :: Tagged b ColumnType)
        , untag (ctype :: Tagged c ColumnType)
        , untag (ctype :: Tagged d ColumnType)
        , untag (ctype :: Tagged e ColumnType)
        , untag (ctype :: Tagged f ColumnType)
        ]
    tuple = (,,,,,)
        <$> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype
    store (a, b, c, d, e, f) = do
        put (6 :: Word16)
        putValue (toCql a)
        putValue (toCql b)
        putValue (toCql c)
        putValue (toCql d)
        putValue (toCql e)
        putValue (toCql f)

element :: (Cql a) => Tagged a ColumnType -> Get a
element t = fromCql <$> getValue (untag t)

typecheck :: [ColumnType] -> [ColumnType] -> [ColumnType]
typecheck rr cc = if and (zipWith (===) rr cc) then [] else rr
  where
    (MaybeColumn a) === b               = a === b
    (ListColumn  a) === (ListColumn  b) = a === b
    (SetColumn   a) === (SetColumn   b) = a === b
    (MapColumn a b) === (MapColumn c d) = a === c && b === d
    a               === b               = a == b
