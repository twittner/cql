-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQL.Row (Row (..), Single (..)) where

import Control.Applicative
import Data.Serialize (Get)
import Data.Tagged
import Database.CQL.Class
import Database.CQL.Frame.Codec (getValue)
import Database.CQL.Frame.Types

------------------------------------------------------------------------------
-- Row

class Row a where
    count :: Tagged a Int
    check :: Tagged a ([ColumnType] -> [ColumnType])
    mkRow :: Get a

newtype Single a = Single a deriving (Eq, Show)

instance Row () where
    mkRow = return ()
    count = Tagged 0
    check = Tagged $ const []

instance (Cql a) => Row (Single a) where
    count = Tagged 1
    check = Tagged $ typecheck [untag (ctype :: Tagged a ColumnType)]
    mkRow = Single <$> element ctype

instance (Cql a, Cql b) => Row (a, b) where
    count = Tagged 2
    check = Tagged $ typecheck
        [ untag (ctype :: Tagged a ColumnType)
        , untag (ctype :: Tagged b ColumnType)
        ]
    mkRow = (,)
        <$> element ctype
        <*> element ctype

instance (Cql a, Cql b, Cql c) => Row (a, b, c) where
    count = Tagged 3
    check = Tagged $ typecheck
        [ untag (ctype :: Tagged a ColumnType)
        , untag (ctype :: Tagged b ColumnType)
        , untag (ctype :: Tagged c ColumnType)
        ]
    mkRow = (,,)
        <$> element ctype
        <*> element ctype
        <*> element ctype

instance (Cql a, Cql b, Cql c, Cql d) => Row (a, b, c, d) where
    count = Tagged 4
    check = Tagged $ typecheck
        [ untag (ctype :: Tagged a ColumnType)
        , untag (ctype :: Tagged b ColumnType)
        , untag (ctype :: Tagged c ColumnType)
        , untag (ctype :: Tagged d ColumnType)
        ]
    mkRow = (,,,)
        <$> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype

instance (Cql a, Cql b, Cql c, Cql d, Cql e) => Row (a, b, c, d, e) where
    count = Tagged 5
    check = Tagged $ typecheck
        [ untag (ctype :: Tagged a ColumnType)
        , untag (ctype :: Tagged b ColumnType)
        , untag (ctype :: Tagged c ColumnType)
        , untag (ctype :: Tagged d ColumnType)
        , untag (ctype :: Tagged e ColumnType)
        ]
    mkRow = (,,,,)
        <$> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype

instance (Cql a, Cql b, Cql c, Cql d, Cql e, Cql f) => Row (a, b, c, d, e, f) where
    count = Tagged 6
    check = Tagged $ typecheck
        [ untag (ctype :: Tagged a ColumnType)
        , untag (ctype :: Tagged b ColumnType)
        , untag (ctype :: Tagged c ColumnType)
        , untag (ctype :: Tagged d ColumnType)
        , untag (ctype :: Tagged e ColumnType)
        , untag (ctype :: Tagged f ColumnType)
        ]
    mkRow = (,,,,,)
        <$> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype
        <*> element ctype

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
