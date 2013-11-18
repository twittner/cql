-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

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
    mkRow :: Get a

newtype Single a = Single a deriving (Eq, Show)

instance Row () where
    mkRow = return ()
    count = Tagged 0

instance (Cql a) => Row (Single a) where
    mkRow = Single <$> element ctype
    count = Tagged 1

instance (Cql a, Cql b) => Row (a, b) where
    count = Tagged 2
    mkRow = (,) <$> element ctype
                <*> element ctype

instance (Cql a, Cql b, Cql c) => Row (a, b, c) where
    count = Tagged 3
    mkRow = (,,) <$> element ctype
                 <*> element ctype
                 <*> element ctype

instance (Cql a, Cql b, Cql c, Cql d) => Row (a, b, c, d) where
    count = Tagged 4
    mkRow = (,,,) <$> element ctype
                  <*> element ctype
                  <*> element ctype
                  <*> element ctype

instance (Cql a, Cql b, Cql c, Cql d, Cql e) => Row (a, b, c, d, e) where
    count = Tagged 5
    mkRow = (,,,,) <$> element ctype
                   <*> element ctype
                   <*> element ctype
                   <*> element ctype
                   <*> element ctype

instance (Cql a, Cql b, Cql c, Cql d, Cql e, Cql f) => Row (a, b, c, d, e, f) where
    count = Tagged 6
    mkRow = (,,,,,) <$> element ctype
                    <*> element ctype
                    <*> element ctype
                    <*> element ctype
                    <*> element ctype
                    <*> element ctype

element :: (Cql a) => Tagged a ColumnType -> Get a
element t = fromCql <$> getValue (untag t)
