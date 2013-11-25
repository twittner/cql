-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE TypeFamilies #-}

module Database.CQL.Protocol.Record where

class Record a where
    type T a
    asTuple  :: a -> T a
    asRecord :: T a -> a
