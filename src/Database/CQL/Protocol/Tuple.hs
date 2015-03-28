-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE CPP                  #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | A tuple represents the types of multiple cassandra columns. It is used
-- to check that column-types match.
module Database.CQL.Protocol.Tuple
    ( Tuple
    , count
    , check
    , tuple
    , store
    ) where

#if __GLASGOW_HASKELL__ < 710
import Control.Applicative
#endif
import Data.Serialize
import Data.Word
import Database.CQL.Protocol.Class
import Database.CQL.Protocol.Codec (putValue)
import Database.CQL.Protocol.Types
import Database.CQL.Protocol.Tuple.TH

genInstances 48
