-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Test.Database.CQL.Protocol where

import Control.Monad
import Control.Monad.IO.Class
import Data.Text (Text)
import Test.Tasty
import Test.Tasty.HUnit
import Test.Client
import Database.CQL.Protocol

tests :: TestTree
tests = testGroup "Request"
    [ testCase "options" (exec optionsRequest)
    , testCase "startup" (exec startupRequest)
    , testCase "query"   (exec queryRequest)
    ]
  where
    exec c = runClient c True "localhost" 9042

optionsRequest :: Client ()
optionsRequest = do
    send None False (StreamId 0) Options
    h <- readHeader
    void (readBody h :: Client (Response ()))
    liftIO $ do
        version  h @?= V2
        streamId h @?= StreamId 0
        opCode   h @?= OcSupported

startupRequest :: Client ()
startupRequest = do
    send None False (StreamId 0) (Startup Cqlv300 None)
    h <- readHeader
    void (readBody h :: Client (Response ()))
    liftIO $ do
        version  h @?= V2
        streamId h @?= StreamId 0
        assertBool "not (READY | AUTHENTICATE)" $
            opCode h `elem` [OcReady, OcAuthenticate]

queryRequest :: Client ()
queryRequest = do
    startupRequest
    r <- query q :: Client (Response (Text, Bool, Text, Text))
    liftIO $ print r
  where
    q :: Query (Some Text)
    q = Query "select * from system.schema_keyspaces where keyspace_name = ?"
              (QueryParams One False (Some "system") Nothing Nothing Nothing)

query :: (Tuple a, Tuple b) => Query a -> Client (Response b)
query q = do
    send None False (StreamId 1) q
    h <- readHeader
    r <- readBody h
    liftIO $ opCode h @?= OcResult
    return r
