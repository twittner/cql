-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Test.Database.CQL.Frame.Request where

import Control.Monad
import Control.Monad.IO.Class
import Data.Text (Text)
import Test.Tasty
import Test.Tasty.HUnit
import Test.Client
import Database.CQL

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
    void $ (readBody h :: Client (Response ()))
    liftIO $ do
        version  h @?= V2
        streamId h @?= StreamId 0
        opCode   h @?= OcSupported

startupRequest :: Client ()
startupRequest = do
    send None False (StreamId 0) (Startup Cqlv300 None)
    h <- readHeader
    void $ (readBody h :: Client (Response ()))
    liftIO $ do
        version  h @?= V2
        streamId h @?= StreamId 0
        assertBool "not (READY | AUTHENTICATE)" $
            opCode h `elem` [OcReady, OcAuthenticate]

queryRequest :: Client ()
queryRequest = do
    startupRequest
    send None False (StreamId 1) (Query qs ps)
    h <- readHeader
    r <- readBody h :: Client (Response (CqlValue Text, CqlValue Bool, CqlValue Text, CqlValue Text))
    liftIO $ opCode h @?= OcResult
    liftIO $ print r
  where
    qs = QueryString "select * from system.schema_keyspaces where keyspace_name = ?"
    ps = QueryParams One True [Value (CqlString "system")] Nothing Nothing Nothing
