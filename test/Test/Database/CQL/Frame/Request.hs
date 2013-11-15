-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Test.Database.CQL.Frame.Request where

import Control.Monad
import Control.Exception (bracket)
import System.IO
import Test.Tasty
import Test.Tasty.HUnit
import Test.Client
import Database.CQL

tests :: TestTree
tests = testGroup "Request"
    [ testCase "options" optionsRequest
    , testCase "startup" startupRequest
    ]

optionsRequest :: IO ()
optionsRequest = withCassandra $ \h -> do
    send h None False (StreamId 0) Options
    hdr <- recvHeader h
    void $ (recvBody h hdr :: IO (Response ()))
    version  hdr @?= V2
    streamId hdr @?= StreamId 0
    opCode   hdr @?= OcSupported

startupRequest :: IO ()
startupRequest = withCassandra $ \h -> do
    send h None False (StreamId 0) (Startup Cqlv300 None)
    hdr <- recvHeader h
    void $ (recvBody h hdr :: IO (Response ()))
    version  hdr @?= V2
    streamId hdr @?= StreamId 0
    assertBool "not (READY | AUTHENTICATE)" $
        opCode hdr `elem` [OcReady, OcAuthenticate]

------------------------------------------------------------------------------
-- Helpers

withCassandra :: (Handle -> IO a) -> IO a
withCassandra = bracket (open "localhost" 9042) close
