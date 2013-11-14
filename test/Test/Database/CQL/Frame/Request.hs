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
    [testCase "options" optionsRequest]

optionsRequest :: IO ()
optionsRequest = withCassandra $ \h -> do
    let req = buildRequest $ Options
    sendRequest h req
    hdr <- recvHeader h
    void (recvBody h hdr :: IO (Response ()))

------------------------------------------------------------------------------
-- Helpers

buildRequest :: RequestMessage -> Request
buildRequest m = request V2 Nothing False (StreamId 0) m

withCassandra :: (Handle -> IO a) -> IO a
withCassandra = bracket (open "localhost" 9042) close
