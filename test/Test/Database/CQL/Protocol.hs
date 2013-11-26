-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}

module Test.Database.CQL.Protocol where

import Control.Applicative
import Control.Monad
import Control.Monad.IO.Class
import Data.Decimal
import Data.Int
import Data.Maybe
import Data.Text (Text)
import Data.Time
import Data.UUID
import Test.Tasty
import Test.Tasty.HUnit
import Test.Client
import Database.CQL.Protocol

data Rec = Rec
    { ra :: Int64
    , rb :: Ascii
    , rc :: Blob
    , rd :: Bool
    , rf :: Decimal
    , rg :: Double
    , rh :: Float
    , ri :: Int32
    , rj :: UTCTime
    , rk :: UUID
    , rl :: Text
    , rm :: Integer
    , rn :: TimeUuid
    , ro :: Inet
    , rp :: [Int32]
    , rq :: Set Ascii
    , rr :: Map Ascii Int32
    } deriving (Eq, Show)

recordInstance ''Rec

tests :: TestTree
tests = testGroup "Request"
    [ testCase "options"  (exec optionsRequest)
    , testCase "startup"  (exec startupRequest)
    , testCase "keyspace" (exec createKeyspace)
    , testCase "table"    (exec createTable)
    , testCase "insert"   (exec insertTable)
    , testCase "query"    (exec queryRequest)
    , testCase "drop"     (exec dropKeyspace)
    ]
  where
    exec c = void $ runClient c True noCompression "localhost" 9042

optionsRequest :: Client ()
optionsRequest = do
    send (StreamId 0) Options
    h <- readHeader
    void (readBody h :: Client (Response ()))
    liftIO $ do
        version  h @?= V2
        streamId h @?= StreamId 0
        opCode   h @?= OcSupported

startupRequest :: Client ()
startupRequest = do
    c <- asks packer
    send (StreamId 0) (Startup Cqlv300 (algorithm c))
    h <- readHeader
    void (readBody h :: Client (Response ()))
    liftIO $ do
        version  h @?= V2
        streamId h @?= StreamId 0
        assertBool "not (READY | AUTHENTICATE)" $
            opCode h `elem` [OcReady, OcAuthenticate]

queryRequest :: Client ()
queryRequest = startupRequest >> void (q "system")
  where
    q :: Text -> Client [(Text, Bool, Text, Text)]
    q = runQuery . query' One "select * from system.schema_keyspaces where keyspace_name = ?"

createKeyspace :: Client ()
createKeyspace = do
    startupRequest
    runQuery_ $ query One "create keyspace cqltest with replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }" ()

dropKeyspace :: Client ()
dropKeyspace = do
    startupRequest
    runQuery_ $ query One "drop keyspace cqltest" ()

createTable :: Client ()
createTable = do
    startupRequest
    runQuery_ $ query One "create columnfamily cqltest.test (\
        \  a bigint\
        \, b ascii\
        \, c blob\
        \, d boolean\
        \, f decimal\
        \, g double\
        \, h float\
        \, i int\
        \, j timestamp\
        \, k uuid\
        \, l varchar\
        \, m varint\
        \, n timeuuid\
        \, o inet\
        \, p list<int>\
        \, q set<ascii>\
        \, r map<ascii,int>\
        \, primary key (a)\
        \)" ()

insertTable :: Client ()
insertTable = do
    startupRequest
    t <- liftIO $ getCurrentTime
    let r = Rec
            { ra = 483563763861853456384
            , rb = "hello world"
            , rc = Blob "blooooooooooooooooooooooob"
            , rd = False
            , rf = 1.2342342342423423423423423442
            , rg = 433243.13
            , rh = 1.23
            , ri = 2342342
            , rj = t
            , rk = fromJust . fromString $ "af93aafe-dea5-4427-bea4-8d7872507efb"
            , rl = "sdfsdžȢぴせそぼξλж҈Ҵאבג"
            , rm = 8763847563478568734687345683765873458734
            , rn = TimeUuid . fromJust $ fromString "559ab19e-52d8-11e3-a847-270bf6910c08"
            , ro = Inet4 0X7F00001
            , rp = [1,2,3]
            , rq = Set ["peter", "paul", "mary"]
            , rr = Map [("peter", 1), ("paul", 2), ("mary", 3)]
            }
    q1 (asTuple r)
    x <- map asRecord <$> q2 () :: Client [Rec]
    liftIO $ print x
  where
    q1 = runQuery_ . query One "insert into cqltest.test (a,b,c,d,f,g,h,i,j,k,l,m,n,o,p,q,r) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    q2 = runQuery . query One "select a,b,c,d,f,g,h,i,j,k,l,m,n,o,p,q,r from cqltest.test"


runQuery :: (Tuple a, Tuple b) => Query a -> Client [b]
runQuery q = do
    send (StreamId 1) q
    h <- readHeader
    r <- readBody h
    case r of
        RsResult _ (RowsResult _ b)        -> return b
        RsResult _ VoidResult              -> return []
        RsResult _ (SchemaChangeResult  _) -> return []
        _ -> fail $ "unexpected result: "

runQuery_ :: (Tuple a) => Query a -> Client ()
runQuery_ q = void (runQuery q :: Client [()])
