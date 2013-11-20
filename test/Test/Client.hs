-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module Test.Client
    ( Client
    , runClient
    , send
    , readHeader
    , readBody
    , compress
    , verbose
    , module R
    ) where

import Control.Applicative
import Control.Exception (bracket)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Reader as R
import Data.ByteString.Lazy (ByteString, hGet, hPut, toStrict)
import Database.CQL.Protocol
import Hexdump
import Network
import System.IO
import System.Console.ANSI
import Test.Tasty.HUnit

import qualified Data.ByteString.Lazy as LB

data Env = Env
    { verbose    :: !Bool
    , compress   :: !Compression
    , connection :: !Handle
    }

newtype Client a = Client
    { client :: ReaderT Env IO a
    } deriving (Functor, Applicative, Monad, MonadIO, MonadReader Env)

runClient :: Client a -> Bool -> Compression -> String -> Int -> IO a
runClient c v f h p =
    bracket (connectTo h (PortNumber . fromIntegral $ p)) hClose $ \x -> do
    hSetBinaryMode x True
    hSetBuffering x NoBuffering
    runReaderT (client c) (Env v f x)

send :: (Request a) => StreamId -> a -> Client ()
send s r = do
    t <- asks verbose
    c <- case getOpCode r rqCode of
        OcStartup -> return None
        OcOptions -> return None
        _         -> asks compress
    b <- either (fail "send: request generation failed") return (pack c t s r)
    hexDump "Request" b
    writeBytes b

readHeader :: Client Header
readHeader = do
    b <- readBytes 8
    hexDump "Response Header" b
    case header b of
        Left  e -> fail $ "readHeader: " ++ e
        Right x -> return x

readBody :: (Tuple a) => Header -> Client (Response a)
readBody h = case headerType h of
    RqHeader -> fail "unexpected request header"
    RsHeader -> do
        let len = lengthRepr (bodyLength h)
        b <- readBytes (fromIntegral len)
        liftIO $ fromIntegral len @=? LB.length b
        hexDump "Response Body" b
        c <- asks compress
        case unpack c h b of
            Left  e -> fail $ "readBody: " ++ e
            Right x -> return x

------------------------------------------------------------------------------
-- Internal

readBytes :: Int -> Client ByteString
readBytes n = asks connection >>= liftIO . flip hGet n

writeBytes :: ByteString -> Client ()
writeBytes b = asks connection >>= liftIO . flip hPut b

hexDump :: String -> ByteString -> Client ()
hexDump h b = do
    v <- asks verbose
    when v $ do
        writeLn Cyan  $ '\n' : h
        writeLn White $ prettyHex (toStrict b)
  where
    writeLn c = liftIO . withColour c . Prelude.putStrLn

    withColour c a = do
        setSGR [Reset, SetColor Foreground Vivid c]
        void a
        setSGR [Reset]
