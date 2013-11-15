{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Test.Client
    ( Client
    , runClient
    , send
    , readHeader
    , readBody
    ) where

import Control.Applicative
import Control.Exception (bracket)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Reader
import Data.ByteString.Lazy (ByteString, hGet, hPut, toStrict)
import Database.CQL
import Hexdump
import Network
import System.IO
import System.Console.ANSI
import Test.Tasty.HUnit

import qualified Data.ByteString.Lazy as LB

data Env = Env
    { verbose    :: !Bool
    , connection :: !Handle
    }

newtype Client a = Client
    { client :: ReaderT Env IO a
    } deriving (Functor, Applicative, Monad, MonadIO, MonadReader Env)

runClient :: Client a -> Bool -> String -> Int -> IO a
runClient c v h p =
    bracket (connectTo h (PortNumber . fromIntegral $ p)) hClose $ \x -> do
    hSetBinaryMode x True
    hSetBuffering x NoBuffering
    runReaderT (client c) (Env v x)

send :: Request a => Compression -> Bool -> StreamId -> a -> Client ()
send c t s r = do
    let b = pack c t s r
    hexDump "Request" b
    writeBytes b

readHeader :: Client Header
readHeader = do
    b <- readBytes 8
    hexDump "Response Header" b
    case header b of
        Left  e -> fail $ "readHeader: " ++ e
        Right x -> return x

readBody :: (Row a) => Header -> Client (Response a)
readBody h = case headerType h of
    RqHeader -> fail "unexpected request header"
    RsHeader -> do
        let len = lengthRepr (bodyLength h)
        b <- readBytes (fromIntegral len)
        liftIO $ fromIntegral len @=? LB.length b
        hexDump "Response Body" b
        case unpack id h b of
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
        writeLn Cyan  $ "\n" ++ h
        writeLn White $ prettyHex (toStrict b)
  where
    writeLn c = liftIO . withColour c . Prelude.putStrLn

    withColour c a = do
        setSGR [Reset, SetColor Foreground Vivid c]
        void $ a
        setSGR [Reset]
