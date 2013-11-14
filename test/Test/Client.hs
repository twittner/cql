module Test.Client where

import Data.ByteString.Lazy
import Database.CQL
import Database.CQL.Frame.Codec
import Hexdump
import Network
import System.IO
import System.Console.ANSI
import Test.Tasty.HUnit

import qualified Data.ByteString.Lazy as LB

open :: String -> Int -> IO Handle
open h p = do
    c <- connectTo h (PortNumber . fromIntegral $ p)
    hSetBinaryMode c True
    hSetBuffering c NoBuffering
    return c

close :: Handle -> IO ()
close = hClose

sendRequest :: Handle -> Request -> IO ()
sendRequest h r = do
    let b = encWriteLazy r
    hexDump "Request" b
    hPut h b

recvHeader :: Handle -> IO Header
recvHeader h = do
    b <- hGet h 8
    hexDump "Response Header" b
    case header b of
        Left  e -> fail $ "recvHeader: " ++ e
        Right x -> return x

recvBody :: (FromCQL a) => Handle -> Header -> IO (Response a)
recvBody _ (RequestHeader  _) = fail "unexpected request header"
recvBody h (ResponseHeader d) = do
    let len = unLength (hdrLength d)
    b <- hGet h (fromIntegral len)
    fromIntegral len @=? LB.length b
    hexDump "Response Body" b
    case response id d b of
        Left  e -> fail $ "recvBody: " ++ e
        Right x -> return x

hexDump :: String -> ByteString -> IO ()
hexDump h b = do
    writeLn Cyan  $ "\n" ++ h
    writeLn White $ prettyHex (toStrict b)

write, writeLn :: Color -> String -> IO ()
write c = withColour c . Prelude.putStr
writeLn c = withColour c . Prelude.putStrLn

withColour :: Color -> IO () -> IO ()
withColour c a = do
    setSGR [Reset, SetColor Foreground Vivid c]
    a
    setSGR [Reset]
