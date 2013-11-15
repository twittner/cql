module Test.Client where

import Data.ByteString.Lazy (ByteString, hGet, hPut, toStrict)
import Database.CQL
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

send :: Request a => Handle -> Compression -> Bool -> StreamId -> a -> IO ()
send h c t s r = do
    let b = pack c t s r
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
recvBody f h = case headerType h of
    RqHeader -> fail "unexpected request header"
    RsHeader -> do
        let len = lengthRepr (bodyLength h)
        b <- hGet f (fromIntegral len)
        fromIntegral len @=? LB.length b
        hexDump "Response Body" b
        case unpack id h b of
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
