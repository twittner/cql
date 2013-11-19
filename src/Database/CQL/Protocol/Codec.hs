-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.Protocol.Codec
    ( Encoding (..)
    , Decoding (..)

    , encWrite
    , encWriteLazy

    , decRead
    , decReadLazy

    , putValue
    , getValue

    , encodeMaybe
    ) where

import Control.Applicative
import Control.Monad
import Data.ByteString (ByteString)
import Data.Int
import Data.Text (Text)
import Data.Time
import Data.Time.Clock.POSIX
import Data.UUID (UUID)
import Data.Word
import Data.Serialize hiding (decode, encode)
import Database.CQL.Protocol.Types
import Network.Socket (SockAddr (..), PortNumber (..))

import qualified Data.ByteString         as B
import qualified Data.ByteString.Lazy    as LB
import qualified Data.Text.Encoding      as T
import qualified Data.Text.Lazy          as LT
import qualified Data.Text.Lazy.Encoding as LT
import qualified Data.UUID               as UUID

class Encoding a where
    encode :: Putter a

class Decoding a where
    decode :: Get a

encWrite :: (Encoding a) => a -> ByteString
encWrite = runPut . encode

encWriteLazy :: (Encoding a) => a -> LB.ByteString
encWriteLazy = runPutLazy . encode

decRead :: (Decoding a) => ByteString -> Either String a
decRead = runGet decode

decReadLazy :: (Decoding a) => LB.ByteString -> Either String a
decReadLazy = runGetLazy decode

------------------------------------------------------------------------------
-- Byte

instance Encoding Word8 where
    encode = put

instance Decoding Word8 where
    decode = get

------------------------------------------------------------------------------
-- Signed Byte

instance Encoding Int8 where
    encode = put

instance Decoding Int8 where
    decode = get

------------------------------------------------------------------------------
-- Short

instance Encoding Word16 where
    encode = put

instance Decoding Word16 where
    decode = get

------------------------------------------------------------------------------
-- Int

instance Encoding Int32 where
    encode = put

instance Decoding Int32 where
    decode = get

------------------------------------------------------------------------------
-- String

instance Encoding Text where
    encode = encode . T.encodeUtf8

instance Decoding Text where
    decode = T.decodeUtf8 <$> decode

------------------------------------------------------------------------------
-- Long String

instance Encoding LT.Text where
    encode = encode . LT.encodeUtf8

instance Decoding LT.Text where
    decode = do
        n <- get :: Get Int32
        LT.decodeUtf8 <$> getLazyByteString (fromIntegral n)

------------------------------------------------------------------------------
-- Bytes

instance Encoding LB.ByteString where
    encode bs = do
        put (fromIntegral (LB.length bs) :: Int32)
        putLazyByteString bs

instance Decoding (Maybe LB.ByteString) where
    decode = do
        n <- get :: Get Int32
        if n < 0
            then return Nothing
            else Just <$> getLazyByteString (fromIntegral n)

------------------------------------------------------------------------------
-- Short Bytes

instance Encoding ByteString where
    encode bs = do
        put (fromIntegral (B.length bs) :: Word16)
        putByteString bs

instance Decoding ByteString where
    decode = do
        n <- get :: Get Word16
        getByteString (fromIntegral n)

------------------------------------------------------------------------------
-- UUID

instance Encoding UUID where
    encode = putLazyByteString . UUID.toByteString

instance Decoding UUID where
    decode = do
        uuid <- UUID.fromByteString <$> getLazyByteString 16
        maybe (fail "decode-uuid: invalid") return uuid

------------------------------------------------------------------------------
-- String List

instance Encoding [Text] where
    encode sl = do
        put (fromIntegral (length sl) :: Word16)
        mapM_ encode sl

instance Decoding [Text] where
    decode = do
        n <- get :: Get Word16
        replicateM (fromIntegral n) decode

------------------------------------------------------------------------------
-- String Map

instance Encoding [(Text, Text)] where
    encode m = do
        put (fromIntegral (length m) :: Word16)
        forM_ m $ \(k, v) -> encode k >> encode v

instance Decoding [(Text, Text)] where
    decode = do
        n <- get :: Get Word16
        replicateM (fromIntegral n) ((,) <$> decode <*> decode)

------------------------------------------------------------------------------
-- String Multi-Map

instance Encoding [(Text, [Text])] where
    encode mm = do
        put (fromIntegral (length mm) :: Word16)
        forM_ mm $ \(k, v) -> encode k >> encode v

instance Decoding [(Text, [Text])] where
    decode = do
        n <- get :: Get Word16
        replicateM (fromIntegral n) ((,) <$> decode <*> decode)

------------------------------------------------------------------------------
-- Inet Address

instance Encoding SockAddr where
    encode (SockAddrInet (PortNum p) a) =
        putWord8 4 >> put p >> put a
    encode (SockAddrInet6 (PortNum p) _ (a, b, c, d) _) =
        putWord8 16 >> put p >> put a >> put b >> put c >> put d
    encode (SockAddrUnix _) = fail "encode-socket: unix address not allowed"

instance Decoding SockAddr where
    decode = do
        n <- getWord8
        case n of
            4  -> SockAddrInet  <$> getPort <*> getIPv4
            16 -> SockAddrInet6 <$> getPort <*> pure 0 <*> getIPv6 <*> pure 0
            _  -> fail $ "decode-socket: unknown: " ++ show n
      where
        getPort :: Get PortNumber
        getPort = PortNum <$> get

        getIPv4 :: Get Word32
        getIPv4 = get

        getIPv6 :: Get (Word32, Word32, Word32, Word32)
        getIPv6 = (,,,) <$> get <*> get <*> get <*> get

------------------------------------------------------------------------------
-- Consistency

instance Encoding Consistency where
    encode Any         = encode (0x00 :: Word16)
    encode One         = encode (0x01 :: Word16)
    encode Two         = encode (0x02 :: Word16)
    encode Three       = encode (0x03 :: Word16)
    encode Quorum      = encode (0x04 :: Word16)
    encode All         = encode (0x05 :: Word16)
    encode LocalQuorum = encode (0x06 :: Word16)
    encode EachQuorum  = encode (0x07 :: Word16)
    encode Serial      = encode (0x08 :: Word16)
    encode LocalSerial = encode (0x09 :: Word16)
    encode LocalOne    = encode (0x10 :: Word16)

instance Decoding Consistency where
    decode = decode >>= mapCode
      where
        mapCode :: Word16 -> Get Consistency
        mapCode 0x00 = return Any
        mapCode 0x01 = return One
        mapCode 0x02 = return Two
        mapCode 0x03 = return Three
        mapCode 0x04 = return Quorum
        mapCode 0x05 = return All
        mapCode 0x06 = return LocalQuorum
        mapCode 0x07 = return EachQuorum
        mapCode 0x08 = return Serial
        mapCode 0x09 = return LocalSerial
        mapCode 0x10 = return LocalOne
        mapCode code = fail $ "decode-consistency: unknown: " ++ show code

------------------------------------------------------------------------------
-- OpCode

instance Encoding OpCode where
    encode OcError         = encode (0x00 :: Word8)
    encode OcStartup       = encode (0x01 :: Word8)
    encode OcReady         = encode (0x02 :: Word8)
    encode OcAuthenticate  = encode (0x03 :: Word8)
    encode OcOptions       = encode (0x05 :: Word8)
    encode OcSupported     = encode (0x06 :: Word8)
    encode OcQuery         = encode (0x07 :: Word8)
    encode OcResult        = encode (0x08 :: Word8)
    encode OcPrepare       = encode (0x09 :: Word8)
    encode OcExecute       = encode (0x0A :: Word8)
    encode OcRegister      = encode (0x0B :: Word8)
    encode OcEvent         = encode (0x0C :: Word8)
    encode OcBatch         = encode (0x0D :: Word8)
    encode OcAuthChallenge = encode (0x0E :: Word8)
    encode OcAuthResponse  = encode (0x0F :: Word8)
    encode OcAuthSuccess   = encode (0x10 :: Word8)

instance Decoding OpCode where
    decode = decode >>= mapCode
      where
        mapCode :: Word8 -> Get OpCode
        mapCode 0x00 = return OcError
        mapCode 0x01 = return OcStartup
        mapCode 0x02 = return OcReady
        mapCode 0x03 = return OcAuthenticate
        mapCode 0x05 = return OcOptions
        mapCode 0x06 = return OcSupported
        mapCode 0x07 = return OcQuery
        mapCode 0x08 = return OcResult
        mapCode 0x09 = return OcPrepare
        mapCode 0x0A = return OcExecute
        mapCode 0x0B = return OcRegister
        mapCode 0x0C = return OcEvent
        mapCode 0x0D = return OcBatch
        mapCode 0x0E = return OcAuthChallenge
        mapCode 0x0F = return OcAuthResponse
        mapCode 0x10 = return OcAuthSuccess
        mapCode word = fail $ "decode-opcode: unknown: " ++ show word

------------------------------------------------------------------------------
-- ColumnType

instance Encoding ColumnType where
    encode (CustomColumn x) = encode (0x0000 :: Word16) >> encode x
    encode AsciiColumn      = encode (0x0001 :: Word16)
    encode BigIntColumn     = encode (0x0002 :: Word16)
    encode BlobColumn       = encode (0x0003 :: Word16)
    encode BooleanColumn    = encode (0x0004 :: Word16)
    encode CounterColumn    = encode (0x0005 :: Word16)
    encode DecimalColumn    = encode (0x0006 :: Word16)
    encode DoubleColumn     = encode (0x0007 :: Word16)
    encode FloatColumn      = encode (0x0008 :: Word16)
    encode IntColumn        = encode (0x0009 :: Word16)
    encode TimestampColumn  = encode (0x000B :: Word16)
    encode UuidColumn       = encode (0x000C :: Word16)
    encode VarCharColumn    = encode (0x000D :: Word16)
    encode VarIntColumn     = encode (0x000E :: Word16)
    encode TimeUuidColumn   = encode (0x000F :: Word16)
    encode InetColumn       = encode (0x0010 :: Word16)
    encode (MaybeColumn x)  = encode x
    encode (ListColumn x)   = encode (0x0020 :: Word16) >> encode x
    encode (MapColumn  x y) = encode (0x0021 :: Word16) >> encode x >> encode y
    encode (SetColumn  x)   = encode (0x0022 :: Word16) >> encode x

instance Decoding ColumnType where
    decode = decode >>= toType
      where
        toType :: Word16 -> Get ColumnType
        toType 0x0000 = CustomColumn <$> decode
        toType 0x0001 = return AsciiColumn
        toType 0x0002 = return BigIntColumn
        toType 0x0003 = return BlobColumn
        toType 0x0004 = return BooleanColumn
        toType 0x0005 = return CounterColumn
        toType 0x0006 = return DecimalColumn
        toType 0x0007 = return DoubleColumn
        toType 0x0008 = return FloatColumn
        toType 0x0009 = return IntColumn
        toType 0x000B = return TimestampColumn
        toType 0x000C = return UuidColumn
        toType 0x000D = return VarCharColumn
        toType 0x000E = return VarIntColumn
        toType 0x000F = return TimeUuidColumn
        toType 0x0010 = return InetColumn
        toType 0x0020 = ListColumn <$> (decode >>= toType)
        toType 0x0021 = MapColumn  <$> (decode >>= toType) <*> (decode >>= toType)
        toType 0x0022 = SetColumn  <$> (decode >>= toType)
        toType other  = fail $ "decode-type: unknown: " ++ show other

------------------------------------------------------------------------------
-- Paging State

instance Encoding PagingState where
    encode (PagingState s) = encode s

instance Decoding (Maybe PagingState) where
    decode = liftM PagingState <$> decode

------------------------------------------------------------------------------
-- Value

putValue :: Putter Value
putValue (CqlCustom x)       = encode x
putValue (CqlBoolean x)      = toBytes $ putWord8 $ if x then 1 else 0
putValue (CqlInt x)          = toBytes $ put x
putValue (CqlBigInt x)       = toBytes $ put x
putValue (CqlFloat x)        = toBytes $ putFloat32be x
putValue (CqlDouble x)       = toBytes $ putFloat64be x
putValue (CqlVarChar x)      = toBytes $ putByteString (T.encodeUtf8 x)
putValue (CqlInet x)         = toBytes $ encode x
putValue (CqlUuid x)         = toBytes $ encode x
putValue (CqlTimeUuid x)     = toBytes $ encode x
putValue (CqlTimestamp x)    = toBytes $ put (timestamp x)
putValue (CqlAscii x)        = toBytes $ putByteString (T.encodeUtf8 x)
putValue (CqlBlob x)         = encode x
putValue (CqlCounter x)      = toBytes $ put x
putValue (CqlList x)         = toBytes $ do
    put (fromIntegral (length x) :: Word16)
    mapM_ putValue x
putValue (CqlSet x)          = toBytes $ do
    put (fromIntegral (length x) :: Word16)
    mapM_ putValue x
putValue (CqlMap x)          = toBytes $ do
    put (fromIntegral (length x) :: Word16)
    forM_ x $ \(k, v) -> putValue k >> putValue v
putValue (CqlMaybe Nothing)  = put (-1 :: Int32)
putValue (CqlMaybe (Just x)) = toBytes $ putValue x
putValue (CqlVarInt _)       = undefined -- TODO
putValue (CqlDecimal _)      = undefined -- TODO

getValue :: ColumnType -> Get Value
getValue (CustomColumn _) = withBytes $ CqlCustom <$> remainingBytesLazy
getValue BooleanColumn    = withBytes $ CqlBoolean . (/= 0) <$> getWord8
getValue IntColumn        = withBytes $ CqlInt <$> get
getValue BigIntColumn     = withBytes $ CqlBigInt <$> get
getValue FloatColumn      = withBytes $ CqlFloat  <$> getFloat32be
getValue DoubleColumn     = withBytes $ CqlDouble <$> getFloat64be
getValue VarCharColumn    = withBytes $ CqlVarChar . T.decodeUtf8 <$> remainingBytes
getValue AsciiColumn      = withBytes $ CqlAscii . T.decodeUtf8 <$> remainingBytes
getValue BlobColumn       = withBytes $ CqlBlob <$> remainingBytesLazy
getValue InetColumn       = withBytes $ CqlInet <$> decode
getValue UuidColumn       = withBytes $ CqlUuid <$> decode
getValue TimeUuidColumn   = withBytes $ CqlTimeUuid <$> decode
getValue TimestampColumn  = withBytes $ do
    CqlBigInt x <- getValue BigIntColumn
    return $ CqlTimestamp (time x)
getValue CounterColumn    = withBytes $ CqlCounter <$> get
getValue (ListColumn t)   = withBytes $ do
    len <- get :: Get Word16
    CqlList <$> replicateM (fromIntegral len) (getValue t)
getValue (SetColumn t)    = withBytes $ do
    len <- get :: Get Word16
    CqlSet <$> replicateM (fromIntegral len) (getValue t)
getValue (MapColumn t u)  = withBytes $ do
    len <- get :: Get Word16
    CqlMap <$> replicateM (fromIntegral len) ((,) <$> getValue t <*> getValue u)
getValue (MaybeColumn t)  = do
    n <- lookAhead (get :: Get Int32)
    if n < 0
        then uncheckedSkip 4 >> return (CqlMaybe Nothing)
        else CqlMaybe . Just <$> getValue t
getValue DecimalColumn    = undefined -- TODO
getValue VarIntColumn     = undefined -- TODO

withBytes :: Get a -> Get a
withBytes p = do
    n <- fromIntegral <$> (get :: Get Int32)
    when (n < 0) $
        fail $ "withBytes: null"
    b <- getBytes n
    case runGet p b of
        Left  e -> fail $ "withBytes: " ++ e
        Right x -> return x

remainingBytes :: Get ByteString
remainingBytes = remaining >>= getByteString . fromIntegral

remainingBytesLazy :: Get LB.ByteString
remainingBytesLazy = remaining >>= getLazyByteString . fromIntegral

toBytes :: Put -> Put
toBytes p = do
    let bytes = runPut p
    put (fromIntegral (B.length bytes) :: Int32)
    putByteString bytes

timestamp :: UTCTime -> Int64
timestamp = truncate
          . (* (1000 :: Double))
          . realToFrac
          . utcTimeToPOSIXSeconds

time :: Int64 -> UTCTime
time ts =
    let (s, ms)     = ts `divMod` 1000
        UTCTime a b = posixSecondsToUTCTime (fromIntegral s)
        ps          = fromIntegral ms * 1000000000
    in UTCTime a (b + picosecondsToDiffTime ps)

------------------------------------------------------------------------------
-- Various

instance Decoding Keyspace where
    decode = Keyspace <$> decode

instance Decoding Table where
    decode = Table <$> decode

instance Decoding QueryId where
    decode = QueryId <$> decode

encodeMaybe :: (Encoding a) => Putter (Maybe a)
encodeMaybe Nothing  = return ()
encodeMaybe (Just x) = encode x
