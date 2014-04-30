-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE BangPatterns      #-}
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
import Data.Bits
import Data.ByteString (ByteString)
import Data.Decimal
import Data.Int
import Data.List (unfoldr)
import Data.Text (Text)
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
    encode LocalOne    = encode (0x0A :: Word16)

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
    encode TextColumn       = encode (0x000A :: Word16)
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
        toType 0x000A = return TextColumn
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
putValue (CqlList x)         = toBytes 4 $ do
    put (fromIntegral (length x) :: Word16)
    mapM_ (toBytes 2 . putNative) x
putValue (CqlSet x)          = toBytes 4 $ do
    put (fromIntegral (length x) :: Word16)
    mapM_ (toBytes 2 . putNative) x
putValue (CqlMap x)          = toBytes 4 $ do
    put (fromIntegral (length x) :: Word16)
    forM_ x $ \(k, v) -> toBytes 2 (putNative k) >> toBytes 2 (putNative v)
putValue (CqlMaybe Nothing)  = put (-1 :: Int32)
putValue (CqlMaybe (Just x)) = putValue x
putValue value               = toBytes 4 $ putNative value

putNative :: Putter Value
putNative (CqlCustom x)    = putLazyByteString x
putNative (CqlBoolean x)   = putWord8 $ if x then 1 else 0
putNative (CqlInt x)       = put x
putNative (CqlBigInt x)    = put x
putNative (CqlFloat x)     = putFloat32be x
putNative (CqlDouble x)    = putFloat64be x
putNative (CqlText x)      = putByteString (T.encodeUtf8 x)
putNative (CqlUuid x)      = encode x
putNative (CqlTimeUuid x)  = encode x
putNative (CqlTimestamp x) = put x
putNative (CqlAscii x)     = putByteString (T.encodeUtf8 x)
putNative (CqlBlob x)      = putLazyByteString x
putNative (CqlCounter x)   = put x
putNative (CqlInet i)      = case i of
    Inet4 a       -> putWord32be a
    Inet6 a b c d -> do
        putWord32be a
        putWord32be b
        putWord32be c
        putWord32be d
putNative (CqlVarInt x)    = integer2bytes x
putNative (CqlDecimal x)   = do
    put (fromIntegral (decimalPlaces x) :: Int32)
    integer2bytes (decimalMantissa x)
putNative v@(CqlList  _)   = fail $ "putNative: collection type: " ++ show v
putNative v@(CqlSet   _)   = fail $ "putNative: collection type: " ++ show v
putNative v@(CqlMap   _)   = fail $ "putNative: collection type: " ++ show v
putNative v@(CqlMaybe _)   = fail $ "putNative: collection type: " ++ show v

getValue :: ColumnType -> Get Value
getValue (ListColumn t)   = withBytes 4 $ do
    len <- get :: Get Word16
    CqlList <$> replicateM (fromIntegral len) (withBytes 2 (getNative t))
getValue (SetColumn t)    = withBytes 4 $ do
    len <- get :: Get Word16
    CqlSet <$> replicateM (fromIntegral len) (withBytes 2 (getNative t))
getValue (MapColumn t u)  = withBytes 4 $ do
    len <- get :: Get Word16
    CqlMap <$> replicateM (fromIntegral len)
               ((,) <$> withBytes 2 (getNative t) <*> withBytes 2 (getNative u))
getValue (MaybeColumn t)  = do
    n <- lookAhead (get :: Get Int32)
    if n < 0
        then uncheckedSkip 4 >> return (CqlMaybe Nothing)
        else CqlMaybe . Just <$> getValue t
getValue colType          = withBytes 4 $ getNative colType

getNative :: ColumnType -> Get Value
getNative (CustomColumn _)  = CqlCustom <$> remainingBytesLazy
getNative BooleanColumn     = CqlBoolean . (/= 0) <$> getWord8
getNative IntColumn         = CqlInt <$> get
getNative BigIntColumn      = CqlBigInt <$> get
getNative FloatColumn       = CqlFloat  <$> getFloat32be
getNative DoubleColumn      = CqlDouble <$> getFloat64be
getNative TextColumn        = CqlText . T.decodeUtf8 <$> remainingBytes
getNative VarCharColumn     = CqlText . T.decodeUtf8 <$> remainingBytes
getNative AsciiColumn       = CqlAscii . T.decodeUtf8 <$> remainingBytes
getNative BlobColumn        = CqlBlob <$> remainingBytesLazy
getNative UuidColumn        = CqlUuid <$> decode
getNative TimeUuidColumn    = CqlTimeUuid <$> decode
getNative TimestampColumn   = CqlTimestamp <$> get
getNative CounterColumn     = CqlCounter <$> get
getNative InetColumn        = CqlInet <$> do
    len <- remaining
    case len of
        4  -> Inet4 <$> getWord32be
        16 -> Inet6 <$> getWord32be <*> getWord32be <*> getWord32be <*> getWord32be
        n -> fail $ "getNative: invalid Inet length: " ++ show n
getNative VarIntColumn      = CqlVarInt <$> bytes2integer
getNative DecimalColumn     = do
    x <- get :: Get Int32
    y <- bytes2integer
    return (CqlDecimal (Decimal (fromIntegral x) y))
getNative c@(ListColumn  _) = fail $ "getNative: collection type: " ++ show c
getNative c@(SetColumn   _) = fail $ "getNative: collection type: " ++ show c
getNative c@(MapColumn _ _) = fail $ "getNative: collection type: " ++ show c
getNative c@(MaybeColumn _) = fail $ "getNative: collection type: " ++ show c

withBytes :: Int -> Get a -> Get a
withBytes s p = do
    n <- case s of
        2 -> fromIntegral <$> (get :: Get Word16)
        4 -> fromIntegral <$> (get :: Get Int32)
        _ -> fail $ "withBytes: invalid size: " ++ show s
    when (n < 0) $
        fail "withBytes: null"
    b <- getBytes n
    case runGet p b of
        Left  e -> fail $ "withBytes: " ++ e
        Right x -> return x

remainingBytes :: Get ByteString
remainingBytes = remaining >>= getByteString . fromIntegral

remainingBytesLazy :: Get LB.ByteString
remainingBytesLazy = remaining >>= getLazyByteString . fromIntegral

toBytes :: Int -> Put -> Put
toBytes s p = do
    let bytes = runPut p
    case s of
        2 -> put (fromIntegral (B.length bytes) :: Word16)
        _ -> put (fromIntegral (B.length bytes) :: Int32)
    putByteString bytes

-- 'integer2bytes' and 'bytes2integer' implementations are taken
-- from cereal's instance declaration of 'Serialize' for 'Integer'
-- except that no distinction between small and large integers is made.
-- Cf. to LICENSE for copyright details.
integer2bytes :: Putter Integer
integer2bytes n = do
    put sign
    put (unroll (abs n))
  where
    sign = fromIntegral (signum n) :: Word8

    unroll :: Integer -> [Word8]
    unroll = unfoldr step
      where
        step 0 = Nothing
        step i = Just (fromIntegral i, i `shiftR` 8)

bytes2integer :: Get Integer
bytes2integer = do
    sign  <- get
    bytes <- get
    let v = roll bytes
    return $! if sign == (1 :: Word8) then v else - v
  where
    roll :: [Word8] -> Integer
    roll = foldr unstep 0
      where
        unstep b a = a `shiftL` 8 .|. fromIntegral b

------------------------------------------------------------------------------
-- Various

instance Decoding Keyspace where
    decode = Keyspace <$> decode

instance Decoding Table where
    decode = Table <$> decode

instance Decoding (QueryId k a b) where
    decode = QueryId <$> decode

encodeMaybe :: (Encoding a) => Putter (Maybe a)
encodeMaybe Nothing  = return ()
encodeMaybe (Just x) = encode x
