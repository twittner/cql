-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module Database.CQL.Protocol.Codec
    ( encodeByte
    , decodeByte

    , encodeSignedByte
    , decodeSignedByte

    , encodeShort
    , decodeShort

    , encodeSignedShort
    , decodeSignedShort

    , encodeInt
    , decodeInt

    , encodeString
    , decodeString

    , encodeLongString
    , decodeLongString

    , encodeBytes
    , decodeBytes

    , encodeShortBytes
    , decodeShortBytes

    , encodeUUID
    , decodeUUID

    , encodeList
    , decodeList

    , encodeMap
    , decodeMap

    , encodeMultiMap
    , decodeMultiMap

    , encodeSockAddr
    , decodeSockAddr

    , encodeConsistency
    , decodeConsistency

    , encodeOpCode
    , decodeOpCode

    , encodeColumnType
    , decodeColumnType

    , encodePagingState
    , decodePagingState

    , decodeKeyspace
    , decodeTable
    , decodeQueryId

    , putValue
    , getValue
    ) where

import Control.Applicative
import Control.Monad
import Data.Bits
import Data.ByteString (ByteString)
import Data.Decimal
import Data.Int
import Data.IP
import Data.List (unfoldr)
import Data.Text (Text)
import Data.UUID (UUID)
import Data.Word
import Data.Serialize hiding (decode, encode)
import Database.CQL.Protocol.Types
import Network.Socket (SockAddr (..), PortNumber (..))
import Prelude

import qualified Data.ByteString         as B
import qualified Data.ByteString.Lazy    as LB
import qualified Data.Text.Encoding      as T
import qualified Data.Text.Lazy          as LT
import qualified Data.Text.Lazy.Encoding as LT
import qualified Data.UUID               as UUID

------------------------------------------------------------------------------
-- Byte

encodeByte :: Putter Word8
encodeByte = put

decodeByte :: Get Word8
decodeByte = get

------------------------------------------------------------------------------
-- Signed Byte

encodeSignedByte :: Putter Int8
encodeSignedByte = put

decodeSignedByte :: Get Int8
decodeSignedByte = get

------------------------------------------------------------------------------
-- Short

encodeShort :: Putter Word16
encodeShort = put

decodeShort :: Get Word16
decodeShort = get

------------------------------------------------------------------------------
-- Signed Short

encodeSignedShort :: Putter Int16
encodeSignedShort = put

decodeSignedShort :: Get Int16
decodeSignedShort = get

------------------------------------------------------------------------------
-- Int

encodeInt :: Putter Int32
encodeInt = put

decodeInt :: Get Int32
decodeInt = get

------------------------------------------------------------------------------
-- String

encodeString :: Putter Text
encodeString = encodeShortBytes . T.encodeUtf8

decodeString :: Get Text
decodeString = T.decodeUtf8 <$> decodeShortBytes

------------------------------------------------------------------------------
-- Long String

encodeLongString :: Putter LT.Text
encodeLongString = encodeBytes . LT.encodeUtf8

decodeLongString :: Get LT.Text
decodeLongString = do
    n <- get :: Get Int32
    LT.decodeUtf8 <$> getLazyByteString (fromIntegral n)

------------------------------------------------------------------------------
-- Bytes

encodeBytes :: Putter LB.ByteString
encodeBytes bs = do
    put (fromIntegral (LB.length bs) :: Int32)
    putLazyByteString bs

decodeBytes :: Get (Maybe LB.ByteString)
decodeBytes = do
    n <- get :: Get Int32
    if n < 0
        then return Nothing
        else Just <$> getLazyByteString (fromIntegral n)

------------------------------------------------------------------------------
-- Short Bytes

encodeShortBytes :: Putter ByteString
encodeShortBytes bs = do
    put (fromIntegral (B.length bs) :: Word16)
    putByteString bs

decodeShortBytes :: Get ByteString
decodeShortBytes = do
    n <- get :: Get Word16
    getByteString (fromIntegral n)

------------------------------------------------------------------------------
-- UUID

encodeUUID :: Putter UUID
encodeUUID = putLazyByteString . UUID.toByteString

decodeUUID :: Get UUID
decodeUUID = do
    uuid <- UUID.fromByteString <$> getLazyByteString 16
    maybe (fail "decode-uuid: invalid") return uuid

------------------------------------------------------------------------------
-- String List

encodeList :: Putter [Text]
encodeList sl = do
    put (fromIntegral (length sl) :: Word16)
    mapM_ encodeString sl

decodeList :: Get [Text]
decodeList = do
    n <- get :: Get Word16
    replicateM (fromIntegral n) decodeString

------------------------------------------------------------------------------
-- String Map

encodeMap :: Putter [(Text, Text)]
encodeMap m = do
    put (fromIntegral (length m) :: Word16)
    forM_ m $ \(k, v) -> encodeString k >> encodeString v

decodeMap :: Get [(Text, Text)]
decodeMap = do
    n <- get :: Get Word16
    replicateM (fromIntegral n) ((,) <$> decodeString <*> decodeString)

------------------------------------------------------------------------------
-- String Multi-Map

encodeMultiMap :: Putter [(Text, [Text])]
encodeMultiMap mm = do
    put (fromIntegral (length mm) :: Word16)
    forM_ mm $ \(k, v) -> encodeString k >> encodeList v

decodeMultiMap :: Get [(Text, [Text])]
decodeMultiMap = do
    n <- get :: Get Word16
    replicateM (fromIntegral n) ((,) <$> decodeString <*> decodeList)

------------------------------------------------------------------------------
-- Inet Address

encodeSockAddr :: Putter SockAddr
encodeSockAddr (SockAddrInet p a) = do
    putWord8 4
    putWord32le a
    putWord32be (fromIntegral p)
encodeSockAddr (SockAddrInet6 p _ (a, b, c, d) _) = do
    putWord8 16
    putWord32host a
    putWord32host b
    putWord32host c
    putWord32host d
    putWord32be (fromIntegral p)
encodeSockAddr (SockAddrUnix _) = fail "encode-socket: unix address not allowed"
#if MIN_VERSION_network(2,6,1)
encodeSockAddr (SockAddrCan _) = fail "encode-socket: can address not allowed"
#endif

decodeSockAddr :: Get SockAddr
decodeSockAddr = do
    n <- getWord8
    case n of
        4  -> do
            i <- getIPv4
            p <- getPort
            return $ SockAddrInet p i
        16 -> do
            i <- getIPv6
            p <- getPort
            return $ SockAddrInet6 p 0 i 0
        _  -> fail $ "decode-socket: unknown: " ++ show n
  where
    getPort :: Get PortNumber
    getPort = fromIntegral <$> getWord32be

    getIPv4 :: Get Word32
    getIPv4 = getWord32le

    getIPv6 :: Get (Word32, Word32, Word32, Word32)
    getIPv6 = (,,,) <$> getWord32host <*> getWord32host <*> getWord32host <*> getWord32host

------------------------------------------------------------------------------
-- Consistency

encodeConsistency :: Putter Consistency
encodeConsistency Any         = encodeShort 0x00
encodeConsistency One         = encodeShort 0x01
encodeConsistency Two         = encodeShort 0x02
encodeConsistency Three       = encodeShort 0x03
encodeConsistency Quorum      = encodeShort 0x04
encodeConsistency All         = encodeShort 0x05
encodeConsistency LocalQuorum = encodeShort 0x06
encodeConsistency EachQuorum  = encodeShort 0x07
encodeConsistency Serial      = encodeShort 0x08
encodeConsistency LocalSerial = encodeShort 0x09
encodeConsistency LocalOne    = encodeShort 0x0A

decodeConsistency :: Get Consistency
decodeConsistency = decodeShort >>= mapCode
      where
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

encodeOpCode :: Putter OpCode
encodeOpCode OcError         = encodeByte 0x00
encodeOpCode OcStartup       = encodeByte 0x01
encodeOpCode OcReady         = encodeByte 0x02
encodeOpCode OcAuthenticate  = encodeByte 0x03
encodeOpCode OcOptions       = encodeByte 0x05
encodeOpCode OcSupported     = encodeByte 0x06
encodeOpCode OcQuery         = encodeByte 0x07
encodeOpCode OcResult        = encodeByte 0x08
encodeOpCode OcPrepare       = encodeByte 0x09
encodeOpCode OcExecute       = encodeByte 0x0A
encodeOpCode OcRegister      = encodeByte 0x0B
encodeOpCode OcEvent         = encodeByte 0x0C
encodeOpCode OcBatch         = encodeByte 0x0D
encodeOpCode OcAuthChallenge = encodeByte 0x0E
encodeOpCode OcAuthResponse  = encodeByte 0x0F
encodeOpCode OcAuthSuccess   = encodeByte 0x10

decodeOpCode :: Get OpCode
decodeOpCode = decodeByte >>= mapCode
  where
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

encodeColumnType :: Putter ColumnType
encodeColumnType (CustomColumn x)   = encodeShort 0x0000 >> encodeString x
encodeColumnType AsciiColumn        = encodeShort 0x0001
encodeColumnType BigIntColumn       = encodeShort 0x0002
encodeColumnType BlobColumn         = encodeShort 0x0003
encodeColumnType BooleanColumn      = encodeShort 0x0004
encodeColumnType CounterColumn      = encodeShort 0x0005
encodeColumnType DecimalColumn      = encodeShort 0x0006
encodeColumnType DoubleColumn       = encodeShort 0x0007
encodeColumnType FloatColumn        = encodeShort 0x0008
encodeColumnType IntColumn          = encodeShort 0x0009
encodeColumnType TextColumn         = encodeShort 0x000A
encodeColumnType TimestampColumn    = encodeShort 0x000B
encodeColumnType UuidColumn         = encodeShort 0x000C
encodeColumnType VarCharColumn      = encodeShort 0x000D
encodeColumnType VarIntColumn       = encodeShort 0x000E
encodeColumnType TimeUuidColumn     = encodeShort 0x000F
encodeColumnType InetColumn         = encodeShort 0x0010
encodeColumnType (MaybeColumn x)    = encodeColumnType x
encodeColumnType (ListColumn x)     = encodeShort 0x0020 >> encodeColumnType x
encodeColumnType (MapColumn  x y)   = encodeShort 0x0021 >> encodeColumnType x >> encodeColumnType y
encodeColumnType (SetColumn  x)     = encodeShort 0x0022 >> encodeColumnType x
encodeColumnType (TupleColumn xs)   = encodeShort 0x0031 >> mapM_ encodeColumnType xs
encodeColumnType (UdtColumn k n xs) = do
    encodeShort 0x0030
    encodeString (unKeyspace k)
    encodeString n
    encodeShort (fromIntegral (length xs))
    forM_ xs $ \(x, t) -> encodeString x >> encodeColumnType t

decodeColumnType :: Get ColumnType
decodeColumnType = decodeShort >>= toType
  where
    toType 0x0000 = CustomColumn <$> decodeString
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
    toType 0x0020 = ListColumn  <$> (decodeShort >>= toType)
    toType 0x0021 = MapColumn   <$> (decodeShort >>= toType) <*> (decodeShort >>= toType)
    toType 0x0022 = SetColumn   <$> (decodeShort >>= toType)
    toType 0x0030 = UdtColumn   <$> (Keyspace <$> decodeString) <*> decodeString <*> do
        n <- fromIntegral <$> decodeShort
        replicateM n ((,) <$> decodeString <*> (decodeShort >>= toType))
    toType 0x0031 = TupleColumn <$> do
        n <- fromIntegral <$> decodeShort
        replicateM n (decodeShort >>= toType)
    toType other  = fail $ "decode-type: unknown: " ++ show other

------------------------------------------------------------------------------
-- Paging State

encodePagingState :: Putter PagingState
encodePagingState (PagingState s) = encodeBytes s

decodePagingState :: Get (Maybe PagingState)
decodePagingState = liftM PagingState <$> decodeBytes

------------------------------------------------------------------------------
-- Value

putValue :: Version -> Putter Value
putValue V3 (CqlList x)        = toBytes 4 $ do
    encodeInt (fromIntegral (length x))
    mapM_ (toBytes 4 . putNative V3) x
putValue V2 (CqlList x)        = toBytes 4 $ do
    encodeShort (fromIntegral (length x))
    mapM_ (toBytes 2 . putNative V2) x
putValue V3 (CqlSet x)         = toBytes 4 $ do
    encodeInt (fromIntegral (length x))
    mapM_ (toBytes 4 . putNative V3) x
putValue V2 (CqlSet x)         = toBytes 4 $ do
    encodeShort (fromIntegral (length x))
    mapM_ (toBytes 2 . putNative V2) x
putValue V3 (CqlMap x)         = toBytes 4 $ do
    encodeInt (fromIntegral (length x))
    forM_ x $ \(k, v) -> toBytes 4 (putNative V3 k) >> toBytes 4 (putNative V3 v)
putValue V2 (CqlMap x)         = toBytes 4 $ do
    encodeShort (fromIntegral (length x))
    forM_ x $ \(k, v) -> toBytes 2 (putNative V2 k) >> toBytes 2 (putNative V2 v)
putValue V3 (CqlTuple x)       =
    toBytes 4 $ putByteString $ runPut (mapM_ (putValue V3) x)
putValue _ (CqlMaybe Nothing)  = put (-1 :: Int32)
putValue v (CqlMaybe (Just x)) = putValue v x
putValue v value               = toBytes 4 $ putNative v value

putNative :: Version -> Putter Value
putNative _ (CqlCustom x)    = putLazyByteString x
putNative _ (CqlBoolean x)   = putWord8 $ if x then 1 else 0
putNative _ (CqlInt x)       = put x
putNative _ (CqlBigInt x)    = put x
putNative _ (CqlFloat x)     = putFloat32be x
putNative _ (CqlDouble x)    = putFloat64be x
putNative _ (CqlText x)      = putByteString (T.encodeUtf8 x)
putNative _ (CqlUuid x)      = encodeUUID x
putNative _ (CqlTimeUuid x)  = encodeUUID x
putNative _ (CqlTimestamp x) = put x
putNative _ (CqlAscii x)     = putByteString (T.encodeUtf8 x)
putNative _ (CqlBlob x)      = putLazyByteString x
putNative _ (CqlCounter x)   = put x
putNative _ (CqlInet x)      = case x of
    IPv4 i -> putWord32le (toHostAddress i)
    IPv6 i -> do
        let (a, b, c, d) = toHostAddress6 i
        putWord32host a
        putWord32host b
        putWord32host c
        putWord32host d
putNative _ (CqlVarInt x)    = integer2bytes x
putNative _ (CqlDecimal x)   = do
    put (fromIntegral (decimalPlaces x) :: Int32)
    integer2bytes (decimalMantissa x)
putNative V3 (CqlUdt  x)     = putByteString $ runPut (mapM_ (putValue V3 . snd) x)
putNative V2 v@(CqlUdt  _)   = fail $ "putNative: udt: " ++ show v
putNative _ v@(CqlList  _)   = fail $ "putNative: collection type: " ++ show v
putNative _ v@(CqlSet   _)   = fail $ "putNative: collection type: " ++ show v
putNative _ v@(CqlMap   _)   = fail $ "putNative: collection type: " ++ show v
putNative _ v@(CqlMaybe _)   = fail $ "putNative: collection type: " ++ show v
putNative _ v@(CqlTuple _)   = fail $ "putNative: tuple type: " ++ show v

-- Note: Empty lists, maps and sets are represented as null in cassandra.
getValue :: Version -> ColumnType -> Get Value
getValue V3 (ListColumn t)    = CqlList <$> (getList $ do
    len <- decodeInt
    replicateM (fromIntegral len) (withBytes 4 (getNative V3 t)))
getValue V2 (ListColumn t)    = CqlList <$> (getList $ do
    len <- decodeShort
    replicateM (fromIntegral len) (withBytes 2 (getNative V2 t)))
getValue V3 (SetColumn t)     = CqlSet <$> (getList $ do
    len <- decodeInt
    replicateM (fromIntegral len) (withBytes 4 (getNative V3 t)))
getValue V2 (SetColumn t)     = CqlSet <$> (getList $ do
    len <- decodeShort
    replicateM (fromIntegral len) (withBytes 2 (getNative V2 t)))
getValue V3 (MapColumn t u)   = CqlMap <$> (getList $ do
    len <- decodeInt
    replicateM (fromIntegral len)
               ((,) <$> withBytes 4 (getNative V3 t) <*> withBytes 4 (getNative V3 u)))
getValue V2 (MapColumn t u)   = CqlMap <$> (getList $ do
    len <- decodeShort
    replicateM (fromIntegral len)
               ((,) <$> withBytes 2 (getNative V2 t) <*> withBytes 2 (getNative V2 u)))
getValue V3 (TupleColumn t)   = do
    b <- withBytes 4 remainingBytes
    either fail return $ flip runGet b $ CqlTuple <$> mapM (getValue V3) t
getValue v (MaybeColumn t)    = do
    n <- lookAhead (get :: Get Int32)
    if n < 0
        then uncheckedSkip 4 >> return (CqlMaybe Nothing)
        else CqlMaybe . Just <$> getValue v t
getValue v colType            = withBytes 4 $ getNative v colType

getNative :: Version -> ColumnType -> Get Value
getNative _ (CustomColumn _) = CqlCustom <$> remainingBytesLazy
getNative _ BooleanColumn    = CqlBoolean . (/= 0) <$> getWord8
getNative _ IntColumn        = CqlInt <$> get
getNative _ BigIntColumn     = CqlBigInt <$> get
getNative _ FloatColumn      = CqlFloat  <$> getFloat32be
getNative _ DoubleColumn     = CqlDouble <$> getFloat64be
getNative _ TextColumn       = CqlText . T.decodeUtf8 <$> remainingBytes
getNative _ VarCharColumn    = CqlText . T.decodeUtf8 <$> remainingBytes
getNative _ AsciiColumn      = CqlAscii . T.decodeUtf8 <$> remainingBytes
getNative _ BlobColumn       = CqlBlob <$> remainingBytesLazy
getNative _ UuidColumn       = CqlUuid <$> decodeUUID
getNative _ TimeUuidColumn   = CqlTimeUuid <$> decodeUUID
getNative _ TimestampColumn  = CqlTimestamp <$> get
getNative _ CounterColumn    = CqlCounter <$> get
getNative _ InetColumn       = CqlInet <$> do
    len <- remaining
    case len of
        4  -> IPv4 . fromHostAddress <$> getWord32le
        16 -> do
            a <- (,,,) <$> getWord32host <*> getWord32host <*> getWord32host <*> getWord32host
            return $ IPv6 (fromHostAddress6 a)
        n  -> fail $ "getNative: invalid Inet length: " ++ show n
getNative _ VarIntColumn  = CqlVarInt <$> bytes2integer
getNative _ DecimalColumn = do
    x <- get :: Get Int32
    y <- bytes2integer
    return (CqlDecimal (Decimal (fromIntegral x) y))
getNative V3 (UdtColumn _ _ x) = do
    b <- remainingBytes
    either fail return $ flip runGet b $ CqlUdt <$> do
        let (n, t) = unzip x
        zip n <$> mapM (getValue V3) t
getNative V2 c@(UdtColumn _ _ _) = fail $ "getNative: udt: " ++ show c
getNative _ c@(ListColumn  _)    = fail $ "getNative: collection type: " ++ show c
getNative _ c@(SetColumn   _)    = fail $ "getNative: collection type: " ++ show c
getNative _ c@(MapColumn _ _)    = fail $ "getNative: collection type: " ++ show c
getNative _ c@(MaybeColumn _)    = fail $ "getNative: collection type: " ++ show c
getNative _ c@(TupleColumn _)    = fail $ "getNative: tuple type: " ++ show c

getList :: Get [a] -> Get [a]
getList m = do
    n <- lookAhead (get :: Get Int32)
    if n < 0 then uncheckedSkip 4 >> return []
             else withBytes 4 m

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

decodeKeyspace :: Get Keyspace
decodeKeyspace = Keyspace <$> decodeString

decodeTable :: Get Table
decodeTable = Table <$> decodeString

decodeQueryId :: Get (QueryId k a b)
decodeQueryId = QueryId <$> decodeShortBytes
