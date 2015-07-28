-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE CPP                 #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQL.Protocol.Tuple.TH where

import Control.Applicative
import Control.Monad
import Data.Functor.Identity
import Data.Serialize
import Data.Vector (Vector, (!?))
import Data.Word
import Database.CQL.Protocol.Class
import Database.CQL.Protocol.Codec (putValue, getValue)
import Database.CQL.Protocol.Types
import Language.Haskell.TH
import Prelude

import qualified Data.Vector as Vec

------------------------------------------------------------------------------
-- Row

-- | A row is a vector of 'Value's.
data Row = Row
    { types  :: !([ColumnType])
    , values :: !(Vector Value)
    } deriving (Eq, Show)

-- | Convert a row element.
fromRow :: Cql a => Int -> Row -> Either String a
fromRow i r =
    case values r !? i of
        Nothing -> Left "out of bounds access"
        Just  v -> fromCql v

mkRow :: [(Value, ColumnType)] -> Row
mkRow xs = let (v, t) = unzip xs in Row t (Vec.fromList v)

rowLength :: Row -> Int
rowLength r = Vec.length (values r)

columnTypes :: Row -> [ColumnType]
columnTypes = types

------------------------------------------------------------------------------
-- Tuples

-- Database.CQL.Protocol.Tuple does not export 'PrivateTuple' but only
-- 'Tuple' effectively turning 'Tuple' into a closed type-class.
class PrivateTuple a where
    count :: Tagged a Int
    check :: Tagged a ([ColumnType] -> [ColumnType])
    tuple :: Version -> [ColumnType] -> Get a
    store :: Version -> Putter a

class PrivateTuple a => Tuple a

------------------------------------------------------------------------------
-- Manual instances

instance PrivateTuple () where
    count     = Tagged 0
    check     = Tagged $ const []
    tuple _ _ = return ()
    store _   = const $ return ()

instance Tuple ()

instance Cql a => PrivateTuple (Identity a) where
    count     = Tagged 1
    check     = Tagged $ typecheck [untag (ctype :: Tagged a ColumnType)]
    tuple v _ = Identity <$> element v ctype
    store v (Identity a) = do
        put (1 :: Word16)
        putValue v (toCql a)

instance Cql a => Tuple (Identity a)

instance PrivateTuple Row where
    count     = Tagged (-1)
    check     = Tagged $ const []
    tuple v t = Row t . Vec.fromList <$> mapM (getValue v) t
    store v r = do
        put (fromIntegral (rowLength r) :: Word16)
        Vec.mapM_ (putValue v) (values r)

instance Tuple Row

------------------------------------------------------------------------------
-- Templated instances

genInstances :: Int -> Q [Dec]
genInstances n = join <$> mapM tupleInstance [2 .. n]

tupleInstance :: Int -> Q [Dec]
tupleInstance n = do
    let cql = mkName "Cql"
    vnames <- replicateM n (newName "a")
    let vtypes    = map VarT vnames
    let tupleType = foldl1 ($:) (TupleT n : vtypes)
#if MIN_VERSION_template_haskell(2,10,0)
    let ctx = map (AppT (ConT cql)) vtypes
#else
    let ctx = map (\t -> ClassP cql [t]) vtypes
#endif
    td <- tupleDecl n
    sd <- storeDecl n
    return
        [ InstanceD ctx (tcon "PrivateTuple" $: tupleType)
            [ FunD (mkName "count") [countDecl n]
            , FunD (mkName "check") [checkDecl vnames]
            , FunD (mkName "tuple") [td]
            , FunD (mkName "store") [sd]
            ]
        , InstanceD ctx (tcon "Tuple" $: tupleType) []
        ]

countDecl :: Int -> Clause
countDecl n = Clause [] (NormalB body) []
  where
    body = con "Tagged" $$ litInt n

-- check = Tagged $
--     typecheck [ untag (ctype :: Tagged x ColumnType)
--               , untag (ctype :: Tagged y ColumnType)
--               , ...
--               ])
checkDecl :: [Name] -> Clause
checkDecl names = Clause [] (NormalB body) []
  where
    body  = con "Tagged" $$ (var "typecheck" $$ ListE (map fn names))
    fn n  = var "untag" $$ SigE (var "ctype") (tty n)
    tty n = tcon "Tagged" $: VarT n $: tcon "ColumnType"

-- tuple v = (,)  <$> element v ctype <*> element v ctype
-- tuple v = (,,) <$> element v ctype <*> element v ctype <*> element v ctype
-- ...
tupleDecl :: Int -> Q Clause
tupleDecl n = do
    let v = mkName "v"
    Clause [VarP v, WildP] (NormalB $ body v) <$> comb
  where
    body v = UInfixE (var "combine") (var "<$>") (foldl1 star (elts v))
    elts v = replicate n (var "element" $$ VarE v $$ var "ctype")
    star   = flip UInfixE (var "<*>")
    comb   = do
        names <- replicateM n (newName "x")
        let f = NormalB $ TupE (map VarE names)
        return [ FunD (mkName "combine") [Clause (map VarP names) f []] ]

-- store v (a, b) = put (2 :: Word16) >> putValue v (toCql a) >> putValue v (toCql b)
storeDecl :: Int -> Q Clause
storeDecl n = do
    let v = mkName "v"
    names <- replicateM n (newName "k")
    return $ Clause [VarP v, TupP (map VarP names)] (NormalB $ body v names) []
  where
    body x names = DoE (NoBindS size : map (NoBindS . value x) names)
    size         = var "put" $$ SigE (litInt n) (tcon "Word16")
    value x v    = var "putValue" $$ VarE x $$ (var "toCql" $$ VarE v)

------------------------------------------------------------------------------
-- Helpers

litInt :: Integral i => i -> Exp
litInt = LitE . IntegerL . fromIntegral

var, con :: String -> Exp
var = VarE . mkName
con = ConE . mkName

tcon :: String -> Type
tcon = ConT . mkName

($$) :: Exp -> Exp -> Exp
($$) = AppE

($:) :: Type -> Type -> Type
($:) = AppT

------------------------------------------------------------------------------
-- Implementation helpers

element :: Cql a => Version -> Tagged a ColumnType -> Get a
element v t = getValue v (untag t) >>= either fail return . fromCql

typecheck :: [ColumnType] -> [ColumnType] -> [ColumnType]
typecheck rr cc = if and (zipWith (===) rr cc) then [] else rr
  where
    (MaybeColumn a) === b               = a === b
    (ListColumn  a) === (ListColumn  b) = a === b
    (SetColumn   a) === (SetColumn   b) = a === b
    (MapColumn a b) === (MapColumn c d) = a === c && b === d
    TextColumn      === VarCharColumn   = True
    VarCharColumn   === TextColumn      = True
    a               === b               = a == b
