-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module Database.CQL.Protocol.Tuple.TH where

import Control.Applicative
import Control.Monad
import Data.Functor.Identity
import Data.Serialize
import Data.Singletons.TypeLits (Nat)
import Data.Word
import Database.CQL.Protocol.Class
import Database.CQL.Protocol.Codec (Codec (..))
import Database.CQL.Protocol.Types
import Language.Haskell.TH

-- Database.CQL.Protocol.Tuple does not export 'PrivateTuple' but only
-- 'Tuple' effectively turning 'Tuple' into a closed type-class.
class PrivateTuple (v :: Nat) a where
    count :: Tagged v a Int
    check :: Tagged v a ([ColumnType] -> [ColumnType])
    tuple :: Codec v -> Get a
    store :: Codec v -> Putter a

class PrivateTuple v a => Tuple v a

------------------------------------------------------------------------------
-- Manual instances

instance PrivateTuple v () where
    count     = Tagged 0
    check     = Tagged $ const []
    tuple _   = return ()
    store _ _ = return ()

instance Tuple v ()

instance Cql v a => PrivateTuple v (Identity a) where
    count   = Tagged 1
    check   = Tagged $ typecheck [untag (ctype :: Tagged v a ColumnType)]
    tuple c = Identity <$> element c ctype
    store (Codec {..}) (Identity a) = do
        put (1 :: Word16)
        encodeValue (toCql a)

instance Cql v a => Tuple v (Identity a)

------------------------------------------------------------------------------
-- Templated instances

genInstances :: Int -> Q [Dec]
genInstances n = join <$> mapM tupleInstance [2 .. n]

tupleInstance :: Int -> Q [Dec]
tupleInstance n = do
    let version = VarT (mkName "v")
    let cql     = mkName "Cql"
    vnames <- replicateM n (newName "a")
    let vtypes    = map VarT vnames
    let tupleType = foldl1 ($:) (TupleT n : vtypes)
    let ctx       = map (\t -> ClassP cql [version, t]) vtypes
    td <- tupleDecl n
    sd <- storeDecl n
    return
        [ InstanceD ctx (tcon "PrivateTuple" $: version $: tupleType)
            [ FunD (mkName "count") [countDecl n]
            , FunD (mkName "check") [checkDecl version vnames]
            , FunD (mkName "tuple") [td]
            , FunD (mkName "store") [sd]
            ]
        , InstanceD ctx (tcon "Tuple" $: version $: tupleType) []
        ]

countDecl :: Int -> Clause
countDecl n = Clause [] (NormalB body) []
  where
    body = con "Tagged" $$ litInt n

checkDecl :: Type -> [Name] -> Clause
checkDecl version names = Clause [] (NormalB body) []
  where
    body  = con "Tagged" $$ (var "typecheck" $$ ListE (map fn names))
    fn n  = var "untag" $$ SigE (var "ctype") (tty n)
    tty n = tcon "Tagged" $: version $: VarT n $: tcon "ColumnType"

tupleDecl :: Int -> Q Clause
tupleDecl n = do
    let codec = mkName "codec"
    Clause [VarP codec] (NormalB $ body codec) <$> comb
  where
    body c = UInfixE (var "combine") (var "<$>") (foldl1 star (elts c))
    elts c = replicate n (var "element" $$ VarE c $$ var "ctype")
    star   = flip UInfixE (var "<*>")
    comb   = do
        names <- replicateM n (newName "x")
        let f = NormalB $ TupE (map VarE names)
        return [ FunD (mkName "combine") [Clause (map VarP names) f []] ]

storeDecl :: Int -> Q Clause
storeDecl n = do
    let codec = mkName "codec"
    names <- replicateM n (newName "k")
    return $ Clause [VarP codec, TupP (map VarP names)] (NormalB $ body codec names) []
  where
    body c names = DoE (NoBindS size : map (NoBindS . value c) names)
    size         = var "put" $$ SigE (litInt n) (tcon "Word16")
    value c v    = var "encodeValue" $$ VarE c $$ (var "toCql" $$ VarE v)

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

element :: Cql v a => Codec v -> Tagged v a ColumnType -> Get a
element c t = decodeValue c (untag t) >>= either fail return . fromCql

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

