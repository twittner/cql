-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQL.Protocol.Tuple.TH where

import Control.Applicative
import Control.Monad
import Data.Serialize
import Data.Tagged
import Data.Word
import Database.CQL.Protocol.Class
import Database.CQL.Protocol.Codec
import Database.CQL.Protocol.Types
import Language.Haskell.TH

-- Database.CQL.Protocol.Tuple does not export 'PrivateTuple' but only
-- 'Tuple' effectively turning 'Tuple' into a closed type-class.
class PrivateTuple a where
    count :: Tagged a Int
    check :: Tagged a ([ColumnType] -> [ColumnType])
    tuple :: Get a
    store :: Putter a

class PrivateTuple a => Tuple a

------------------------------------------------------------------------------
-- Manual instances

instance PrivateTuple () where
    count = Tagged 0
    check = Tagged $ const []
    tuple = return ()
    store = const $ return ()

instance Tuple ()

newtype Some a = Some a
    deriving (Eq, Show)

instance (Cql a) => PrivateTuple (Some a) where
    count = Tagged 1
    check = Tagged $ typecheck [untag (ctype :: Tagged a ColumnType)]
    tuple = Some <$> element ctype
    store (Some a) = do
        put (1 :: Word16)
        putValue (toCql a)

instance (Cql a) => Tuple (Some a)

------------------------------------------------------------------------------
-- Templated instances

genInstances :: Int -> Q [Dec]
genInstances n = join <$> mapM tupleInstance [2 .. n]

tupleInstance :: Int -> Q [Dec]
tupleInstance n = do
    vnames <- replicateM n (newName "a")
    let vtypes    = map VarT vnames
    let tupleType = foldl1 ($:) (TupleT n : vtypes)
    let ctx       = map (ClassP (mkName "Cql") . (:[])) vtypes
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

checkDecl :: [Name] -> Clause
checkDecl names = Clause [] (NormalB body) []
  where
    body  = con "Tagged" $$ (var "typecheck" $$ ListE (map fn names))
    fn n  = var "untag" $$ SigE (var "ctype") (tty n)
    tty n = tcon "Tagged" $: VarT n $: tcon "ColumnType"

tupleDecl :: Int -> Q Clause
tupleDecl n = Clause [] (NormalB body) <$> comb
  where
    body = UInfixE (var "combine") (var "<$>") (foldl1 star elts)
    elts = replicate n (var "element" $$ var "ctype")
    star = flip UInfixE (var "<*>")
    comb = do
        names <- replicateM n (newName "x")
        let f = NormalB $ TupE (map VarE names)
        return [ FunD (mkName "combine") [Clause (map VarP names) f []] ]

storeDecl :: Int -> Q Clause
storeDecl n = do
    names <- replicateM n (newName "k")
    return $ Clause [TupP (map VarP names)] (NormalB $ body names) []
  where
    body names = DoE (NoBindS size : map (NoBindS . value) names)
    size       = var "put" $$ SigE (litInt n) (tcon "Word16")
    value v    = var "putValue" $$ (var "toCql" $$ VarE v)

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

element :: (Cql a) => Tagged a ColumnType -> Get a
element t = fromCql <$> getValue (untag t)

typecheck :: [ColumnType] -> [ColumnType] -> [ColumnType]
typecheck rr cc = if and (zipWith (===) rr cc) then [] else rr
  where
    (MaybeColumn a) === b               = a === b
    (ListColumn  a) === (ListColumn  b) = a === b
    (SetColumn   a) === (SetColumn   b) = a === b
    (MapColumn a b) === (MapColumn c d) = a === c && b === d
    a               === b               = a == b

