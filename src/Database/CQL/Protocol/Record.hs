-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE TypeFamilies #-}

module Database.CQL.Protocol.Record
    ( Record    (..)
    , TupleType
    , recordInstance
    ) where

import Control.Monad
import Language.Haskell.TH

type family TupleType a

class Record a where
    asTuple  :: a -> TupleType a
    asRecord :: TupleType a -> a

recordInstance :: Name -> Q [Dec]
recordInstance n = do
    i <- reify n
    case i of
        TyConI d -> start d
        _        -> fail "expecting record type"

start :: Dec -> Q [Dec]
start (DataD _ tname _ cons _) = do
    unless (length cons == 1) $
        fail "expecting single data constructor"
    tt <- tupleType (head cons)
    at <- asTupleDecl (head cons)
    ar <- asRecrdDecl (head cons)
    return
        [ TySynInstD (mkName "TupleType") [ConT tname] tt
        , InstanceD [] (ConT (mkName "Record") $: ConT tname)
            [ FunD (mkName "asTuple")  [at]
            , FunD (mkName "asRecord") [ar]
            ]
        ]
start _ = fail "unsupported data type"

tupleType :: Con -> Q Type
tupleType c = do
    let tt = types c
    return $ foldl1 ($:) (TupleT (length tt) : types c)
  where
    types (NormalC _ tt) = map snd tt
    types (RecC _ tt)    = map (\(_, _, t) -> t) tt
    types _              = fail "record and normal constructors only"

asTupleDecl ::Con -> Q Clause
asTupleDecl c =
    case c of
        (NormalC n t) -> go n t
        (RecC    n t) -> go n t
        _             -> fail "record and normal constructors only"
  where
    go n t = do
        vars <- replicateM (length t) (newName "a")
        return $ Clause [ConP n (map VarP vars)] (body vars) []
    body = NormalB . TupE . map VarE

asRecrdDecl ::Con -> Q Clause
asRecrdDecl c =
    case c of
        (NormalC n t) -> go n t
        (RecC    n t) -> go n t
        _             -> fail "record and normal constructors only"
  where
    go n t = do
        vars <- replicateM (length t) (newName "a")
        return $ Clause [TupP (map VarP vars)] (body n vars) []
    body n v = NormalB $ foldl1 ($$) (ConE n : map VarE v)

($$) :: Exp -> Exp -> Exp
($$) = AppE

($:) :: Type -> Type -> Type
($:) = AppT

