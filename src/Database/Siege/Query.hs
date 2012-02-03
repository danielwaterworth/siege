module Database.Siege.Query where

import Data.Char
import Data.List

data SExpr =
  Atom String |
  List [SExpr]

parse :: String -> SExpr
parse = undefined

generate :: SExpr -> String
generate (List exprs) =
  "(" ++ (intercalate " " $ map generate exprs) ++ ")"
generate (Atom var) =
  if any isSpace var || elem ')' var then
    undefined
  else
    var

-- main = print $ generate $ List [Atom "lambda", List [Atom "a"], Atom "a"]

data PreConditions =
  Type |
  HashExists String Bool |
  HashLookup String PreConditions |
  SetExists String Bool |
  ListLookup Int PreConditions |
  ListEmpty |
  SequenceAt Int Preconditions |
  SequenceSize (Int -> Bool) |
  Branch [PreConditions]

data Path =
  HashLookup String Path |
  SequenceLookup Int Path

data WriteOperation =
  Set Path Ref |
  Del Path |
  SetInsert Path String |
  SetRemove Path String |
  DropList Path Int

data ReadOperation =
  Get Path |
  Exists Path |
  SetExists Path String |
  Size Path

data Query =
  Get [ReadOperation] |
  Alter PreConditions [WriteOperation]

--SExpr -> Ref -> (Ref, SExpr)
