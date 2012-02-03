module Database.Siege.DBSequence where

import qualified Data.Enumerator as E
import Data.Word
import Database.Siege.DBNode

-- a Monoid tree using Sum

set :: r -> Word64 -> r -> RawDBOperation r m r
set = undefined

insertBefore :: r -> Word64 -> r -> RawDBOperation r m r
insertBefore = undefined

insertAfter :: r -> Word64 -> r -> RawDBOperation r m r
insertAfter = undefined

lpush :: r -> r -> RawDBOperation r m r
lpush = undefined

lpop :: r -> RawDBOperation r m (r, r)
lpop = undefined

rpush :: r -> r -> RawDBOperation r m r
rpush = undefined

rpop :: r -> RawDBOperation r m (r, r)
rpop = undefined

delete :: r -> Word64 -> RawDBOperation r m r
delete = undefined

lookup :: r -> Word64 -> RawDBOperation r m r
lookup = undefined

iterate :: r -> E.Enumerator (Word64, r) (RawDBOperation r m) a
iterate = undefined

length :: r -> RawDBOperation r m Word64
length = undefined
