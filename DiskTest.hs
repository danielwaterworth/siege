import Disk
import DBOperation
import Disk
import Control.Monad.Trans.Error
import StringHelper
import System.IO

main = do
  withFile "./test.db" ReadWriteMode (\hnd -> do
    Right head <- withHandle hnd $ runErrorT $ convert $ do
      v <- createValue $ stToB "world"
      mapInsert Nothing (stToB "hello") v
    print head
    Right v <- withHandle hnd $ runErrorT $ convert $ do
      v <- mapLookup head (stToB "hello")
      getValue v
    print v)
