module Control.Pipe where

data Step i o m a =
  Done a |
  Stop |
  AwaitInput (Maybe i -> Pipe i o m a) |
  ReplaceInput i (Pipe i o m a) |
  AwaitOutput o (Bool -> Pipe i o m a)

data Pipe i o m a = Pipe {
  runPipe :: m (Step i o m a)
}

instance (Monad m) => Monad (Pipe i o m) where
  return = Pipe . return . Done
  m >>= f = Pipe $ do
    v <- runPipe m
    case v of
      Done x -> (runPipe . f) x
      Stop -> return Stop
      AwaitInput c -> return $ AwaitInput (\i -> c i >>= f)
      ReplaceInput i c -> return $ ReplaceInput i (c >>= f)
      AwaitOutput o c -> return $ AwaitOutput o (\i -> c i >>= f)

stop :: Monad m => Pipe i o m a
stop = Pipe $ return Stop

awaitInput :: Monad m => Pipe i o m (Maybe i)
awaitInput = Pipe $ return $ AwaitInput return

awaitOutput :: Monad m => o -> Pipe i o m Bool
awaitOutput o = Pipe $ return $ AwaitOutput o return

-- TODO: handle ReplaceInput
($>) :: Monad m => Pipe a b m x -> Pipe b c m x -> Pipe a c m x
a $> b = Pipe $ do
  a' <- runPipe a
  case a' of
    Done x ->
      runPipe $ return x
    Stop -> do
      b' <- runPipe b
      case b' of
        Done x ->
          runPipe $ return x
        Stop ->
          runPipe stop
        AwaitInput c ->
          runPipe (stop $> (c Nothing))
        AwaitOutput o c ->
          return $ AwaitOutput o (\i -> stop $> (c i))
    AwaitInput c ->
      return $ AwaitInput (\i -> (c i) $> b)
    AwaitOutput o c -> do
      b' <- runPipe b
      case b' of
        Done x ->
          runPipe $ return x
        Stop ->
          runPipe ((c False) $> stop)
        AwaitInput c' ->
          runPipe ((c True) $> (c' $ Just o))
        AwaitOutput o' c' ->
          return $ AwaitOutput o' (\i -> (Pipe $ return a') $> (c' i))

runPipeline :: Monad m => Pipe () () m a -> m (Maybe a)
runPipeline p = do
  p' <- runPipe p
  case p' of
    Done x -> return $ Just x
    Stop -> return Nothing
    AwaitInput c ->
      runPipeline $ c $ Just ()
    AwaitOutput _ c ->
      runPipeline $ c True
