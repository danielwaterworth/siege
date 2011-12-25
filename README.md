The quickest way to get up and running is to do:

    cabal install binary hex SHA enumerator
    ghc --make disk.hs
    ./disk

Then you can interact with it using the redis command line tool:

    redis-cli -p 4050

