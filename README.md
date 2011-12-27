The quickest way to get up and running is to do:

    ./Setup.lhs configure # (you may need to add the --user flag)
    ./Setup.lhs build
    ./dist/build/siege-disk/siege-disk # for the disk based backend, or
    ./dist/build/siege-memory/siege-memory # for the memory based backend

Then you can interact with it using the redis command line tool:

    redis-cli -p 4050

