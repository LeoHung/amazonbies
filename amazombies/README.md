# amazombies frontend server #

Team: Amazombies

Authors: San-Chuan Hung, Jiajun Wang, Chi Chang

## Packages

Please install "memcached" first.

    sudo apt-get install memcached

Besides, please download "mysql-connector-java-5.1.18-bin.jar" into lib/ directory.

## Build & Run ##

### launch memcached

    memcached

### start server

    $ cd amazombies
    $ ./sbt
    > container:start
    > browse


If `browse` doesn't launch your browser, manually open [http://localhost:8080/](http://localhost:8080/) in your browser.
