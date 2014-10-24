# amazombies #

team: Amazombies

authors: San-Chuan Hung, Jiajung Wang, Chi Chang

## Packages

Please install "memcached" first. 

    sudo apt-get install memcached


## Build & Run ##

### launch memcached

    memcached   

### start server

    $ cd amazombies
    $ ./sbt
    > container:start
    > browse


If `browse` doesn't launch your browser, manually open [http://localhost:8080/](http://localhost:8080/) in your browser.
