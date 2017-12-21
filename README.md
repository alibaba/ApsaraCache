What is ApsaraCache ?
--------------

ApsaraCache is based on the Redis official release 4.0 and has many features and performance enhancements. ApsaraCache has proven to be very stable and efficient in production environment.

There are many features in ApsaraCache, the following two are included in this release and the other features will be gradually released in the subsequent, so stay tuned.

* ApsaraCache supports two main protocols of Memcached: the classic ASCII, and the newer binary. You can use ApsaraCache just as Memcached, and no client code need to be modified. You can persist your data by using ApsaraCache in Memcached mode just like Redis.
* In short connection scenario, ApsaraCache makes 30% performance increase compared with the vanilla version.


Building ApsaraCache
--------------

It is as simple as:

    % make
    
Running ApsaraCache
-------------
In default, ApsaraCache run in Redis mode. If you want ApsaraCache to run in Memcached mode, just add option 

       protocol  memcache
       
to redis.conf.

To run ApsaraCache with the default configuration just type:

    % cd src
    % ./redis-server

If you want to provide your redis.conf, you have to run it using an additional
parameter (the path of the configuration file):

    % cd src
    % ./redis-server /path/to/redis.conf

It is possible to alter the ApsaraCache configuration by passing parameters directly
as options using the command line. Examples:

    % ./redis-server --port 9999 --slaveof 127.0.0.1 6379
    % ./redis-server /etc/redis/6379.conf --loglevel debug

All the options in redis.conf are also supported as options using the command
line, with exactly the same name.

Playing with ApsaraCache in Redis mode
------------------

You can use redis-cli to play with ApsaraCache. Start a redis-server instance,
then in another terminal try the following:

    % cd src
    % ./redis-cli
    redis> ping
    PONG
    redis> set foo bar
    OK
    redis> get foo
    "bar"
    redis> incr mycounter
    (integer) 1
    redis> incr mycounter
    (integer) 2
    redis>

You can find the list of all the available commands at http://redis.io/commands.

Playing with ApsaraCache in Memcached mode
------------------

You can use telnet to visit ApsaraCache(telnet use the classic ASCII protocol).

     % telnet your-host your-port(usually 11211)
     
       set key 10 3600 2
       ok
       STORED
       get key
       VALUE key 10 2
       ok
       END  



Enjoy!

Test point in time recovery(PITR)
------------------
* [Documentation](https://github.com/alibaba/ApsaraCache/wiki/Test-point-in-time-recovery(PITR))

Test log based replication(AOF PSYNC)
------------------
* [Documentation](https://github.com/alibaba/ApsaraCache/wiki/Test-log-based-replication(AOF-PSYNC))

Documentation
------------------
* [Documentation Home](https://github.com/alibaba/ApsaraCache/wiki/ApsaraCache-document)
* [Frequently Asked Questions](https://github.com/alibaba/ApsaraCache/wiki/frequently-ask-questions)

Contributing
------------------
See [ApsaraCache Contributing Guide](https://github.com/alibaba/ApsaraCache/wiki/CONTRIBUTING) for more information.
