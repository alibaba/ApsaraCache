start_server {
    tags {"aof bio"}
    overrides { appendonly {yes} appendfsync {bio}}} {
    test "SAVE AOF BY bio write" {
        assert_equal {bio} [lindex [r config get appendfsync] 1]
        r del myintset myhashset mylargeintset
        for {set i 0} {$i <  100} {incr i} { r sadd myintset $i }
        for {set i 0} {$i < 1280} {incr i} { r sadd mylargeintset $i }
        for {set i 0} {$i <  256} {incr i} { r sadd myhashset [format "i%03d" $i] }
        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset

        r debug loaddisk

        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset

    }

    foreach d {string int} {
        foreach e {ziplist linkedlist} {
            test "AOF load of list with $e encoding, $d data" {
                r flushall
                if {$e eq {ziplist}} {set len 10} else {set len 1000}
                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    r lpush key $data
                }
                assert_equal [r object encoding key] $e
                set d1 [r debug digest]
                r debug loaddisk
                set d2 [r debug digest]
                if {$d1 ne $d2} {
                    error "assertion:$d1 is not equal to $d2"
                }
            }
        }
    }

    foreach d {string int} {
        foreach e {intset hashtable} {
            test "AOF load of set with $e encoding, $d data" {
                r flushall
                if {$e eq {intset}} {set len 10} else {set len 1000}
                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    r sadd key $data
                }
                if {$d ne {string}} {
                    assert_equal [r object encoding key] $e
                }
                set d1 [r debug digest]
                r debug loaddisk
                set d2 [r debug digest]
                if {$d1 ne $d2} {
                    error "assertion:$d1 is not equal to $d2"
                }
            }
        }
    }

    foreach d {string int} {
        foreach e {ziplist hashtable} {
            test "AOF load of hash with $e encoding, $d data" {
                r flushall
                if {$e eq {ziplist}} {set len 10} else {set len 1000}
                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    r hset key $data $data
                }
                assert_equal [r object encoding key] $e
                set d1 [r debug digest]
                r debug loaddisk
                set d2 [r debug digest]
                if {$d1 ne $d2} {
                    error "assertion:$d1 is not equal to $d2"
                }
            }
        }
    }

    foreach d {string int} {
        foreach e {ziplist skiplist} {
            test "AOF load of zset with $e encoding, $d data" {
                r flushall
                if {$e eq {ziplist}} {set len 10} else {set len 1000}
                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    r zadd key [expr rand()] $data
                }
                assert_equal [r object encoding key] $e
                set d1 [r debug digest]
                r debug loaddisk
                set d2 [r debug digest]
                if {$d1 ne $d2} {
                    error "assertion:$d1 is not equal to $d2"
                }
            }
        }
    }

}

start_server {
    tags {"switch aof sync mode"}
    overrides { appendonly {yes} appendfsync {bio}}} {
    test "switch bio to everysec" {
        assert_equal {bio} [lindex [r config get appendfsync] 1]
        r config set aof-buf-queue-max-size 524288
        assert_equal {268435456} [lindex [r config get aof-buf-queue-max-size] 1]

        r config set aof-buf-queue-max-size 536870912
        assert_equal {536870912} [lindex [r config get aof-buf-queue-max-size] 1]
        r flushall
        for {set i 0} {$i <  100} {incr i} { r sadd myintset $i }
        after 1000
        for {set i 0} {$i < 1280} {incr i} { r sadd mylargeintset $i }
        r config set appendfsync everysec
        for {set i 0} {$i <  256} {incr i} { r sadd myhashset [format "i%03d" $i] }
        r set hello world
        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset

        r debug loaddisk
        assert_equal {everysec} [lindex [r config get appendfsync] 1]

        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset
        assert {[r get hello] eq "world"}
    }

    test "switch everysec to bio" {
        r config set appendfsync everysec
        assert_equal {everysec} [lindex [r config get appendfsync] 1]
        r flushall

        for {set i 0} {$i <  100} {incr i} { r sadd myintset $i }
        after 1000
        for {set i 0} {$i < 1280} {incr i} { r sadd mylargeintset $i }
        r config set appendfsync bio
        for {set i 0} {$i <  256} {incr i} { r sadd myhashset [format "i%03d" $i] }
        r set hello world
        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset

        r debug loaddisk
        assert_equal {bio} [lindex [r config get appendfsync] 1]

        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset
        assert {[r get hello] eq "world"}
     }
}

start_server {
    tags {"switch aof sync mode"}
    overrides { appendonly {yes} appendfsync {everysec}}} {
    test "switch everysec to bio" {
        assert_equal {everysec} [lindex [r config get appendfsync] 1]
        r config set aof-buf-queue-max-size 524288
        assert_equal {268435456} [lindex [r config get aof-buf-queue-max-size] 1]

        r config set aof-buf-queue-max-size 536870912
        assert_equal {536870912} [lindex [r config get aof-buf-queue-max-size] 1]
        r flushall
        for {set i 0} {$i <  100} {incr i} { r sadd myintset $i }
        after 1000
        for {set i 0} {$i < 1280} {incr i} { r sadd mylargeintset $i }
        r config set appendfsync bio
        for {set i 0} {$i <  256} {incr i} { r sadd myhashset [format "i%03d" $i] }
        r set hello world
        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset

        r debug loaddisk
        assert_equal {bio} [lindex [r config get appendfsync] 1]

        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset
        assert {[r get hello] eq "world"}
    }

    test "switch bio to everysec" {
        r config set appendfsync bio
        assert_equal {bio} [lindex [r config get appendfsync] 1]
        r flushall

        for {set i 0} {$i <  100} {incr i} { r sadd myintset $i }
        after 1000
        for {set i 0} {$i < 1280} {incr i} { r sadd mylargeintset $i }
        r config set appendfsync everysec
        for {set i 0} {$i <  256} {incr i} { r sadd myhashset [format "i%03d" $i] }
        r set hello world
        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset

        r debug loaddisk
        assert_equal {everysec} [lindex [r config get appendfsync] 1]

        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset
        assert {[r get hello] eq "world"}
     }
}

