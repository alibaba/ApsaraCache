# opget and opapply test

proc get_aof_count {aof_index_path} {
    return [exec wc -l < $aof_index_path]
}

set server_path [tmpdir "server.aof-opget-test"]
start_server {tags {"repl"}} {
    start_server [list overrides [list "dir" $server_path]] {
        r config set appendonly yes
        r -1 slaveof [srv 0 host] [srv 0 port]
        wait_for_condition 50 100 {
            [string match {*connected_slaves:1*} [r info replication]]
        } else {
            fail "Can't turn the instance into a slave"
        }

        test {opget - opget with right content, use multi opget client} {
            r select 1
            r set a b
            r select 2
            r set c d
            set data [exec tail -n 1 "${server_path}/aof-inc.index"]
            set aof_path "${server_path}/${data}"
            # wait for slave to report its opid
            after 1000
            set result_aof_origin [exec src/redis-print-aof $aof_path]
            set opget_res1 [lindex [lindex [r opget 1 count 1] 1] 0]
            set opget_res2 [lindex [lindex [r opget 2 count 1] 1] 0]
            # write oplog with opget command into a tmp file
            set fp_tmp [open "${server_path}/tmp_opget_results_file" a]
            # binary write
            fconfigure $fp_tmp -translation binary
            puts -nonewline $fp_tmp $opget_res1
            puts -nonewline $fp_tmp $opget_res2
            close $fp_tmp
            # print tmp oplog file, check received oplog format by the way
            set result_aof_tmp [exec src/redis-print-aof "${server_path}/tmp_opget_results_file"]
            # compare
            assert {$result_aof_origin eq $result_aof_tmp}
        }
    }
}

set server_path [tmpdir "server.aof-opget-test2"]
start_server {tags {"repl"}} {
    start_server [list overrides [list "dir" $server_path]] {
        r config set appendonly yes
        r -1 config set appendonly yes
        r slaveof [srv -1 host] [srv -1 port]
        wait_for_condition 50 100 {
            [s role] eq {slave} &&
            [string match {*master_link_status:up*} [r info replication]]
        } else {
            fail "Can't turn the instance into a slave"
        }

        test {opget - check opget [count] option} {
            r -1 select 4
            for {set j 0} {$j < 100} {incr j} {
                r -1 set "xdj_key_$j" xdj_value
            }
            # wait for master replication
            after 1000
            set res [r opget 2 count 3]
            # get number of oplog
            set count [llength [lindex $res 1]]
            assert_equal 3 $count
        }

        test {opget - no need to set count option again after set once} {
            set res [r opget 5]
            set count [llength [lindex $res 1]]
            assert_equal 3 $count
        }

        test {opget - change value of count option only once} {
            set res [r opget 8 count 50]
            set count [llength [lindex $res 1]]
            assert_equal 50 $count
            set res [r opget 58]
            set count [llength [lindex $res 1]]
            assert_equal 43 $count
        }
    }
}

set server_path [tmpdir "server.aof-opget-test-matchdb"]
start_server {tags {"repl"}} {
    start_server [list overrides [list "dir" $server_path]] {
        r config set appendonly yes
        r -1 config set appendonly yes
        r slaveof [srv -1 host] [srv -1 port]
        wait_for_condition 50 100 {
            [s role] eq {slave} &&
            [string match {*master_link_status:up*} [r info replication]]
        } else {
            fail "Can't turn the instance into a slave"
        }

        test {opget - check opget filter [MATCHDB] option} {
            r -1 select 4
            r -1 set key4 val4
            r -1 select 5
            r -1 set key5 val5
            r -1 select 9
            r -1 set key9 val9
            # make sure that opget client can read the just write oplog
            after 400
            set res [r opget 1 matchdb 4 count 2]
            set count [llength [lindex $res 1]]
            assert_equal 1 $count
        }

        test {opget - multi opget [MATCHDB] option} {
            r -1 select 11
            r -1 set key11 va11
            r -1 select 4
            r -1 set key4 val4
            # make sure that opget client can read the just write oplog
            after 400
            set res [r opget 3 matchdb 9 matchdb 11 count 100]
            set count [llength [lindex $res 1]]
            assert_equal 2 $count
        }

        test {opget - opget [MATCHDB] option is -1 which will matches all the db} {
            r -1 select 11
            r -1 set key11 va11
            r -1 select 4
            r -1 set key4 val4
            after 400
            set res [r opget 6 matchdb 12 matchdb 11 matchdb -1 count 100]
            set count [llength [lindex $res 1]]
            assert_equal 2 $count
        }
    }
}

set server_path [tmpdir "server.aof-opget-test-matchkey"]
start_server {tags {"repl"}} {
    start_server [list overrides [list "dir" $server_path]] {
        r config set appendonly yes
        r -1 config set appendonly yes
        r slaveof [srv -1 host] [srv -1 port]
        wait_for_condition 50 100 {
            [s role] eq {slave} &&
            [string match {*master_link_status:up*} [r info replication]]
        } else {
            fail "Can't turn the instance into a slave"
        }

        test {opget - check opget filter [MATCHKEY] option} {
            r -1  set aa aa
            r -1  set ab ab
            r -1  set bb bb
            # wait for data replication
            after 400
            set res [r opget 1 matchkey "a*" count 2]
            set count [llength [lindex $res 1]]
            assert_equal 2 $count
        }

        test {opget - multi opget [MATCHKEY] option} {
            r -1 set gf gf
            r -1 set c12f cf
            r -1 set ace ace
            after 400
            set res [r opget 3 matchkey "b*" matchkey "c*" count 100]
            set count [llength [lindex $res 1]]
            assert_equal 2 $count
        }

        test {opget - opget [MATCHDB] option is -1 which will matches all the db} {
            r -1 set key11 va11
            r -1 set key4 val4
            r -1 set bfe bfe
            after 400
            set res [r opget 7 matchkey "b*" matchkey "c+" matchkey "*" count 100]
            set count [llength [lindex $res 1]]
            assert_equal 3 $count
        }
    }
}

set server_path [tmpdir "server.aof-opget-test-multifilter"]
start_server {tags {"repl"}} {
    start_server [list overrides [list "dir" $server_path]] {
        r config set appendonly yes
        r -1 config set appendonly yes
        r slaveof [srv -1 host] [srv -1 port]
        wait_for_condition 50 100 {
            [s role] eq {slave} &&
            [string match {*master_link_status:up*} [r info replication]]
        } else {
            fail "Can't turn the instance into a slave"
        }

        test {opget - multi filter [MATCHKEY] and [MATCHDB] option} {
            r -1 select 1
            r -1 set aa aa
            r -1 select 2
            r -1 set ab ab
            r -1 select 2
            r -1 set bb bb
            r -1 select 1
            r -1 set ab ab
            after 400
            set res [r opget 1 matchkey "ab*" matchdb 2]
            set count [llength [lindex $res 1]]
            assert_equal 1 $count
        }
    }
}

set server_path [tmpdir "server.aof-opget-test-opdel"]
start_server [list overrides [list "dir" $server_path]] {
    r config set appendonly yes

    test {opdel - aof delete based on multi opdel source} {
        r select 6
        for {set i 0} {$i < 4} {incr i} {
            for {set j 0} {$j < 100} {incr j} {
                r set xdj_key xdj_value
            }
            # to generate deferent aof
            after 1000
            r aofflush
        }
        # write a key in last aof
        r set xdj_key xdj_value
        r bgsave
        waitForBgsave r
        r opdel "source0" 80
        # create deferent expire time of opdel source
        after 3000
        r opdel "source1" 230
        set aof_index_path "${server_path}/aof-inc.index"
        # save aof count before delete
        set aof_count_before [get_aof_count $aof_index_path]
        # get last aof
        set last_aof [exec tail -n 1 $aof_index_path]
        # del to the last aof, but based on opdel source, nothing will be deleted
        r purgeaofto $last_aof
        # get aof count after
        set aof_count_after [get_aof_count $aof_index_path]
        assert_equal $aof_count_before $aof_count_after
    }

    test {opdel - aof delete after multi opdel source expired} {
        r config set opdel-source-timeout 2
        # wait for opdel source 0 to expire
        after 2100
        set aof_index_path "${server_path}/aof-inc.index"
        set last_aof [exec tail -n 1 $aof_index_path]
        r purgeaofto $last_aof
        set aof_count [get_aof_count $aof_index_path]
        assert_equal 3 $aof_count
    }
}

set server_path [tmpdir "server.aof-opget-test-opapply-src"]
start_server [list overrides [list "dir" $server_path "server-id" 863]] {
    r config set appendonly yes
    r config set opget-master-min-slaves 0

    set server_path [tmpdir "server.aof-opget-test-opapply-dest"]
    start_server [list overrides [list "dir" $server_path "server-id" 985]] {
        r config set appendonly yes
        # setex is a special command which would generate two commands in aof
        test {opapply - apply setex command and check correctness} {
            r -1 setex xdj_key 100 xdj_val
            set res [lindex [lindex [r -1 opget 1 count 1] 1] 0]
            set opapply_cmd "*1\r\n\$7\r\nopapply\r\n"
            # write oplog with opget command into a tmp file
            set fp_tmp [open "${server_path}/tmp_opget_results_file" a]
            # binary write
            fconfigure $fp_tmp -translation binary
            puts -nonewline $fp_tmp $opapply_cmd
            puts -nonewline $fp_tmp $res
            close $fp_tmp
            # apply oplog
            exec cat "${server_path}/tmp_opget_results_file" | src/redis-cli -p [srv port] --pipe
            # to make sure the key is applied
            after 500
            set ttl_of_applied_key [lindex [r ttl xdj_key] 0]
            assert {$ttl_of_applied_key <= 100 && $ttl_of_applied_key >= 1}
        }

        test {opapply - check opapply information in [info oplog] command} {
            set res [lindex [r info oplog] 5]
            # find oplog source information in info oplog
            set needlestr "server_id=863,applied_opid=1"
            set substr [lindex [split $res ":"] 1]
            assert_equal $substr $needlestr
        }
    }
}

set server_path [tmpdir "server.aof-opRestore-test"]
start_server [list overrides [list "dir" $server_path "server-id" 863]] {
    r config set appendonly yes
    r config set opget-master-min-slaves 0

    test {opRestore - set key} {
        r set foo value
        r opget 1 count 1
    } {*foo*}

    test {opRestore - reproduce key} {
        r opRestore foo
        r opget 2 count 1
    } {*RESTORE*foo*REPLACE*}
}
