#!/bin/sh
ps -ef |grep com.esoft.dp.Main|grep -v grep|awk '{print $2}' |while read pid 
        do 
                kill -9 $pid 
        done 