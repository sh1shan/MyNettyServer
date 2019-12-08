#!/bin/bash

#-Xmx 设置JVM最大可用内存
#-Xms 设置JVM促使内存为3550m。此值可以设置与-Xmx相同，以避免每次垃圾回收完成后JVM重新分配内存。
#-Xmn 设置年轻代大小为2G。整个堆大小=年轻代大小 + 老年代大小 + 持久代大小。持久代一般固定大小为64m，所以增大年轻代后，将会减小年老代大小，此值对系统性能影响较大，sun官方推荐配置为整个丢的3/8
#-Xss 设置每个线程的堆栈大小，JDK5.0以后每个线程堆栈大小为1m，以前每个线程堆栈大小为256k。更具应用的线程所需要内存大小进行调整。在相同的物理内存下，减小这个值能生成更多的线程，但是操作系统对一个进程内的线程数还是有限制的，不能无限生成。

Project_Home=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd | sed 's/\/sbin//')

echo "work directory: $Project_HOME"

LOG_DIR=$Project_HOME/logs

CURDIR=$(dirname $0)

cd $CURDIR

logDir(){
    if [ ! -d "$LOG_DIR" ];then
        mkdir "$LOG_DIR"
    fi
}

logDir

nohup java \
-server \
-Xmx2048m \
-Xms2048m \
-XX:+PrintGCDetails \
-XX:+PrintGCTimeStamps \
-Xloggc:${LOG_DIR}/gc.log \
-classpath ../conf:../lib/* \
com.dragon.netty.start.NettyServerStart test> /dev/null 2>&1 &