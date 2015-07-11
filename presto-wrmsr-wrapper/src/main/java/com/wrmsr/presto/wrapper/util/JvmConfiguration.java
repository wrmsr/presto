/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
add hotspot specificness
add version awareness (even though j8 only)
add os awareness
add task awareness
gc configs
properties
heap size

-Djava.awt.headless=true

parse java -XX:+PrintFlagsFinal -version

http://www.oracle.com/technetwork/java/javase/tech/vmoptions-jsp-140102.html
http://www.oracle.com/technetwork/java/javase/tech/exactoptions-jsp-141536.html

ES_CLASSPATH=$ES_CLASSPATH:$ES_HOME/lib/elasticsearch-1.0.1.jar:$ES_HOME/lib/*:$ES_HOME/lib/sigar/*

if [ "x$ES_MIN_MEM" = "x" ]; then
    ES_MIN_MEM=256m
fi
if [ "x$ES_MAX_MEM" = "x" ]; then
    ES_MAX_MEM=1g
fi
if [ "x$ES_HEAP_SIZE" != "x" ]; then
    ES_MIN_MEM=$ES_HEAP_SIZE
    ES_MAX_MEM=$ES_HEAP_SIZE
fi

# min and max heap sizes should be set to the same value to avoid
# stop-the-world GC pauses during resize, and so that we can lock the
# heap in memory on startup to prevent any of it from being swapped
# out.
JAVA_OPTS="$JAVA_OPTS -Xms${ES_MIN_MEM}"
JAVA_OPTS="$JAVA_OPTS -Xmx${ES_MAX_MEM}"

# new generation
if [ "x$ES_HEAP_NEWSIZE" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -Xmn${ES_HEAP_NEWSIZE}"
fi

# max direct memory
if [ "x$ES_DIRECT_SIZE" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -XX:MaxDirectMemorySize=${ES_DIRECT_SIZE}"
fi

# reduce the per-thread stack size
JAVA_OPTS="$JAVA_OPTS -Xss256k"

# set to headless, just in case
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true"

# Force the JVM to use IPv4 stack
if [ "x$ES_USE_IPV4" != "x" ]; then
  JAVA_OPTS="$JAVA_OPTS -Djava.net.preferIPv4Stack=true"
fi

JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC"
JAVA_OPTS="$JAVA_OPTS -XX:+UseConcMarkSweepGC"

JAVA_OPTS="$JAVA_OPTS -XX:CMSInitiatingOccupancyFraction=75"
JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSInitiatingOccupancyOnly"

# GC logging options
if [ "x$ES_USE_GC_LOGGING" != "x" ]; then
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCTimeStamps"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintClassHistogram"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintTenuringDistribution"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCApplicationStoppedTime"
  JAVA_OPTS="$JAVA_OPTS -Xloggc:/var/log/elasticsearch/gc.log"
fi

# Causes the JVM to dump its heap on OutOfMemory.
JAVA_OPTS="$JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError"
# The path to the heap dump location, note directory must exists and have enough
# space for a full heap dump.
#JAVA_OPTS="$JAVA_OPTS -XX:HeapDumpPath=$ES_HOME/logs/heapdump.hprof"bash-3.2$

    -Xmixed           mixed mode execution (default)
    -Xint             interpreted mode execution only
    -Xbootclasspath:<directories and zip/jar files separated by ;>
                      set search path for bootstrap classes and resources
    -Xbootclasspath/a:<directories and zip/jar files separated by ;>
                      append to end of bootstrap class path
    -Xbootclasspath/p:<directories and zip/jar files separated by ;>
                      prepend in front of bootstrap class path
    -Xnoclassgc       disable class garbage collection
    -Xincgc           enable incremental garbage collection
    -Xloggc:<file>    log GC status to a file with time stamps
    -Xbatch           disable background compilation
    -Xms<size>        set initial Java heap size
    -Xmx<size>        set maximum Java heap size
    -Xss<size>        set java thread stack size
    -Xprof            output cpu profiling data
    -Xfuture          enable strictest checks, anticipating future default
    -Xcheck:jni       perform additional checks for JNI functions
    -Xshare:off	      do not attempt to use shared class data
    -Xshare:auto      use shared class data if possible (default)
    -Xshare:on	      require using shared class data, otherwise fail.

The -X options are non-standard and subject to change without notice.

java -help

-Dcom.sun.management.jmxremote
-Dcom.sun.management.jmxremote.port=9010
-Dcom.sun.management.jmxremote.local.only=false
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false

#JAVA_HOME=/usr/lib/jvm/java-7-oracle \
#  /usr/lib/jvm/java-7-oracle/bin/java \
#  -Xms8192m -Xmx8192m \
#  -XX:+PrintGCCause \
#  -Xms24576m -Xmx24576m \
# JAVA_HOME=/nail/home/wtimoney/jdk1.8.0
#   /nail/home/wtimoney/jdk1.8.0/bin/java \
  -server \
  -XX:+UnlockDiagnosticVMOptions -XX:+PrintFlagsFinal \
  -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintTenuringDistribution \
  -Xms8192m -Xmx8192m \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:+UseCompressedOops \
  -Djava.security.egd=file:/dev/./urandom \
  -XX:+AggressiveOpts \
  -XX:+EliminateLocks -XX:+UseLargePages \
  -Xdebug -Xrunjdwp:transport=dt_socket,address=48192,server=y,suspend=n \
  -XX:+UseNUMA \
#  -XX:+UseG1GC -XX:MaxGCPauseMillis=250 \

RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
List<String> arguments = runtimeMxBean.getInputArguments();

http://hg.openjdk.java.net/jdk8/jdk8/hotspot/file/tip/src/share/vm/runtime/arguments.cpp
http://hg.openjdk.java.net/jdk8/jdk8/hotspot/file/tip/src/share/vm/runtime/arguments.hpp

    public stat

-Djava.awt.headless=true
ulimits

-server
-Xmx16G
-XX:+UseConcMarkSweepGC
-XX:+ExplicitGCInvokesConcurrent
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p

hotspot/src/share/vm/runtime/globals.hpp
*/

package com.wrmsr.presto.wrapper.util;

import io.airlift.units.DataSize;

public class JvmConfiguration
{
    public enum Prefix
    {
        NONE(""),
        DASH("-"),
        NONSTANDARD("-X"),
        UNSTABLE("-XX:"),
        PROPERTY("-D")
        ;

        private final String value;

        Prefix(String value)
        {
            this.value = value;
        }

        @Override
        public String toString()
        {
            return value;
        }
    }

    public enum Separator
    {
        NONE(""),
        COLON(":"),
        EQUALS("=")
        ;

        private final String value;

        Separator(String value)
        {
            this.value = value;
        }

        @Override
        public String toString()
        {
            return value;
        }
    }

    public static abstract class Item
    {
        private final Prefix prefix;
        private final String name;
        private final Separator separator;

        public Item(Prefix prefix, String name, Separator separator)
        {
            this.prefix = prefix;
            this.name = name;
            this.separator = separator;
        }

        public Prefix getPrefix()
        {
            return prefix;
        }

        public String getName()
        {
            return name;
        }

        public Separator getSeparator()
        {
            return separator;
        }

        public abstract Object getValue();

        public abstract String toString();
    }

    public static class StringItem extends Item
    {
        private final String value;

        public StringItem(Prefix prefix, String name, Separator seperator, String value)
        {
            super(prefix, name, seperator);
            this.value = value;
        }

        @Override
        public String getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return getPrefix() + getName() + getSeparator() + value;
        }
    }

    public static class DataSizeItem extends Item
    {
        private final DataSize value;

        public DataSizeItem(Prefix prefix, String name, Separator seperator, DataSize value)
        {
            super(prefix, name, seperator);
            this.value = value;
        }

        @Override
        public DataSize getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            final String stringValue;
            if (Double.toString(value.getValue()).endsWith(".0")) {  // lol
                long longValue = (long) value.getValue();
                switch (value.getUnit()) {
                    case TERABYTE:
                        stringValue = Long.toString(longValue) + "T";
                        break;
                    case GIGABYTE:
                        stringValue = Long.toString(longValue) + "G";
                        break;
                    case MEGABYTE:
                        stringValue = Long.toString(longValue) + "M";
                        break;
                    case KILOBYTE:
                        stringValue = Long.toString(longValue) + "K";
                        break;
                    default:
                        stringValue = Long.toString(value.toBytes());
                }
            }
            else {
                stringValue = Long.toString(value.toBytes());
            }

            return getPrefix() + getName() + getSeparator() + stringValue;
        }
    }

    public static class ToggleItem extends Item
    {
        private final boolean value;

        public ToggleItem(Prefix prefix, String name, boolean value)
        {
            super(prefix, name, Separator.NONE);
            this.value = value;
        }

        @Override
        public Boolean getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return getPrefix() + (value ? "+" : "-") + getName();
        }
    }

    public static class MinHeapSize extends DataSizeItem
    {
        public MinHeapSize(DataSize value)
        {
            super(Prefix.NONSTANDARD, "ms", Separator.NONE, value);
        }
    }

    public static class MaxHeapSize extends DataSizeItem
    {
        public MaxHeapSize(DataSize value)
        {
            super(Prefix.NONSTANDARD, "mx", Separator.NONE, value);
        }
    }

    public static abstract class Group implements Iterable<Item>
    {
    }

    public static abstract class Debug extends Group
    {

    }

    public static abstract class GC extends Group
    {
    }

    public static abstract class CMSConfiguration extends GC
    {
    }

    public static void main(String[] args) throws Throwable
    {
        System.out.println(new MaxHeapSize(DataSize.valueOf("100MB")));

    }
}
