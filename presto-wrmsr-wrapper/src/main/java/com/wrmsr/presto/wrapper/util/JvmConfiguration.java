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

http://www.oracle.com/technetwork/java/javase/tech/vmoptions-jsp-140102.html
http://www.oracle.com/technetwork/java/javase/tech/exactoptions-jsp-141536.html

ES_CLASSPATH=$ES_CLASSPATH:$ES_HOME/lib/elasticsearch-1.0.1.jar:$ES_HOME/lib/*:$ES_HOME/lib/sigar/*

# min and max heap sizes should be set to the same value to avoid
# stop-the-world GC pauses during resize, and so that we can lock the
# heap in memory on startup to prevent any of it from being swapped
# out.
-XX:+PrintClassHistogram
-XX:+PrintGCApplicationStoppedTime
-Xloggc:/var/log/elasticsearch/gc.log

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
fi

# The path to the heap dump location, note directory must exists and have enough
# space for a full heap dump.
#JAVA_OPTS="$JAVA_OPTS -XX:HeapDumpPath=$ES_HOME/logs/heapdump.hprof"bash-3.2$

    -Xmixed           mixed mode execution (default)
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

#  -XX:+PrintGCCause \
  -Djava.security.egd=file:/dev/./urandom \
 -XX:+UseG1GC -XX:MaxGCPauseMillis=250 \

RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
List<String> arguments = runtimeMxBean.getInputArguments();

http://hg.openjdk.java.net/jdk8/jdk8/hotspot/file/tip/src/share/vm/runtime/arguments.cpp
http://hg.openjdk.java.net/jdk8/jdk8/hotspot/file/tip/src/share/vm/runtime/arguments.hpp

-Djava.awt.headless=true
ulimits

-XX:+UseConcMarkSweepGC
-XX:+ExplicitGCInvokesConcurrent

hotspot/src/share/vm/runtime/globals.hpp
*/

package com.wrmsr.presto.wrapper.util;

import io.airlift.units.DataSize;

import java.util.function.Supplier;

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

    public static abstract class Item<T>
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

        public T getValue()
        {
            throw new UnsupportedOperationException();
        }

        public abstract class Value implements Supplier<T>
        {
            public abstract String toString();
        }

        public abstract Value valueOf(T value);
    }

    public static class ValuelessItem extends Item<Void>
    {
        public ValuelessItem(Prefix prefix, String name)
        {
            super(prefix, name, Separator.NONE);
        }

        public class Value extends Item.Value
        {
            @Override
            public String toString()
            {
                return getPrefix() + getName();
            }

            @Override
            public Void get()
            {
                return null;
            }
        }

        public Value valueOf()
        {
            return valueOf(null);
        }

        @Override
        public Value valueOf(Void value)
        {
            return new Value();
        }
    }

    public static class StringItem extends Item<String>
    {
        public StringItem(Prefix prefix, String name, Separator separator)
        {
            super(prefix, name, separator);
        }

        public class Value extends Item.Value
        {
            private final String value;

            public Value(String value)
            {
                this.value = value;
            }

            @Override
            public String toString()
            {
                return getPrefix() + getName() + getSeparator() + value;
            }

            @Override
            public String get()
            {
                return value;
            }
        }

        @Override
        public Value valueOf(String value)
        {
            return new Value(value);
        }
    }

    public static class DataSizeItem extends Item<DataSize>
    {
        public DataSizeItem(Prefix prefix, String name, Separator separator)
        {
            super(prefix, name, separator);
        }

        public class Value extends Item.Value
        {
            private final DataSize value;

            public Value(DataSize value)
            {
                this.value = value;
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

            @Override
            public DataSize get()
            {
                return value;
            }
        }

        @Override
        public Value valueOf(DataSize value)
        {
            return new Value(value);
        }
    }

    public static class ToggleItem extends Item<Boolean>
    {
        public ToggleItem(Prefix prefix, String name)
        {
            super(prefix, name, Separator.NONE);
        }

        public class Value extends Item.Value
        {
            private final boolean value;

            public Value(boolean value)
            {
                this.value = value;
            }

            @Override
            public String toString()
            {
                return getPrefix() + (value ? "+" : "-") + getName();
            }

            @Override
            public Boolean get()
            {
                return value;
            }
        }

        @Override
        public Value valueOf(Boolean value)
        {
            return new Value(value);
        }
    }

    public static final DataSizeItem MIN_HEAP_SIZE  = new DataSizeItem(Prefix.NONSTANDARD, "ms", Separator.NONE);
    public static final DataSizeItem MAX_HEAP_SIZE  = new DataSizeItem(Prefix.NONSTANDARD, "mx", Separator.NONE);
    public static final DataSizeItem YOUNG_GENERATION_SIZE  = new DataSizeItem(Prefix.NONSTANDARD, "mn", Separator.NONE);
    public static final DataSizeItem THREAD_STACK_SIZE  = new DataSizeItem(Prefix.NONSTANDARD, "ss", Separator.NONE);
    public static final DataSizeItem MAX_DIRECT_MEMORY_SIZE  = new DataSizeItem(Prefix.UNSTABLE, "MaxDirectMemorySize", Separator.EQUALS);
    public static final ToggleItem PRINT_GC_DATE_STAMPS = new ToggleItem(Prefix.UNSTABLE, "PrintGCDateStamps");

    /*
    public static final class PrintGcDetails extends ToggleItem
    {
        public PrintGcDetails(boolean value)
        {
            super(Prefix.UNSTABLE, "PrintGCDetails", value);
        }
    }

    public static final class PrintTenuringDistribution extends ToggleItem
    {
        public PrintTenuringDistribution(boolean value)
        {
            super(Prefix.UNSTABLE, "PrintTenuringDistribution", value);
        }
    }

    public static final class PrintJniGcStalls extends ToggleItem
    {
        public PrintJniGcStalls(boolean value)
        {
            super(Prefix.UNSTABLE, "PrintJNIGCStalls", value);
        }
    }

    public static final class UseNuma extends ToggleItem
    {
        public UseNuma(boolean value)
        {
            super(Prefix.UNSTABLE, "UseNUMA", value);
        }
    }

    public static final class HeapDumpOnOutOfMemoryError extends ToggleItem
    {
        public HeapDumpOnOutOfMemoryError(boolean value)
        {
            super(Prefix.UNSTABLE, "HeapDumpOnOutOfMemoryError", value);
        }
    }

    public static final class HeapDumpPath extends StringItem
    {
        public HeapDumpPath( String value)
        {
            super(Prefix.UNSTABLE, "HeapDumpPath", Separator.EQUALS, value);
        }
    }

    public static final class Server extends ValuelessItem
    {
        public Server()
        {
            super(Prefix.NONE, "server");
        }
    }

    public static final class Interpreted extends ValuelessItem
    {
        public Interpreted()
        {
            super(Prefix.NONSTANDARD, "int");
        }
    }

    public static final class OnOutOfMemoryError extends StringItem
    {
        public OnOutOfMemoryError(String value)
        {
            super(Prefix.UNSTABLE, "OnOutOfMemoryError", Separator.EQUALS, value);
        }
    }

    public static final class PrintFlags extends ToggleItem
    {
        public PrintFlags(boolean value)
        {
            super(Prefix.UNSTABLE, "PrintFlagsFinal", value);
        }
    }

    public static final class UnlockDiagnostics extends ToggleItem
    {
        public UnlockDiagnostics(boolean value)
        {
            super(Prefix.UNSTABLE, "UnlockDiagnosticVMOptions", value);
        }
    }

    public static final class AggressiveOpts extends ToggleItem
    {
        public AggressiveOpts(boolean value)
        {
            super(Prefix.UNSTABLE, "AggressiveOpts", value);
        }
    }

    public static final class EliminateLocks extends ToggleItem
    {
        public EliminateLocks(boolean value)
        {
            super(Prefix.UNSTABLE, "EliminateLocks", value);
        }
    }

    public static final class UseLargePages extends ToggleItem
    {
        public UseLargePages(boolean value)
        {
            super(Prefix.UNSTABLE, "UseLargePages", value);
        }
    }

    public static final class Debug extends ValuelessItem
    {
        public Debug()
        {
            super(Prefix.NONSTANDARD, "debug");
        }
    }

    public static final class PrintInlining extends ToggleItem
    {
        public PrintInlining(boolean value)
        {
            super(Prefix.UNSTABLE, "PrintInlining", value);
        }
    }

    public static final class PrintCompilation extends ToggleItem
    {
        public PrintCompilation(boolean value)
        {
            super(Prefix.UNSTABLE, "PrintCompilation", value);
        }
    }

    public static final class UseCompressedOops extends ToggleItem
    {
        public UseCompressedOops(boolean value)
        {
            super(Prefix.UNSTABLE, "UseCompressedOops", value);
        }
    }

    public static final class RemoteDebug extends StringItem
    {
        private final int port;
        private final boolean suspend;

        public RemoteDebug(int port, boolean suspend)
        {
            super(Prefix.NONSTANDARD, "runjdwp", Separator.COLON, String.format("transport=dt_socket,address=%d,server=y,suspend=%s", port, suspend ? "y" : "n"));
            this.port = port;
            this.suspend = suspend;
        }

        public int getPort()
        {
            return port;
        }

        public boolean isSuspend()
        {
            return suspend;
        }
    }

    public static abstract class Group implements Iterable<Item>
    {
    }

    public static abstract class GC extends Group
    {
    }

    public static abstract class CMSConfiguration extends GC
    {
    }
    */

    public static void main(String[] args) throws Throwable
    {
        /*
        System.out.println(new MaxHeapSize(DataSize.valueOf("100MB")));
        System.out.println(new RemoteDebug(41414, false));
        System.out.println(new AggressiveOpts(true));
        */
        System.out.println(MAX_HEAP_SIZE.valueOf(DataSize.valueOf("100MB")));
        System.out.println(PRINT_GC_DATE_STAMPS.valueOf(true));
    }
}
