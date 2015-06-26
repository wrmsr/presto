#!/bin/sh
find /Library/Java/JavaVirtualMachines/ -maxdepth 1 -type d -not -path /Library/Java/JavaVirtualMachines/
find /usr/lib/jvm/ -maxdepth 1 -type d -not -path /usr/lib/jvm/
find ~ -maxdepth 1 -type d -not -path ~

if [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    for include in /usr/share/elasticsearch/elasticsearch.in.sh \
    JAVA=`which java`
fi


potential_jvms

