#!/bin/sh

if [ "$#" -lt 2 ]; then
  echo "Illegal number of parameters (must be 2: configuration file and working dir)"
  exit 1
fi

if [ ${KS_ROOT_DIR:-unset} == unset ]
then
  export KS_ROOT_DIR=$(cd $(dirname "$0")/../../../ ; /bin/pwd)
fi

# set default JVM options if not supplied externally
if [ -z "$JAVA_OPTS" ]; then
  JAVA_OPTS=-Xmx1024m
fi

# resolve program name in case it is a symbolic link
PRG="$0"
while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# retrieve directories
BINDIR=`dirname "$PRG"`
BASEDIR=`cd "$BINDIR/.." >/dev/null; pwd`
ETCDIR=$BASEDIR/etc
LIBDIR=$BASEDIR/lib

# build classpath
CLASSPATH=$BASEDIR:$ETCDIR:$2
for JAR in `ls $LIBDIR/*.jar` ; do
        CLASSPATH=$CLASSPATH:$JAR;
done

# retrieve path of java executable
JAVA="java"
if [ -n "$JAVA_HOME"  ] ; then
        if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
                JAVA="$JAVA_HOME/jre/sh/java"
    elif [ -x "$JAVA_HOME/bin/java" ] ; then
                JAVA="$JAVA_HOME/bin/java"
        fi
fi

# execute the program in the base directory
cd $BASEDIR
exec "$JAVA" $JAVA_OPTS  \
  -classpath "$CLASSPATH" \
  -Dfile.encoding=UTF-8 \
  -Dlauncher.executable="ksd" \
  -Dlauncher.description="Runs the KnowledgeStore daemon, using the daemon and logging configurations optionally specified" \
  -Dlauncher.config="$1" \
  eu.fbk.knowledgestore.runtime.Launcher \
  "${@:3}"
