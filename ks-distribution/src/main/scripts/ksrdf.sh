#!/bin/sh

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
LIBDIR=$BASEDIR/lib

# build classpath
CLASSPATH=$BASEDIR
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

# execute the program
cd $BASEDIR
exec "$JAVA" $JAVA_OPTS  \
  -classpath "$CLASSPATH" \
  -Dfile.encoding=UTF-8 \
  eu.fbk.knowledgestore.populator.rdf.RDFPopulator \
  "$@"
