#!/bin/sh
APP_DIR_HOME=`pwd`/.
export APP_DIR_LIB=$APP_DIR_HOME/lib
for file in `ls $APP_DIR_LIB`
do
	if [ -e $APP_DIR_LIB/$file ] 
	then
		export APP_LIB=$APP_LIB:$APP_DIR_LIB/$file
	fi
done

export APP_CP=$APP_DIR_HOME/classes$APP_LIB

nohup java -Dfile.encoding=GBK -classpath $APP_CP com.esoft.dp.DPMain ./conf/context.xml > /dev/null &
