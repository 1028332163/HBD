@ECHO OFF

REM --- 开启变量延迟
SETLOCAL ENABLEDELAYEDEXPANSION

REM --- 设置路径
SET APP_DIR_HOME=.
SET APP_DIR_LIB=%APP_DIR_HOME%\lib

REM --- 设置类路径
FOR /F %%f IN ('dir %APP_DIR_LIB% /B /A:-D') DO (
	SET APP_LIB=!APP_LIB!;%APP_DIR_LIB%\%%f
)
SET APP_CP=%APP_DIR_HOME%\classes%APP_LIB%
ECHO %APP_CP%

REM --- 运行
java -Dfile.encoding=GBK -classpath %APP_CP% com.esoft.dp.DPMain %APP_DIR_HOME%/conf/context.xml