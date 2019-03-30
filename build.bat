@echo off
set WORKING_DIR=%~dp0
setx CLASSPATH "%WORKING_DIR%;%WORKING_DIR%\classes;%WORKING_DIR%\lib\CUP;%WORKING_DIR%\lib\JLEX;"
setx COMPONENT "%WORKING_DIR%"
echo "Query environment setup successfully"

javac  -d %COMPONENT%\classes %COMPONENT%\src\qp\utils\*.java
javac  -d %COMPONENT%\classes %COMPONENT%\src\qp\parser\*.java
javac -d %COMPONENT%\classes %COMPONENT%\src\qp\operators\*.java
javac -d %COMPONENT%\classes %COMPONENT%\src\qp\optimizer\*.java
javac -d %COMPONENT%\classes %COMPONENT%\testcases\*.java
javac -d %COMPONENT%\classes %COMPONENT%\src\QueryMain.java
