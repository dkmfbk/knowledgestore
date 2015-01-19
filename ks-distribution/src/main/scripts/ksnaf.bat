@echo off

rem check that the O.S. is NT-based
if "%OS%"=="Windows_NT" goto nt_ok
echo This script only works with NT-based versions of Windows.
goto end
:nt_ok

rem restart the script (with variable expansion) to reliably change directory
if "%DELAYED_VAR_EXPANSION%"=="true" goto var_ok
setlocal
set DELAYED_VAR_EXPANSION=true
cmd /V:ON /C %0 %*
endlocal
goto end
:var_ok

rem switch to base directory
cd %~dp0\..

rem build the classpath
set CLASSPATH=.;etc
for %%f in (lib\*.jar) do set CLASSPATH=!CLASSPATH!;%%f

rem retrieve the path of the java executable
set JAVA=java.exe
if "%JAVA_HOME%" == "" goto java_ok
if not exist "%JAVA_HOME%\bin\java.exe" goto java_ok
set _JAVA=%JAVA_HOME%\bin\java.exe
:java_ok

rem  Execute the command.
"%JAVA%" %JAVA_OPTS% -classpath "%CLASSPATH%" -Dfile.encoding=UTF-8 eu.fbk.knowledgestore.populator.naf.nafPopulator %*

:end