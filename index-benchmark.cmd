@echo off

SET LOG=benchmark-log.txt

echo %DATE% - %TIME% - start >> %LOG%


:start
es-index-couchdb.py benchmark.ini
wait 3
echo %DATE% - %TIME% - restart >> %LOG%
goto start


