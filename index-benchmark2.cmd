@echo off

SET LOG=benchmark2-log.txt

echo %DATE% - %TIME% - start >> %LOG%


:start
es-index-couchdb.py benchmark2.ini
wait 3
echo %DATE% - %TIME% - restart >> %LOG%
goto start


