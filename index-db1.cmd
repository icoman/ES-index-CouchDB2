@echo off
:start
python es-index-couchdb.py db1.ini
pause
goto start

