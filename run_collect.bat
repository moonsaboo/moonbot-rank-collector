@echo off
chcp 65001 > nul
cd /d "%~dp0"
"C:\Users\thati\AppData\Local\Programs\Python\Python314\python.exe" collect.py >> collect_log.txt 2>&1
