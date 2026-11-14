@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo Проверяю/устанавливаю библиотеки kazoo, boto3, psutil...
python -m pip install --quiet kazoo boto3 psutil
echo.
python auto_runner.py
echo.
pause