@echo off
REM Batch wrapper for box_daily_update.exe to capture output logs
REM Working directory is set to the script's directory

cd /d "%~dp0"

echo ============================================ >> box_daily_update.log
echo Execution started at %date% %time% >> box_daily_update.log
echo ============================================ >> box_daily_update.log
echo. >> box_daily_update.log

"%~dp0box_daily_update.exe" >> box_daily_update.log 2>&1

set EXIT_CODE=%ERRORLEVEL%

echo. >> box_daily_update.log
echo ============================================ >> box_daily_update.log
echo Execution ended at %date% %time% >> box_daily_update.log
echo Exit code: %EXIT_CODE% >> box_daily_update.log
echo ============================================ >> box_daily_update.log
echo. >> box_daily_update.log

exit /b %EXIT_CODE%
