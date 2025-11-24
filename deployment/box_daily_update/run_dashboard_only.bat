@echo off
REM Dashboard generation only (skip Box API data collection)
REM This is much faster - completes in seconds instead of minutes

cd /d "%~dp0"

echo ============================================ >> dashboard_only.log
echo Dashboard generation started at %date% %time% >> dashboard_only.log
echo ============================================ >> dashboard_only.log
echo. >> dashboard_only.log

REM Set environment variables to skip data collection and Netlify deploy
set SKIP_DATA_COLLECTION=1
set SKIP_NETLIFY_DEPLOY=1

"%~dp0box_daily_update.exe" >> dashboard_only.log 2>&1

set EXIT_CODE=%ERRORLEVEL%

echo. >> dashboard_only.log
echo ============================================ >> dashboard_only.log
echo Dashboard generation ended at %date% %time% >> dashboard_only.log
echo Exit code: %EXIT_CODE% >> dashboard_only.log
echo ============================================ >> dashboard_only.log
echo. >> dashboard_only.log

exit /b %EXIT_CODE%
