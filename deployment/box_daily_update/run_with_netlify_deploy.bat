@echo off
REM Dashboard generation with Netlify deployment
REM This version updates the online dashboard at https://box-dashboard-report.netlify.app/

cd /d "%~dp0"

echo ============================================ >> netlify_deploy.log
echo Execution started at %date% %time% >> netlify_deploy.log
echo ============================================ >> netlify_deploy.log
echo. >> netlify_deploy.log

REM Set environment variables to skip Box API data collection but enable Netlify deploy
set SKIP_DATA_COLLECTION=1
set SKIP_NETLIFY_DEPLOY=0

"%~dp0box_daily_update.exe" >> netlify_deploy.log 2>&1

set EXIT_CODE=%ERRORLEVEL%

echo. >> netlify_deploy.log
echo ============================================ >> netlify_deploy.log
echo Execution ended at %date% %time% >> netlify_deploy.log
echo Exit code: %EXIT_CODE% >> netlify_deploy.log
echo ============================================ >> netlify_deploy.log
echo. >> netlify_deploy.log

exit /b %EXIT_CODE%
