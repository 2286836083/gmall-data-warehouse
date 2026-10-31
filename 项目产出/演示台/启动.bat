@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>nul
if not errorlevel 1 (
  py -3 serve.py --open
  goto end
)

where python >nul 2>nul
if not errorlevel 1 (
  python serve.py --open
  goto end
)

echo.
echo [ERROR] Python 3 not found in PATH.
echo Install Python 3, or run: python serve.py --open
echo.
pause

:end
