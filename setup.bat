@echo off
SETLOCAL ENABLEDELAYEDEXPANSION

echo Starting setup script...

:: Check if virtual environment exists
if exist .venv (
    echo Virtual environment found. Activating...
) else (
    echo Virtual environment not found. Creating...
    python -m venv .venv
)

:: Ensure virtual environment was created successfully
if not exist .venv (
    echo Failed to create virtual environment.
    exit /b 1
)

:: Activate virtual environment
call .venv\Scripts\activate
if %ERRORLEVEL% NEQ 0 (
    echo Failed to activate virtual environment.
    exit /b 1
)

echo Virtual environment activated.

:: Update pip, setuptools, and wheel
echo Updating pip...
pip install --upgrade pip
:: Install required packages
echo Installing required packages...
pip install -r requirements.txt
echo Setup complete!
ENDLOCAL