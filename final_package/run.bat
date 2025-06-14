@echo OFF
set SCRIPT_DIR=%~dp0
set PYTHON_EXE=%SCRIPT_DIR%python.exe
set SCRIPT_TO_RUN=%SCRIPT_DIR%batchactivity_json2csv.py

echo "========================================="
echo "        Azure Batch Job Runner"
echo "========================================="
echo "Script Directory: %SCRIPT_DIR%"
echo "Python Executable: %PYTHON_EXE%"
echo "Running Script: %SCRIPT_TO_RUN%"
echo "Arguments: %*"
echo.

REM Set the path to include our embedded python and packages
set PATH=%SCRIPT_DIR%;%SCRIPT_DIR%packages;%PATH%

echo "Updated PATH: %PATH%"
echo.
echo "Starting Python script..."
echo "-----------------------------------------"

REM Execute the python script
"%PYTHON_EXE%" "%SCRIPT_TO_RUN%" %*

REM Capture exit code
set SCRIPT_EXIT_CODE=%ERRORLEVEL%
echo "-----------------------------------------"
echo "Python script finished with exit code: %SCRIPT_EXIT_CODE%"

exit /b %SCRIPT_EXIT_CODE% 