@echo off
echo Starting VC++ Runtime Installation at %date% %time% > %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
echo ===================================== >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1

REM Log environment information
echo Environment Information: >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
echo AZ_BATCH_TASK_DIR: %AZ_BATCH_TASK_DIR% >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
echo AZ_BATCH_NODE_STARTUP_DIR: %AZ_BATCH_NODE_STARTUP_DIR% >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
echo Current Directory: %CD% >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
echo ===================================== >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1

REM Check if VC++ 2015-2022 runtime is already installed
echo Checking for existing VC++ runtime... >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
reg query "HKLM\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64" /v Version >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
if %errorlevel% equ 0 (
    echo VC++ Runtime already installed, skipping installation >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
    goto :end
)

REM Download VC++ Redistributable
echo Downloading VC++ Redistributable... >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
powershell -Command "& {[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://aka.ms/vs/17/release/vc_redist.x64.exe' -OutFile '%AZ_BATCH_TASK_DIR%\vc_redist.x64.exe'}" >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1

if not exist "%AZ_BATCH_TASK_DIR%\vc_redist.x64.exe" (
    echo ERROR: Failed to download VC++ Redistributable >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
    exit /b 1
)

echo Download completed successfully >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
dir "%AZ_BATCH_TASK_DIR%\vc_redist.x64.exe" >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1

REM Install VC++ Redistributable
echo Installing VC++ Redistributable... >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
"%AZ_BATCH_TASK_DIR%\vc_redist.x64.exe" /install /quiet /norestart >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
set INSTALL_RESULT=%errorlevel%
echo Installation completed with exit code: %INSTALL_RESULT% >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1

REM Verify installation
echo Verifying installation... >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
reg query "HKLM\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64" /v Version >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
if %errorlevel% equ 0 (
    echo VC++ Runtime installed successfully >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
) else (
    echo WARNING: Could not verify VC++ Runtime installation >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
)

:end
echo ===================================== >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1
echo Start task completed at %date% %time% >> %AZ_BATCH_TASK_DIR%\starttask.log 2>&1

REM Copy log to stdout and stderr for Azure Batch to capture
type %AZ_BATCH_TASK_DIR%\starttask.log
type %AZ_BATCH_TASK_DIR%\starttask.log >&2

exit /b 0 