@echo off

REM Check if running with administrator privileges
NET SESSION >nul 2>&1
if %errorLevel% == 0 (
    REM Already running with administrator privileges
    goto run_script
)

REM Relaunch the script with administrator privileges
echo Running with administrator privileges...
powershell -Command "Start-Process -Verb RunAs '%~0'"

REM Exit the current instance of the script
exit

:run_script
REM Install Chocolatey (if not already installed)
if not exist %SystemRoot%\System32\choco.exe (
    echo Installing Chocolatey...
    @powershell -NoProfile -ExecutionPolicy Bypass -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))"
    echo Chocolatey installation complete.
) else (
    echo Chocolatey is already installed.
)

REM Install Python
choco install -y python

REM Install Git
choco install -y git

REM Install Docker
choco install -y docker-desktop

REM Install Google Cloud SDK
choco install -y gcloudsdk

REM Other tools and dependencies...

echo Development environment setup complete!
pause
