@echo off

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
choco install -y googlecloudsdk

REM Other tools and dependencies...

echo Development environment setup complete!
pause
