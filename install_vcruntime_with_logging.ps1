# Azure Batch Start Task - Install VC++ Runtime with Logging
$ErrorActionPreference = "Stop"
$logFile = "$env:AZ_BATCH_TASK_DIR\starttask.log"

function Write-Log {
    param($Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "$timestamp - $Message"
    Add-Content -Path $logFile -Value $logMessage
    Write-Host $logMessage
    Write-Error $logMessage
}

try {
    Write-Log "Starting VC++ Runtime Installation"
    Write-Log "====================================="
    
    # Log environment information
    Write-Log "Environment Information:"
    Write-Log "AZ_BATCH_TASK_DIR: $env:AZ_BATCH_TASK_DIR"
    Write-Log "AZ_BATCH_NODE_STARTUP_DIR: $env:AZ_BATCH_NODE_STARTUP_DIR"
    Write-Log "Current Directory: $(Get-Location)"
    Write-Log "====================================="
    
    # Check if VC++ runtime is already installed
    Write-Log "Checking for existing VC++ runtime..."
    $vcInstalled = $false
    try {
        $vcKey = Get-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64" -Name Version -ErrorAction SilentlyContinue
        if ($vcKey) {
            Write-Log "VC++ Runtime already installed (Version: $($vcKey.Version)), skipping installation"
            $vcInstalled = $true
        }
    } catch {
        Write-Log "VC++ Runtime not found, proceeding with installation"
    }
    
    if (-not $vcInstalled) {
        # Download VC++ Redistributable
        Write-Log "Downloading VC++ Redistributable..."
        $vcRedistUrl = "https://aka.ms/vs/17/release/vc_redist.x64.exe"
        $vcRedistPath = "$env:AZ_BATCH_TASK_DIR\vc_redist.x64.exe"
        
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri $vcRedistUrl -OutFile $vcRedistPath -UseBasicParsing
        
        if (Test-Path $vcRedistPath) {
            $fileInfo = Get-Item $vcRedistPath
            Write-Log "Download completed successfully (Size: $($fileInfo.Length) bytes)"
        } else {
            throw "Failed to download VC++ Redistributable"
        }
        
        # Install VC++ Redistributable
        Write-Log "Installing VC++ Redistributable..."
        $process = Start-Process -FilePath $vcRedistPath -ArgumentList "/install", "/quiet", "/norestart" -Wait -PassThru
        Write-Log "Installation completed with exit code: $($process.ExitCode)"
        
        # Verify installation
        Write-Log "Verifying installation..."
        try {
            $vcKey = Get-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64" -Name Version
            Write-Log "VC++ Runtime installed successfully (Version: $($vcKey.Version))"
        } catch {
            Write-Log "WARNING: Could not verify VC++ Runtime installation"
        }
    }
    
    Write-Log "====================================="
    Write-Log "Start task completed successfully"
    
} catch {
    Write-Log "ERROR: $($_.Exception.Message)"
    Write-Log "Stack Trace: $($_.Exception.StackTrace)"
    exit 1
}

# Ensure log is written to stdout/stderr
Get-Content $logFile
exit 0 