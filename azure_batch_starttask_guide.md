# Azure Batch Start Task Configuration Guide

## Problem: No Output from Start Task

When Azure Batch Start Task doesn't produce output, it's usually due to one of these issues:

1. **Output not being redirected properly**
2. **Start Task failing before it can write logs**
3. **Incorrect file paths or permissions**
4. **Output files not being retained**

## Solution 1: Using the Batch Script (Recommended)

Upload `install_vcruntime_with_logging.bat` to your storage account and configure the Start Task:

```json
{
  "commandLine": "cmd /c install_vcruntime_with_logging.bat",
  "resourceFiles": [
    {
      "httpUrl": "https://yourstorage.blob.core.windows.net/scripts/install_vcruntime_with_logging.bat",
      "filePath": "install_vcruntime_with_logging.bat"
    }
  ],
  "userIdentity": {
    "autoUser": {
      "scope": "pool",
      "elevationLevel": "admin"
    }
  },
  "maxTaskRetryCount": 1,
  "waitForSuccess": true
}
```

## Solution 2: Using PowerShell Script

```json
{
  "commandLine": "powershell -ExecutionPolicy Bypass -File install_vcruntime_with_logging.ps1",
  "resourceFiles": [
    {
      "httpUrl": "https://yourstorage.blob.core.windows.net/scripts/install_vcruntime_with_logging.ps1",
      "filePath": "install_vcruntime_with_logging.ps1"
    }
  ],
  "userIdentity": {
    "autoUser": {
      "scope": "pool",
      "elevationLevel": "admin"
    }
  },
  "maxTaskRetryCount": 1,
  "waitForSuccess": true
}
```

## Solution 3: Inline Command with Logging

If you can't upload scripts, use an inline command:

```json
{
  "commandLine": "cmd /c \"echo Start Task Begin > %AZ_BATCH_TASK_DIR%\\starttask.log 2>&1 && powershell -Command \\\"[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://aka.ms/vs/17/release/vc_redist.x64.exe' -OutFile '%AZ_BATCH_TASK_DIR%\\vc_redist.x64.exe'\\\" >> %AZ_BATCH_TASK_DIR%\\starttask.log 2>&1 && %AZ_BATCH_TASK_DIR%\\vc_redist.x64.exe /install /quiet /norestart >> %AZ_BATCH_TASK_DIR%\\starttask.log 2>&1 && echo Start Task Complete >> %AZ_BATCH_TASK_DIR%\\starttask.log 2>&1 && type %AZ_BATCH_TASK_DIR%\\starttask.log\"",
  "userIdentity": {
    "autoUser": {
      "scope": "pool",
      "elevationLevel": "admin"
    }
  },
  "maxTaskRetryCount": 1,
  "waitForSuccess": true
}
```

## How to View Start Task Output

### 1. Azure Portal
- Navigate to your Batch account
- Go to Pools → Select your pool
- Click on Nodes → Select a node
- Click on "Start task" → View stdout.txt and stderr.txt

### 2. Azure Batch Explorer
- Download from: https://azure.github.io/BatchExplorer/
- Connect to your Batch account
- Navigate to Pools → Your Pool → Nodes
- Right-click on a node → View Start Task Output

### 3. Using Azure CLI
```bash
# List nodes in pool
az batch node list --pool-id YOUR_POOL_ID

# Get start task stdout
az batch node file download \
  --pool-id YOUR_POOL_ID \
  --node-id NODE_ID \
  --file-path startup/stdout.txt \
  --destination ./stdout.txt

# Get start task stderr  
az batch node file download \
  --pool-id YOUR_POOL_ID \
  --node-id NODE_ID \
  --file-path startup/stderr.txt \
  --destination ./stderr.txt

# Get custom log file
az batch node file download \
  --pool-id YOUR_POOL_ID \
  --node-id NODE_ID \
  --file-path startup/wd/starttask.log \
  --destination ./starttask.log
```

### 4. Using Python SDK
```python
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials

# Create Batch client
credentials = SharedKeyCredentials(account_name, account_key)
batch_client = BatchServiceClient(credentials, batch_url)

# Get node
nodes = batch_client.compute_node.list(pool_id)
node_id = next(nodes).id

# Download stdout
stream = batch_client.file.get_from_compute_node(
    pool_id, node_id, 'startup/stdout.txt')
with open('stdout.txt', 'wb') as f:
    for data in stream:
        f.write(data)

# Download custom log
stream = batch_client.file.get_from_compute_node(
    pool_id, node_id, 'startup/wd/starttask.log')
with open('starttask.log', 'wb') as f:
    for data in stream:
        f.write(data)
```

## Troubleshooting Tips

1. **Enable retain files**: Make sure your pool configuration has file retention enabled
2. **Check permissions**: Start task needs admin elevation to install software
3. **Use absolute paths**: When in doubt, use `%AZ_BATCH_TASK_DIR%` for file paths
4. **Test locally**: Test your script on a similar Windows VM first
5. **Simple test**: Start with a simple echo command to verify output is working:
   ```
   cmd /c "echo Test > test.txt && type test.txt"
   ```

## Alternative: Pre-built VM Image

Instead of installing at runtime, consider creating a custom VM image with VC++ runtime pre-installed:

1. Create a Windows VM in Azure
2. Install all required software (Python, VC++ runtime, etc.)
3. Generalize and capture the VM as an image
4. Use the custom image for your Batch pool

This approach is more reliable and faster than installing software via Start Task. 