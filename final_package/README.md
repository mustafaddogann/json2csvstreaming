# Azure Batch JSON to CSV Conversion

This project contains a Python script and a self-contained environment to convert large JSON files to CSV format within an Azure Batch service.

## Deployment Package

The `final_package` directory contains everything needed to run the job. To deploy, zip the contents of this directory:

```bash
# On macOS or Linux
cd final_package
zip -r ../batch-deployment.zip .

# On Windows (using PowerShell)
Compress-Archive -Path final_package\* -DestinationPath batch-deployment.zip
```

Upload the resulting `batch-deployment.zip` to your Azure Blob Storage resource container.

## Azure Data Factory (ADF) Setup

In your ADF pipeline, use a "Custom" activity to run the Azure Batch job.

1.  **Link to your Azure Batch Account.**
2.  **Command:** In the command box for the activity, you must first unzip the package and then execute the `run.bat` script. Pass all arguments to the batch script.

    ```bash
    cmd /c "powershell.exe -NoProfile -ExecutionPolicy Bypass -Command \"Expand-Archive -Path batch-deployment.zip -DestinationPath . -Force\" && run.bat"
    ```

3.  **Resource Files:** Point to the `batch-deployment.zip` you uploaded to storage.

## How it Works

The `run.bat` script handles setting up the environment on the Batch node. It prepends the local Python executable and the `packages` directory to the system's PATH. This ensures that our self-contained Python and all its dependencies (especially the `ijson` C-backend) are found and used correctly, avoiding reliance on the node's pre-installed environment.

The main script, `batchactivity_json2csv.py`, then streams the JSON file, converts it to CSV, and uploads it back to storage, chunking the output for very large files. 