import os
import shutil
import subprocess
import sys

# --- Configuration ---
BUILD_DIR = "dist"
PACKAGE_DIR = os.path.join(BUILD_DIR, "packages")
SOURCE_FILES = [
    "batchactivity_json2csv.py",
    # "run.bat", # This is not used by the ADF Custom Activity
    "batchaccounttest.py",
    "hello_batch.py",
    "json2csv1.py",
    "local_json2csv.py",
]
REQUIREMENTS_FILE = "requirements.txt"
OUTPUT_ZIP_FILE = "batch-script" # The .zip extension is added automatically

# --- Build Script ---

def main():
    """Builds the deployment package."""
    print("--- Starting build ---")

    # 1. Clean up previous build
    if os.path.exists(BUILD_DIR):
        print(f"Removing existing build directory: {BUILD_DIR}")
        shutil.rmtree(BUILD_DIR)
    
    print(f"Creating build directory: {BUILD_DIR}")
    os.makedirs(PACKAGE_DIR)

    # 2. Copy source files
    print("Copying source files...")
    for file_name in SOURCE_FILES:
        if os.path.exists(file_name):
            shutil.copy(file_name, BUILD_DIR)
            print(f"  - Copied {file_name}")
        else:
            print(f"  - Warning: {file_name} not found, skipping.")

    # 3. Install packages
    print("\nInstalling dependencies from requirements.txt...")
    if not os.path.exists(REQUIREMENTS_FILE):
        print(f"Error: {REQUIREMENTS_FILE} not found. Cannot install dependencies.")
        sys.exit(1)
        
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install",
            "--target", PACKAGE_DIR,
            "--requirement", REQUIREMENTS_FILE,
            "--only-binary", ":all:"
        ])
        print("Dependencies installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error installing dependencies: {e}")
        sys.exit(1)

    # 4. Create zip file
    print(f"\nCreating deployment archive: {OUTPUT_ZIP_FILE}.zip")
    shutil.make_archive(OUTPUT_ZIP_FILE, 'zip', BUILD_DIR)

    print("\n--- Build successful! ---")
    print(f"Deployment package created at: {OUTPUT_ZIP_FILE}.zip")
    print("You can now upload this file to Azure Blob Storage.")

if __name__ == "__main__":
    main() 