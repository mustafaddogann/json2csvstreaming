import os
import shutil
import subprocess
import sys
import glob

# --- Configuration ---
BUILD_DIR = "dist"
PACKAGE_DIR = os.path.join(BUILD_DIR, "packages")
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

    # 2. Copy source files and Python runtime
    print("Copying source files and Python runtime...")
    
    # Use glob to find all relevant files. This is more robust than a fixed list.
    extensions_to_copy = ["*.py", "*.pyd", "*.dll", "*.exe", "*.cat", "*.zip", "*._pth", "*.bat"]
    files_to_copy = []
    for ext in extensions_to_copy:
        files_to_copy.extend(glob.glob(ext))

    # De-duplicate the list
    files_to_copy = list(set(files_to_copy))

    # Exclude the build script itself
    if "build.py" in files_to_copy:
        files_to_copy.remove("build.py")

    for file_name in files_to_copy:
        shutil.copy(file_name, BUILD_DIR)
        print(f"  - Copied {file_name}")
        
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
            "--only-binary", ":all:",
            "--platform", "win_amd64",
            "--python-version", "310"
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