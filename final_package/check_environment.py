import os
import sys
import subprocess
import ctypes

print("=== Environment Diagnostic Script ===")
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")
print(f"Current directory: {os.getcwd()}")
print(f"Script location: {os.path.abspath(__file__)}")

# Check if running as admin
try:
    is_admin = ctypes.windll.shell32.IsUserAnAdmin()
    print(f"Running as admin: {is_admin}")
except:
    print("Could not determine admin status")

# Check for Visual C++ Runtime
print("\n=== Checking for Visual C++ Runtime ===")
system32_path = r"C:\Windows\System32"
dlls_to_check = ["msvcp140.dll", "vcruntime140.dll", "vcruntime140_1.dll"]

for dll in dlls_to_check:
    dll_path = os.path.join(system32_path, dll)
    exists = os.path.exists(dll_path)
    print(f"{dll}: {'FOUND' if exists else 'NOT FOUND'} at {dll_path}")

# Check local directory for DLLs
print("\n=== Checking local directory for DLLs ===")
local_dlls = ["vcruntime140.dll", "vcruntime140_1.dll", "msvcp140.dll"]
for dll in local_dlls:
    if os.path.exists(dll):
        print(f"{dll}: FOUND in current directory")
    else:
        print(f"{dll}: NOT FOUND in current directory")

# Check packages/site-packages directory
print("\n=== Checking packages directory ===")
for packages_dir in ['packages', 'site-packages', 'Lib/site-packages']:
    if os.path.exists(packages_dir):
        print(f"\nFound {packages_dir} directory")
        # Add to path for import to work
        if packages_dir not in sys.path:
            sys.path.insert(0, packages_dir)
        
        # Check for ijson
        ijson_paths = [
            os.path.join(packages_dir, 'ijson'),
            'ijson'  # Check if ijson is in root
        ]
        for ijson_path in ijson_paths:
            if os.path.exists(ijson_path):
                print(f"ijson found at: {ijson_path}")
                backends_path = os.path.join(ijson_path, 'backends')
                if os.path.exists(backends_path):
                    print("Checking for C extensions (.pyd files):")
                    found_pyd = False
                    for file in os.listdir(backends_path):
                        if file.endswith('.pyd'):
                            print(f"  - {file}")
                            found_pyd = True
                    if not found_pyd:
                        print("  - No .pyd files found.")


# Try to import C backend with detailed error
print("\n=== Testing ijson C backend import ===")
try:
    # Add common paths to sys.path just in case
    for path in ['site-packages', 'Lib/site-packages', '.']:
        if os.path.exists(path) and path not in sys.path:
            sys.path.insert(0, path)
    
    import ijson.backends.yajl2_c as yajl2_c
    print("SUCCESS: yajl2_c imported successfully!")
    print(f"Module location: {yajl2_c.__file__}")
except ImportError as e:
    print(f"FAILED to import yajl2_c: {e}")
    
    # Try alternative imports
    try:
        import ijson.backends._yajl2 as _yajl2
        print("SUCCESS: _yajl2 imported successfully!")
        print(f"Module location: {_yajl2.__file__}")
    except ImportError as e2:
        print(f"FAILED to import _yajl2: {e2}")

print("\n=== End of diagnostic ===") 