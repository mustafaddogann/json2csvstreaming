import csv
import sys

def diagnose_csv(file_path):
    """Diagnose potential CSV issues that could cause ADF errors"""
    print(f"🔍 Analyzing: {file_path}\n")
    
    try:
        # First, check raw file characteristics
        with open(file_path, 'rb') as f:
            first_bytes = f.read(3)
            has_bom = first_bytes == b'\xef\xbb\xbf'
            if has_bom:
                print("⚠️  File has UTF-8 BOM (byte order mark)")
            
        # Check line endings
        with open(file_path, 'rb') as f:
            sample = f.read(10000)
            if b'\r\n' in sample:
                line_ending = "Windows (CRLF)"
            elif b'\n' in sample:
                line_ending = "Unix (LF)"
            else:
                line_ending = "Unknown"
            print(f"📄 Line endings: {line_ending}")
        
        # Analyze CSV structure
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            # Show first few lines raw
            print("\n📝 First 3 lines (raw):")
            f.seek(0)
            for i in range(3):
                line = f.readline().rstrip('\n\r')
                print(f"  Line {i+1}: {repr(line)}")
            
            # Parse CSV
            f.seek(0)
            reader = csv.reader(f)
            header = next(reader)
            print(f"\n📊 Header: {len(header)} columns")
            print(f"  Columns: {header}")
            
            # Check rows
            row_issues = []
            for row_num, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    row_issues.append(f"Row {row_num}: {len(row)} columns (expected {len(header)})")
                    if len(row_issues) == 1:  # Show details of first problematic row
                        print(f"\n❌ First problematic row:")
                        print(f"  Row {row_num}: {row}")
                        print(f"  Column count: {len(row)} (expected {len(header)})")
                if row_num > 100:  # Check first 100 rows
                    break
            
            if row_issues:
                print(f"\n⚠️  Found {len(row_issues)} rows with column count issues:")
                for issue in row_issues[:5]:  # Show first 5
                    print(f"  {issue}")
            else:
                print("\n✅ All rows have correct column count")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        diagnose_csv(sys.argv[1])
    else:
        # Try common locations
        import os
        if os.path.exists('client_t_chunks/client_t_1.csv'):
            diagnose_csv('client_t_chunks/client_t_1.csv')
        elif os.path.exists('client_t.csv'):
            diagnose_csv('client_t.csv')
        else:
            print("Usage: python diagnose_csv.py <csv_file>") 