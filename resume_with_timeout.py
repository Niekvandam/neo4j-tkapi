#!/usr/bin/env python3
"""
Resume script with timeout configuration.
This script resumes the failed run with proper timeout settings.
"""

import subprocess
import sys
import os

def main():
    """Resume the failed run with timeout configuration."""
    
    # Set environment variables for timeout configuration
    os.environ['TKAPI_CONNECT_TIMEOUT'] = '15.0'  # 15 seconds
    os.environ['TKAPI_READ_TIMEOUT'] = '300.0'    # 5 minutes (reduced from infinite)
    os.environ['TKAPI_MAX_RETRIES'] = '3'
    os.environ['TKAPI_BACKOFF_FACTOR'] = '0.5'
    
    # Resume the specific failed run
    run_id = "run_20250625_100712"
    
    print("🚀 Resuming failed run with timeout configuration:")
    print(f"   • Connect timeout: {os.environ['TKAPI_CONNECT_TIMEOUT']} seconds")
    print(f"   • Read timeout: {os.environ['TKAPI_READ_TIMEOUT']} seconds")
    print(f"   • Max retries: {os.environ['TKAPI_MAX_RETRIES']}")
    print(f"   • Backoff factor: {os.environ['TKAPI_BACKOFF_FACTOR']}")
    print(f"   • Resuming run: {run_id}")
    print()
    
    # Build the command
    cmd = [
        sys.executable, 
        "src/main.py", 
        "--resume-run", 
        run_id
    ]
    
    print(f"📋 Command: {' '.join(cmd)}")
    print()
    
    try:
        # Execute the command
        result = subprocess.run(cmd, check=True)
        print("✅ Run completed successfully!")
        return result.returncode
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Run failed with exit code: {e.returncode}")
        return e.returncode
    except KeyboardInterrupt:
        print("\n⚠️ Run interrupted by user")
        return 130
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 