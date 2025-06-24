#!/usr/bin/env python3
print("Hello from Python!")
print("Current working directory:")
import os
print(os.getcwd())
print("Python path:")
import sys
for p in sys.path:
    print(f"  {p}") 