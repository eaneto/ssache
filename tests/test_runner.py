import glob
import subprocess

from ssache_client import initialize_ssache, kill_ssache

test_files = glob.glob("tests/*_test.py")

for test in test_files:
    pid = initialize_ssache()
    try:
        print(f"Executing {test}")
        command = ["python", test]
        subprocess.run(command)
    finally:
        kill_ssache(pid)
