import subprocess
import sys
import os

os.chdir("dbt_transform")

result = subprocess.run(
    [sys.executable, "-c",
     "from dbt.cli.main import cli; cli()",
     "run", "--profiles-dir", "."],
    capture_output=False
)
sys.exit(result.returncode)