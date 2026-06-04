from configparser import ConfigParser
import subprocess
import sys

# Load config
config = ConfigParser()
config.read("pdcd_config.ini")

# Adjust section/key names to match your file
schema_name = config["database"]["schema_name"]

# PostgreSQL connection details
host = config["database"]["host"]
port = config["database"].get("port", "5432")
dbname = config["database"]["dbname"]
user = config["database"]["user"]

script_file = "revert_pdcd_functions.sql"

cmd = [
    "psql",
    "-h", host,
    "-p", port,
    "-U", user,
    "-d", dbname,
    "-v", f"schema_name={schema_name}",
    "-f", script_file,
]

print("Running:")
print(" ".join(cmd))

result = subprocess.run(cmd)

if result.returncode != 0:
    print(f"Failed with exit code {result.returncode}")
    sys.exit(result.returncode)

print("Revert script completed successfully.")
