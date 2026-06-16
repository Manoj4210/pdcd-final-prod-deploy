from configparser import ConfigParser
import subprocess
import os
import sys

CONFIG_FILE = "pdcd_config.ini"
SECTION = "database_1"  # Change to database_2 if needed

config = ConfigParser()
config.read(CONFIG_FILE)

if SECTION not in config:
    sys.exit(f"Section '{SECTION}' not found in {CONFIG_FILE}")

db = config[SECTION]

host = db["host"]
port = db.get("port", "5432")
dbname = db["dbname"]
user = db["user"]
password = db.get("password", "")
schema_name = db["schema_name"]

cmd = [
    "psql",
    "-h", host,
    "-p", port,
    "-U", user,
    "-d", dbname,
    "-v", f"schema_name={schema_name}",
    "-f", "revert_pdcd_fucntions.sql",
]

env = os.environ.copy()
if password:
    env["PGPASSWORD"] = password

print("Executing:")
print(" ".join(cmd))

result = subprocess.run(cmd, env=env)

if result.returncode != 0:
    sys.exit(result.returncode)

print("Completed successfully.")
