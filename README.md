# PostgreSQL Database Change Detection (PDCD) Deployment & UI

This standardizes the deployment and change-tracking visibility for PostgreSQL databases across multiple environments. The suite provides an automated way to initialize schemas, run change detection tracking, and visualize database changes via an interactive Streamlit UI.

## Project Structure

The project consists of three main parts:
1. **Database Initialization (`deploy_scripts/deploy_pdcd.sh`)**: Deploys the base schema and tracking logic using the optimized SQL code base (`deploy_scripts/initialize_pdcd_functions.sql`).
2. **Change Detection Execution (`deploy_scripts/execute_database_changes.py`)**: Generates and audits metadata changes across multiple databases using an advisory-locked, production-safe Python runner.
3. **Change Visibility UI (`streamlit_app/pdcd_app.py`)**: A Streamlit application for reviewing and analyzing tracked database changes.

---

## 1. Configuration

The backend scripts rely on a central configuration file located at `deploy_scripts/pdcd_config.ini`. The Python UI expects a similar `config.ini` in its execution directory (`streamlit_app/config.ini`).

### Example `pdcd_config.ini`
```ini
[database_1]
host = localhost
port = 5432
dbname = my_database
user = my_user
password = secret
schema_name = data_tools

[database_2]
host = dev.example.com
port = 5432
dbname = dev_db
user = dev_user
password = secret
schema_name = data_tools
```

---

## 2. Automation Scripts & Core Execution

### a) `deploy_pdcd.sh`
This bash script reads the listed database sections in `pdcd_config.ini` and deploys the essential SQL code base (`initialize_pdcd_functions.sql`) to each of them sequentially.
- **What it does**: Ensures the configured custom schema logic and tables are available in the target database with zero-cascade safe creation.
- **How to run**:
  ```bash
  cd deploy_scripts
  chmod +x deploy_pdcd.sh
  ./deploy_pdcd.sh
  ```

### b) `execute_database_changes.py`
This python script connects to each initialized database and executes the underlying PL/pgSQL functions that handle the logging of database changes.
- **What it does**: Compares snapshots or previous states and calculates MD5s to identify DDL differences sequentially with minimal lock overhead.
- **Logs**: Creates execution logs in a `logs/` subdirectory named based on the database and date (e.g., `logs/my_database_2026-06-02.log`).
- **How to run**:
  ```bash
  cd deploy_scripts
  python execute_database_changes.py
  ```

---

## 3. Streamlit UI (`pdcd_app.py`)

A fully-featured dashboard built with Streamlit and Pandas that provides insight into all detected metadata changes. 

### Features
- **Multi-DB Support**: Dropdown menu at the top to seamlessly iterate through various configured databases.
- **Date & Time Filters**: Support for selecting specific viewing time windows and quick-filters (`1D`, `1W`, `1M`, `3M`, `1Y`).
- **Granular Viewing tabs**:
  - **Overview**: Summarizes schemas affected, total operation count, and table-specific CRUD states.
  - **Detailed View**: Displays granular column modifications, sequence drops, dependent/impacted constraint changes, and specific dependency tracking previews.

### Dependencies
Ensure the following Python packages are installed:
```bash
cd streamlit_app
pip install -r requirements.txt
```

### How to Run
Ensure you have a `config.ini` alongside the Python script containing your connection sections.
```bash
cd streamlit_app
streamlit run pdcd_app.py
```

## End-to-End Workflow summary
1. Setup connections in `deploy_scripts/pdcd_config.ini` and `streamlit_app/config.ini`.
2. Run `./deploy_pdcd.sh` to install schemas required to track the data.
3. Schedule or perform manual runs using `python execute_database_changes.py`.
4. Spin up the Streamlit UI using `streamlit run pdcd_app.py` inside the `streamlit_app` folder for visual representations of DB changes.
# pdcd-final-prod-deploy
