import streamlit as st
import pandas as pd
import configparser
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from streamlit_datetime_picker import date_time_picker
import re

# -------------------------------------------------
# Page config
# -------------------------------------------------
st.set_page_config(page_title="Postgres CDC Report",
                   layout="wide", initial_sidebar_state="expanded")

# Increase sidebar width
st.markdown("""
    <style>
        [data-testid="stSidebar"] {
            width: 500px !important;
        }
        [data-testid="stSidebar"] > div:first-child {
            width: 500px !important;
        }
    </style>
    """, unsafe_allow_html=True)
st.title("PostgreSQL Change Report")

# -------------------------------------------------
# Load DB config
# -------------------------------------------------
config = configparser.ConfigParser()
config.read("pdcd_config.ini")

# Parse multiple database configurations
db_configs = {}
for section in config.sections():
    if section.startswith("database"):
        db_configs[section] = {
            'host': config[section].get('host'),
            'port': config[section].get('port'),
            'dbname': config[section].get('dbname'),
            'user': config[section].get('user'),
            'password': config[section].get('password'),
            'schema_name': config[section].get('schema_name', '')
        }

if not db_configs:
    st.error("No database configurations found in pdcd_config.ini")
    st.stop()

# -------------------------------------------------
# Helper Functions
# -------------------------------------------------


def normalize(name: str) -> str:
    return name.split("(", 1)[0] if name else ""


def preview(text: str, limit: int = 90) -> str:
    return text if len(text) <= limit else text[:limit] + "..."


def format_processed_time(dt) -> str:
    """Format datetime to YYYY-MM-DD HH:MM:SS.xx format"""
    if pd.isna(dt):
        return ""
    if isinstance(dt, str):
        try:
            dt = pd.to_datetime(dt)
        except:
            return dt
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]


def load_data_for_database(db_config, use_time_window, start_time, end_time):
    """
    Reuses existing single-database logic exactly as-is.
    Returns: (df, metadata_dict) or (None, error_message)
    """
    try:
        DB_URL = (
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        engine = create_engine(DB_URL)

        metadata = {
            'host': db_config['host'],
            'dbname': db_config['dbname'],
            'use_time_window': use_time_window
        }
        schema_name = db_config['schema_name']

        # Detect snapshots if time not provided (EXISTING LOGIC)
        if not use_time_window:
            snap_df = pd.read_sql(
                f"""
                SELECT DISTINCT snapshot_id, MIN(processed_time) as snapshot_time
                FROM {schema_name}.metadata_md5_changes
                GROUP BY snapshot_id
                ORDER BY snapshot_id DESC
                LIMIT 2
                """,
                engine,
            )

            if len(snap_df) < 2:
                return None, "At least 2 snapshots are required for comparison."

            latest_snap = int(snap_df.iloc[0]["snapshot_id"])
            prev_snap = int(snap_df.iloc[1]["snapshot_id"])

            latest_time = snap_df.iloc[0]["snapshot_time"]
            prev_time = snap_df.iloc[1]["snapshot_time"]

            metadata['prev_snap'] = prev_snap
            metadata['latest_snap'] = latest_snap
            metadata['caption'] = f"Comparing: {prev_time} → {latest_time}"
        else:
            metadata['caption'] = f"Monitoring Window: {start_time} → {end_time}"

        # Load change data
        if use_time_window:
            QUERY = f"""
                SELECT snapshot_id, schema_name, object_type, object_type_name,
                       object_subtype, object_subtype_name, change_type,
                       processed_time, object_subtype_details
                FROM {schema_name}.metadata_md5_changes
                WHERE processed_time BETWEEN %(start)s AND %(end)s
                ORDER BY snapshot_id, schema_name;
            """
            df = pd.read_sql(
                QUERY,
                engine,
                params={"start": start_time, "end": end_time}
            )
        else:
            QUERY = f"""
                SELECT snapshot_id, schema_name, object_type, object_type_name,
                       object_subtype, object_subtype_name, change_type,
                       processed_time, object_subtype_details
                FROM {schema_name}.metadata_md5_changes
                WHERE snapshot_id = %(latest)s
                ORDER BY snapshot_id, schema_name;
            """
            df = pd.read_sql(
                QUERY,
                engine,
                params={"latest": metadata['latest_snap']}
            )

        if df.empty:
            return None, "No changes found."

        # Normalize
        df["object_type"] = df["object_type"].str.upper()
        df["object_subtype"] = df["object_subtype"].fillna("").str.upper()
        df["change_type"] = df["change_type"].str.upper()
        df["object_subtype_details"] = df["object_subtype_details"].fillna("")

        return df, metadata

    except Exception as e:
        return None, f"Error connecting to database: {str(e)}"


def render_overview_tab(df, db_config):
    """Renders the Overview tab (EXISTING LOGIC)"""

    # Schema Impact Summary (EXISTING LOGIC + timing columns)
    schema_rows = []
    # for schema, sdf in df.groupby("schema_name"):
    for (snapshot_id, schema), sdf in df.groupby(["snapshot_id", "schema_name"]):
        schema_rows.append({
            "Snapshot ID": snapshot_id,
            "Processed Time": format_processed_time(sdf["processed_time"].iloc[0]),
            "Schema": schema,
            "Change Types Detected": ", ".join(sorted(sdf["change_type"].unique())),
            "Tables Affected": sdf[sdf["object_type"] == "TABLE"]["object_type_name"].nunique(),
            "Schema Objects Affected": sdf[sdf["object_type"] != "TABLE"]["object_type_name"].nunique(),
            "Total Change Operations": len(sdf),
        })

    st.subheader("Schema Impact Summary")
    schema_df = pd.DataFrame(schema_rows)
    schema_df.index = schema_df.index + 1
    st.dataframe(schema_df, use_container_width=True)

    # Table Summary
    fully_deleted_tables = {
        (r["snapshot_id"], r["schema_name"], r["object_type_name"])
        for _, r in df[
            (df["object_type"] == "TABLE")
            & (df["object_subtype"] == "")
            & (df["change_type"] == "DELETED")
        ].iterrows()
    }

    table_rows = []
    for (snapshot_id, schema, table), tdf in df[df["object_type"] == "TABLE"].groupby(
        ["snapshot_id", "schema_name", "object_type_name"]
    ):
        if (snapshot_id, schema, table) in fully_deleted_tables:
            change = "DELETED"
        else:
            change = ", ".join(sorted(tdf["change_type"].unique()))

        table_rows.append({
            "Snapshot ID": snapshot_id,
            "Processed Time": format_processed_time(tdf["processed_time"].min()),
            "Schema": schema,
            "Table": table,
            "Change Type": change,
            "Total Changes": len(tdf),
        })

    st.subheader("Table Changes Summary")
    table_df = pd.DataFrame(table_rows)
    table_df.index = table_df.index + 1
    st.dataframe(table_df, use_container_width=True)


def render_detailed_tab(df):
    """Renders the Detailed View tab (EXISTING LOGIC + timing columns)"""

    fully_deleted_tables = {
        (r["snapshot_id"], r["schema_name"], r["object_type_name"])
        for _, r in df[
            (df["object_type"] == "TABLE")
            & (df["object_subtype"] == "")
            & (df["change_type"] == "DELETED")
        ].iterrows()
    }

    # Column & Dependency Changes (EXISTING LOGIC + timing columns)
    column_rows = []
    columns_df = df[df["object_subtype"] == "COLUMN"]

    for (snapshot_id, schema, parent, col), cdf in columns_df.groupby(
        ["snapshot_id", "schema_name", "object_type_name", "object_subtype_name"]
    ):
        parent_type = cdf["object_type"].iloc[0]

        if parent_type == "TABLE" and (snapshot_id, schema, parent) in fully_deleted_tables:
            continue

        scope_df = df[
            (df["schema_name"] == schema)
            & (df["object_type_name"] == parent)
        ]

        safe_col = re.escape(str(col))  # escape regex special characters

        deps = scope_df[
            scope_df["object_subtype"].isin(
                ["INDEX", "CONSTRAINT", "SEQUENCE", "REFERENCE", "TRIGGER"]
            )
            & scope_df["object_subtype_details"].astype(str).str.contains(
                rf"\b{safe_col}\b",
                case=False,
                regex=True,
                na=False
            )
        ]

        dep_list = [
            f"{r['object_subtype']}: {r['object_subtype_name']} ({r['change_type']})"
            for _, r in deps.iterrows()
        ]

        column_rows.append({
            "Snapshot ID": snapshot_id,
            "Processed Time": format_processed_time(cdf["processed_time"].min()),
            "Schema": schema,
            "Object Type": parent_type,
            "Object Name": parent,
            "Column": col,
            "Change Type": ", ".join(sorted(cdf["change_type"].unique())),
            "Dependent Objects": "; ".join(dep_list),
        })

    st.subheader("Column & Dependency Changes")

    if column_rows:
        column_df = pd.DataFrame(column_rows)
        column_df.index = column_df.index + 1
        column_df["Impacted Objects (Preview)"] = column_df["Dependent Objects"].apply(
            preview)

        st.dataframe(
            column_df[
                [
                    "Snapshot ID",
                    "Processed Time",
                    "Schema",
                    "Object Type",
                    "Object Name",
                    "Column",
                    "Change Type",
                    "Impacted Objects (Preview)",
                ]
            ],
            use_container_width=True,
        )

        with st.expander("View full dependency details"):
            # REQUIREMENT 2: Only show rows with impacted/dependent objects
            impacted_column_df = column_df[column_df["Dependent Objects"].str.len(
            ) > 0]

            if not impacted_column_df.empty:
                for _, r in impacted_column_df.iterrows():
                    st.markdown(f"""
**Snapshot ID:** {r['Snapshot ID']}

**Processed Time:** {r['Processed Time']}

**Schema:** {r['Schema']}

**Parent Object:** {r['Object Name']}

**Column:** {r['Column']}

**Impacted Objects:**
{r['Dependent Objects']}

---
""")
            else:
                st.info("No columns with dependent/impacted objects found.")
    else:
        st.info("No column-level changes found.")

    # Table-Level Objects (EXISTING LOGIC + timing columns)
    table_object_rows = []
    table_level_objects = df[
        (df["object_type"] == "TABLE")
        & (df["object_subtype"].isin(["TRIGGER", "RULE", "CHECK CONSTRAINT", "TABLE CONSTRAINT", "SEQUENCE"]))
    ]

    for (snapshot_id, schema, table, subtype, name), g in table_level_objects.groupby(
        ["snapshot_id", "schema_name", "object_type_name",
            "object_subtype", "object_subtype_name"]
    ):
        if (snapshot_id, schema, table) in fully_deleted_tables:
            continue

        table_object_rows.append({
            "Snapshot ID": snapshot_id,
            "Processed Time": format_processed_time(g["processed_time"].min()),
            "Schema": schema,
            "Table": table,
            "Object Type": subtype,
            "Object Name": name,
            "Change Type": ", ".join(sorted(g["change_type"].unique())),
        })

    st.subheader("Table-Level Object Changes")

    if table_object_rows:
        table_obj_df = pd.DataFrame(table_object_rows)
        table_obj_df.index = table_obj_df.index + 1
        st.dataframe(table_obj_df, use_container_width=True)
    else:
        st.info("No table-level object changes found.")

    # Schema-Level Objects (EXISTING LOGIC + timing columns)
    schema_object_detail_rows = []

    for (snapshot_id, schema, ot, on), g in df[df["object_type"] != "TABLE"].groupby(
        ["snapshot_id", "schema_name", "object_type", "object_type_name"]
    ):
        schema_object_detail_rows.append({
            "Snapshot ID": snapshot_id,
            "Processed Time": format_processed_time(g["processed_time"].min()),
            "Schema": schema,
            "Object Type": ot,
            "Object Name": normalize(on),
            "Change Type": ", ".join(sorted(g["change_type"].unique())),
            "Total Changes": len(g),
        })

    st.subheader("Schema-Level Object Changes ")

    if schema_object_detail_rows:
        schema_obj_df = pd.DataFrame(schema_object_detail_rows)
        schema_obj_df.index = schema_obj_df.index + 1
        st.dataframe(schema_obj_df,
                     use_container_width=True)
    else:
        st.info("No schema-level object changes found.")


# -------------------------------------------------
# Initialize session state for tracking selected tab
# -------------------------------------------------
if 'selected_db_index' not in st.session_state:
    st.session_state.selected_db_index = 0

# -------------------------------------------------
# Global Time Filter (Applied to ALL databases) - REQUIREMENT 1 & 3
# -------------------------------------------------

with st.sidebar:
    st.subheader("Time Filter")

    # Custom CSS to auto-expand and fix alignment
    st.markdown("""
    <style>
        .stDateInput, .stTimeInput {
            width: 100% !important;
        }
        [data-testid="stDateInput"] {
            max-width: 100% !important;
        }
    </style>
    """, unsafe_allow_html=True)

    # Quick Filter Buttons (before widgets)
    col1, col2 = st.columns([3, 1])

    with col1:
        st.write("**Quick Filters**")
        quick_filter_cols = st.columns(5)

        current_time = datetime.now()

        with quick_filter_cols[0]:
            if st.button("1D", key="filter_1d", use_container_width=True):
                st.session_state.quick_filter_range = (
                    current_time - timedelta(days=1), current_time)
                st.rerun()

        with quick_filter_cols[1]:
            if st.button("1W", key="filter_1w", use_container_width=True):
                st.session_state.quick_filter_range = (
                    current_time - timedelta(weeks=1), current_time)
                st.rerun()

        with quick_filter_cols[2]:
            if st.button("1M", key="filter_1m", use_container_width=True):
                st.session_state.quick_filter_range = (
                    current_time - timedelta(days=30), current_time)
                st.rerun()

        with quick_filter_cols[3]:
            if st.button("3M", key="filter_3m", use_container_width=True):
                st.session_state.quick_filter_range = (
                    current_time - timedelta(days=90), current_time)
                st.rerun()

        with quick_filter_cols[4]:
            if st.button("1Y", key="filter_1y", use_container_width=True):
                st.session_state.quick_filter_range = (
                    current_time - timedelta(days=365), current_time)
                st.rerun()

    with col2:
        if st.button(" Clear", key="clear_time_filter"):
            # Clear all time-related session state keys
            if "start_datetime" in st.session_state:
                del st.session_state["start_datetime"]
            if "end_datetime" in st.session_state:
                del st.session_state["end_datetime"]
            if "quick_filter_range" in st.session_state:
                del st.session_state["quick_filter_range"]
            st.rerun()

    st.write("**Date & Time Selection**")

    start_dt = date_time_picker(
        "Start Date & Time",
        key="start_datetime"
    )

    end_dt = date_time_picker(
        "End Date & Time",
        key="end_datetime"
    )

    # Apply quick filter if set
    if "quick_filter_range" in st.session_state:
        start_dt, end_dt = st.session_state.quick_filter_range
        del st.session_state.quick_filter_range

    USE_TIME_WINDOW = start_dt is not None and end_dt is not None
    START_TIME = END_TIME = None

    if start_dt is not None and end_dt is None:
        # REQUIREMENT 3: Auto-fill end datetime if only start is selected
        end_dt = datetime.now()
        USE_TIME_WINDOW = True

    if USE_TIME_WINDOW:
        try:
            # Remove timezone info if present (make naive)
            if start_dt.tzinfo is not None:
                start_dt = start_dt.replace(tzinfo=None)
            if end_dt.tzinfo is not None:
                end_dt = end_dt.replace(tzinfo=None)

            # If only date was selected, adjust time accordingly
            if start_dt.time() == datetime.min.time():
                START_TIME = datetime.combine(
                    start_dt.date(), datetime.min.time())
            else:
                START_TIME = start_dt

            if end_dt.time() == datetime.min.time():
                # Set to 23:59:59 for end date
                END_TIME = datetime.combine(
                    end_dt.date(), datetime.max.time().replace(microsecond=0))
            else:
                END_TIME = end_dt

            # Validate if both provided
            if START_TIME >= END_TIME:
                st.error(
                    "Start DateTime must be earlier than End DateTime")
                st.stop()

        except Exception as e:
            st.error(f"Invalid datetime: {str(e)}")
            st.stop()
    else:
        st.info("No time window selected. Using latest 2 snapshots for comparison.")

# -------------------------------------------------
# Load data for ALL databases
# -------------------------------------------------
db_data = {}
for db_key, db_config in db_configs.items():
    df, result = load_data_for_database(
        db_config, USE_TIME_WINDOW, START_TIME, END_TIME)
    if df is not None:
        db_data[db_key] = {
            'df': df,
            'metadata': result,
            'config': db_config
        }
    else:
        db_data[db_key] = {
            'df': None,
            'error': result,
            'config': db_config
        }

# -------------------------------------------------
# Create parent tabs for each database
# -------------------------------------------------
tab_labels = [
    f"{data['config']['host']}/{data['config']['dbname']}" for data in db_data.values()]

# Use dropdown to select database
selected_tab_label = st.selectbox(
    "Select Database:",
    tab_labels,
    label_visibility="visible"
)

selected_db_index = tab_labels.index(selected_tab_label)
st.session_state.selected_db_index = selected_db_index

# Get the selected database data
selected_db_key = list(db_data.keys())[selected_db_index]
selected_data = db_data[selected_db_key]

# -------------------------------------------------
# Context-specific filters (appear once, update per tab)
# -------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.subheader(
    f" Filters for {selected_data['config']['host']}/{selected_data['config']['dbname']}")

if selected_data['df'] is not None:
    df = selected_data['df']

    # Schema Filter (context-specific, single instance)
    available_schemas = sorted(df["schema_name"].unique())
    schema_filter = st.sidebar.multiselect(
        "Schema",
        available_schemas,
        default=available_schemas,
        key="schema_filter"
    )

    # Change Type Filter (context-specific, single instance)
    available_change_types = sorted(df["change_type"].unique())
    change_filter = st.sidebar.multiselect(
        "Change Type",
        available_change_types,
        default=available_change_types,
        key="change_filter"
    )
else:
    schema_filter = []
    change_filter = []
    st.sidebar.info("No data available for filtering")

# -------------------------------------------------
# Render content for selected database
# -------------------------------------------------
if selected_data['df'] is None:
    st.error(selected_data['error'])
else:
    df = selected_data['df']
    metadata = selected_data['metadata']
    db_config = selected_data['config']

    # Display caption
    st.caption(metadata['caption'])

    # Apply filters - REQUIREMENT 2: Fallback to ALL if none selected
    if not schema_filter:
        schema_filter = df["schema_name"].unique().tolist()
    if not change_filter:
        change_filter = df["change_type"].unique().tolist()

    filtered_df = df[
        df["schema_name"].isin(schema_filter)
        & df["change_type"].isin(change_filter)
    ]

    if filtered_df.empty:
        st.warning("No changes found with current filters.")
    else:
        # Create child tabs: Overview and Detailed View
        tab_overview, tab_detail = st.tabs(["Overview", "Detailed View"])

        with tab_overview:
            render_overview_tab(filtered_df, db_config)

        with tab_detail:
            render_detailed_tab(filtered_df)

###################################################################################
