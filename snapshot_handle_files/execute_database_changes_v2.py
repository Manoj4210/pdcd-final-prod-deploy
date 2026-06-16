#!/usr/bin/env python3
"""
Production PDCD runner using partition rotation.

This runner keeps the same high-level flow as run_pdcd_changes.sh, but avoids
TRUNCATE ... CASCADE for staging cleanup. It requires the partition-based PDCD
objects to be deployed in the target schema.
"""

import configparser
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta

import psycopg2
from psycopg2 import OperationalError, pool, sql


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "pdcd_config.ini")
LOG_DIR = os.path.join(BASE_DIR, "logs")

LOCK_TIMEOUT = os.getenv("PDCD_LOCK_TIMEOUT", "5s")
STATEMENT_TIMEOUT = os.getenv("PDCD_STATEMENT_TIMEOUT", "45min")
IDLE_TX_TIMEOUT = os.getenv("PDCD_IDLE_TX_TIMEOUT", "2min")
MAX_PARALLEL_WORKERS_PER_GATHER = os.getenv(
    "PDCD_MAX_PARALLEL_WORKERS_PER_GATHER", "0")
RETRY_DELAY_SECONDS = int(os.getenv("PDCD_RETRY_DELAY_SECONDS", "180"))
MAX_RETRIES = int(os.getenv("PDCD_MAX_RETRIES", "3"))
ALLOW_INITIAL_CLEANUP = os.getenv(
    "PDCD_ALLOW_INITIAL_CLEANUP", "false").lower() == "true"
# Configure lock waiting thresholds
MAX_LOCK_WAIT_SECONDS = int(os.getenv("PDCD_MAX_LOCK_WAIT_SECONDS", "600"))
LOCK_CHECK_INTERVAL_SECONDS = int(os.getenv("PDCD_LOCK_CHECK_INTERVAL_SECONDS", "10"))


def format_duration(seconds):
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def format_error_message(exc):
    """Extracts the primary error message from an exception, stripping multiline SQL contexts."""
    return str(exc).split('\n')[0].strip()


class LogWriter:
    def __init__(self, path):
        self.handle = open(path, "a", buffering=1)

    def write(self, message=""):
        self.handle.write(f"{message}\n")
        self.handle.flush()

    def log(self, level, section, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"{timestamp} [{level:<5}] [{section}] {message}"
        self.write(log_line)
        if level in ("ERROR", "CRITICAL"):
            print(log_line, file=sys.stderr)
        else:
            print(log_line)

    def step_header(self, section):
        header_text = f"{'step_name':^45}|{'result':^24}|{'duration':^17}"
        separator = f"{'-' * 45}+{'-' * 24}+{'-' * 17}"
        self.log("INFO", section, header_text)
        self.log("INFO", section, separator)

    def step(self, section, step_name, result, started_at):
        duration = format_duration(time.monotonic() - started_at)
        step_text = f" {step_name:<44}| {result:<23}| {duration}"
        self.log("INFO", section, step_text)

    def close(self):
        self.handle.close()


class PDCDPartitionRunner:
    def __init__(self, section, config):
        self.section = section
        self.host = config.get("host")
        self.port = config.get("port", "5432")
        self.dbname = config.get("dbname")
        self.user = config.get("user")
        self.password = config.get("password")
        self.schema_name = config.get("schema_name")
        self.conn_pool = None
        self.log = None

    def init_pool(self):
        self.conn_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=1,
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            connect_timeout=10,
            application_name=f"pdcd_partition_runner:{self.section}",
        )

    @contextmanager
    def connection(self):
        conn = self.conn_pool.getconn()
        try:
            conn.autocommit = True
            self.configure_session(conn)
            yield conn
        finally:
            self.conn_pool.putconn(conn)

    def configure_session(self, conn):
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = %s;", (LOCK_TIMEOUT,))
            cur.execute("SET statement_timeout = %s;", (STATEMENT_TIMEOUT,))
            cur.execute(
                "SET idle_in_transaction_session_timeout = %s;", (IDLE_TX_TIMEOUT,))
            cur.execute(
                "SET max_parallel_workers_per_gather = %s;",
                (MAX_PARALLEL_WORKERS_PER_GATHER,),
            )
            cur.execute("SET client_min_messages = warning;")
            cur.execute(
                sql.SQL("SET search_path TO {};").format(
                    sql.Identifier(self.schema_name),
                )
            )

    @contextmanager
    def advisory_lock(self, conn):
        lock_key = f"{self.dbname}:{self.schema_name}:pdcd_partition"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s));", (lock_key,))
            locked = cur.fetchone()[0]

        if not locked:
            raise RuntimeError(
                f"Another PDCD partition run is already active for {self.dbname}.{self.schema_name}"
            )

        try:
            yield
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_unlock(hashtext(%s));", (lock_key,))

    def scalar(self, conn, query, params=None):
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()[0]

    def execute(self, conn, query, params=None):
        with conn.cursor() as cur:
            cur.execute(query, params)

    def run_step(self, conn, step_name, query, params=None, result="Completed"):
        started_at = time.monotonic()
        self.execute(conn, query, params)
        self.log.step(self.section, step_name, result, started_at)

    def run_scalar_step(self, conn, step_name, query, params=None, result_prefix="Completed"):
        started_at = time.monotonic()
        value = self.scalar(conn, query, params)
        self.log.step(self.section, step_name, f"{result_prefix}: {value}", started_at)
        return value

    def effective_schemas_cte(self):
        return """
            WITH effective_schemas AS (
              SELECT COALESCE(array_agg(schema_name), ARRAY[]::text[]) AS schemas
              FROM information_schema.schemata
              WHERE schema_name NOT LIKE 'pg_%'
                AND schema_name <> 'information_schema'
                AND schema_name <> current_schema()
            )
        """

    def validate_partition_setup(self, conn):
        required_functions = [
            "create_staging_partitions",
            "drop_old_staging_partitions",
            "rollback_failed_snapshot",
        ]
        for function_name in required_functions:
            exists = self.scalar(
                conn,
                """
                SELECT to_regprocedure(%s) IS NOT NULL;
                """,
                (f"{self.schema_name}.{function_name}(integer)",),
            )
            if not exists:
                raise RuntimeError(
                    f"Required function missing: {self.schema_name}.{function_name}(integer)"
                )

        for table_name in [
            "metadata_md5_staging_table_objects",
            "metadata_md5_staging_non_table_objects",
        ]:
            partitioned = self.scalar(
                conn,
                """
                SELECT c.relkind = 'p'
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s
                  AND c.relname = %s;
                """,
                (self.schema_name, table_name),
            )
            if not partitioned:
                raise RuntimeError(
                    f"Required partitioned table missing: {self.schema_name}.{table_name}"
                )

    def wait_for_exclusive_locks(self, conn, max_wait_seconds=MAX_LOCK_WAIT_SECONDS, check_interval_seconds=LOCK_CHECK_INTERVAL_SECONDS):
        start_time = time.monotonic()
        query = """
            SELECT COALESCE(string_agg(n.nspname || '.' || c.relname, ', '), '')
            FROM pg_locks l
            JOIN pg_class c ON c.oid = l.relation
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE l.mode = 'AccessExclusiveLock'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', %s)
              AND n.nspname NOT LIKE 'pg_%%'
              AND l.granted = true;
        """
        while True:
            with conn.cursor() as cur:
                cur.execute(query, (self.schema_name,))
                locked_relations = cur.fetchone()[0]

            if not locked_relations:
                break

            elapsed = time.monotonic() - start_time
            if elapsed >= max_wait_seconds:
                raise RuntimeError(
                    f"Timed out waiting {max_wait_seconds}s for active AccessExclusiveLocks to release on: {locked_relations}"
                )

            self.log.log(
                "WARN",
                self.section,
                f"Active AccessExclusiveLock on: {locked_relations}. Waiting {check_interval_seconds}s (elapsed: {int(elapsed)}s)..."
            )
            time.sleep(check_interval_seconds)

    def latest_snapshot_id(self, conn):
        return self.scalar(conn, "SELECT max(snapshot_id) FROM metadata_snapshot;")

    def is_initial_run(self, conn):
        return self.scalar(conn, "SELECT NOT EXISTS (SELECT 1 FROM metadata_snapshot);")

    def latest_change_count(self, conn):
        return self.scalar(
            conn,
            """
            SELECT count(*)
            FROM metadata_md5_changes
            WHERE snapshot_id = (SELECT max(snapshot_id) FROM metadata_snapshot)
              AND change_type <> 'UNCHANGED';
            """,
        )

    def write_header(self):
        self.log.log("INFO", self.section, "========================================")
        self.log.log(
            "INFO",
            self.section,
            f"Run started : {datetime.now().strftime('%a %b %d %H:%M:%S %Z %Y')}"
        )
        self.log.log("INFO", self.section, f"Section     : [{self.section}]")
        self.log.log("INFO", self.section, f"Host        : {self.host}")
        self.log.log("INFO", self.section, f"Port        : {self.port}")
        self.log.log("INFO", self.section, f"Database    : {self.dbname}")
        self.log.log("INFO", self.section, f"User        : {self.user}")
        self.log.log("INFO", self.section, f"Schema Name : {self.schema_name}")
        self.log.log("INFO", self.section, "Active Conns: 1")
        self.log.log("INFO", self.section, f"Lock Timeout: {LOCK_TIMEOUT}")
        self.log.log("INFO", self.section, f"Stmt Timeout: {STATEMENT_TIMEOUT}")
        self.log.log(
            "INFO",
            self.section,
            f"Max Parallel: {MAX_PARALLEL_WORKERS_PER_GATHER}"
        )
        self.log.log("INFO", self.section, "========================================")
        self.log.log("INFO", self.section, "Executing process_metadata_md5_changes...")

    def run(self):
        os.makedirs(LOG_DIR, exist_ok=True)
        log_path = os.path.join(
            LOG_DIR, f"{self.dbname}_{datetime.now().strftime('%Y-%m-%d')}.log")
        self.log = LogWriter(log_path)

        self.current_snapshot_id = None
        try:
            run_started_at = time.monotonic()
            self.init_pool()
            with self.connection() as conn:
                with self.advisory_lock(conn):
                    self.validate_partition_setup(conn)
                    self.write_header()
                    self.log.step_header(self.section)

                    # Pre-emptively wait for active exclusive locks on metadata target tables
                    self.wait_for_exclusive_locks(conn)

                    # Run connection in autocommit = True to avoid holding catalog locks across steps
                    conn.autocommit = True
                    try:
                        if self.is_initial_run(conn):
                            self.run_initial(conn)
                        else:
                            self.run_subsequent(conn)

                        change_count = self.latest_change_count(conn)
                        self.run_step(conn, "load_metadata_md5_metrics",
                                      "SELECT load_metadata_md5_metrics();")
                    except Exception as exc:
                        if self.current_snapshot_id is not None:
                            self.log.log("WARN", self.section, f"Run failed. Cleaning up failed snapshot_id: {self.current_snapshot_id}...")
                            try:
                                self.execute(conn, "SELECT rollback_failed_snapshot(%s);", (self.current_snapshot_id,))
                                self.log.log("INFO", self.section, f"Cleaned up failed snapshot_id: {self.current_snapshot_id}.")
                            except Exception as cleanup_exc:
                                clean_cleanup_err = format_error_message(cleanup_exc)
                                self.log.log("ERROR", self.section, f"Failed to cleanup snapshot_id {self.current_snapshot_id}: {clean_cleanup_err}")
                        raise exc

                    if change_count == 0:
                        self.log.log(
                            "INFO",
                            self.section,
                            "NOTICE:  No changes detected -> Only base metrics inserted."
                        )

                    self.log.log(
                        "INFO",
                        self.section,
                        f"Run completed: {datetime.now().strftime('%a %b %d %H:%M:%S %Z %Y')}"
                    )
                    self.log.log(
                        "INFO",
                        self.section,
                        f"Total duration: {format_duration(time.monotonic() - run_started_at)}"
                    )
                    self.log.write()

                    self.log.log("INFO", self.section, "========================================")

        except Exception as exc:
            if self.log:
                clean_err = format_error_message(exc)
                self.log.log("ERROR", self.section, f"Batch failed: {clean_err}")
            raise
        finally:
            if self.conn_pool:
                self.conn_pool.closeall()
            if self.log:
                self.log.close()

    def run_initial(self, conn):
        cte = self.effective_schemas_cte()
        self.log.log("INFO", self.section, f" {'mode':<44}| {'Initial Load':<23}| 00:00:00")

        if self.has_existing_initial_data(conn):
            if not ALLOW_INITIAL_CLEANUP:
                raise RuntimeError(
                    "Initial run found existing PDCD data. Refusing cleanup without "
                    "PDCD_ALLOW_INITIAL_CLEANUP=true because production mode avoids TRUNCATE CASCADE."
                )
            self.run_initial_cleanup(conn)

        snapshot_id = self.run_scalar_step(
            conn,
            "load_snapshot_table",
            "SELECT snapshot_id FROM load_snapshot_table();",
            result_prefix="snapshot_id",
        )
        self.current_snapshot_id = snapshot_id
        self.run_step(
            conn,
            "create_staging_partitions",
            "SELECT create_staging_partitions(%s);",
            (snapshot_id,),
            result=f"Created _p{snapshot_id}",
        )
        self.run_step(
            conn,
            "load_md5_metadata_table",
            f"""
            {cte}
            SELECT count(*)
            FROM effective_schemas, load_md5_metadata_table(effective_schemas.schemas);
            """,
        )
        self.run_step(
            conn,
            "load_md5_metadata_staging_table_objects",
            f"""
            {cte}
            SELECT count(*)
            FROM effective_schemas, load_md5_metadata_staging_table_objects(effective_schemas.schemas);
            """,
        )
        self.run_step(
            conn,
            "load_md5_metadata_staging_non_table_objects",
            f"""
            {cte}
            SELECT count(*)
            FROM effective_schemas, load_md5_metadata_staging_non_table_objects(effective_schemas.schemas);
            """,
        )
        # self.run_step(
        #     conn,
        #     "analyze_tables",
        #     """
        #     ANALYZE metadata_md5_changes;
        #     ANALYZE metadata_md5_staging_table_objects;
        #     ANALYZE metadata_md5_staging_non_table_objects;
        #     """,
        # )
        return snapshot_id

    def run_subsequent(self, conn):
        cte = self.effective_schemas_cte()
        self.log.log(
            "INFO",
            self.section,
            f" {'mode':<44}| {'Subsequent Compare Run':<23}| 00:00:00"
        )

        previous_snapshot_id = self.latest_snapshot_id(conn)
        self.run_step(
            conn,
            "prepare_previous_partitions",
            "SELECT drop_old_staging_partitions(%s);",
            (previous_snapshot_id,),
            result=f"Kept _p{previous_snapshot_id}",
        )

        snapshot_id = self.run_scalar_step(
            conn,
            "load_snapshot_table",
            "SELECT snapshot_id FROM load_snapshot_table();",
            result_prefix="snapshot_id",
        )
        self.current_snapshot_id = snapshot_id

        self.run_step(
            conn,
            "compare_load_md5_table_metadata",
            f"""
            {cte}
            SELECT count(*)
            FROM effective_schemas, compare_load_md5_table_metadata(effective_schemas.schemas);
            """,
        )
        self.run_step(
            conn,
            "compare_load_md5_non_table_metadata",
            f"""
            {cte}
            SELECT count(*)
            FROM effective_schemas, compare_load_md5_non_table_metadata(effective_schemas.schemas);
            """,
        )
        self.run_step(
            conn,
            "create_staging_partitions",
            "SELECT create_staging_partitions(%s);",
            (snapshot_id,),
            result=f"Created _p{snapshot_id}",
        )
        self.run_step(
            conn,
            "load_md5_metadata_staging_table_objects",
            f"""
            {cte}
            SELECT count(*)
            FROM effective_schemas, load_md5_metadata_staging_table_objects(effective_schemas.schemas);
            """,
        )
        self.run_step(
            conn,
            "load_md5_metadata_staging_non_table_objects",
            f"""
            {cte}
            SELECT count(*)
            FROM effective_schemas, load_md5_metadata_staging_non_table_objects(effective_schemas.schemas);
            """,
        )
        self.run_step(
            conn,
            "drop_old_partitions",
            "SELECT drop_old_staging_partitions(%s);",
            (snapshot_id,),
            result=f"Kept _p{snapshot_id}",
        )
        self.log.log(
            "INFO",
            self.section,
            f" Previous snapshot partition was _p{previous_snapshot_id}; current snapshot partition is _p{snapshot_id}."
        )
        return snapshot_id

    def has_existing_initial_data(self, conn):
        return self.scalar(
            conn,
            """
            SELECT EXISTS (
                SELECT 1 FROM metadata_md5_changes LIMIT 1
            ) OR EXISTS (
                SELECT 1 FROM metadata_md5_metrics LIMIT 1
            ) OR EXISTS (
                SELECT 1 FROM metadata_md5_staging_table_objects LIMIT 1
            ) OR EXISTS (
                SELECT 1 FROM metadata_md5_staging_non_table_objects LIMIT 1
            );
            """,
        )

    def run_initial_cleanup(self, conn):
        self.run_step(
            conn,
            "delete_orphan_initial_data",
            """
            DELETE FROM metadata_md5_changes;
            DELETE FROM metadata_md5_metrics;
            DELETE FROM metadata_md5_staging_table_objects;
            DELETE FROM metadata_md5_staging_non_table_objects;
            """,
        )


def load_config():
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Config file not found: {CONFIG_FILE}")
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config


def is_retryable_error(exc):
    message = str(exc).lower()
    retryable_markers = (
        "lock timeout",
        "deadlock detected",
        "could not obtain lock",
        "canceling statement due to statement timeout",
        "connection refused",
        "could not connect to server",
        "server closed the connection unexpectedly",
        "terminating connection",
        "timeout expired",
    )
    return isinstance(exc, OperationalError) or any(
        marker in message for marker in retryable_markers
    )


def main():
    config = load_config()
    sections = [section for section in config.sections(
        ) if section.startswith("database_")]
    if not sections:
        raise RuntimeError(f"No [database_*] sections found in {CONFIG_FILE}")

    for section in sections:
        runner = PDCDPartitionRunner(section, config[section])
        missing = [
            name
            for name in ("host", "port", "dbname", "user", "schema_name")
            if not getattr(runner, name)
        ]
        if missing:
            print(
                f"Skipping [{section}], missing required values: {', '.join(missing)}")
            continue

        attempt = 0
        while True:
            try:
                runner.run()
                break
            except Exception as exc:
                attempt += 1
                should_retry = attempt <= MAX_RETRIES and is_retryable_error(
                    exc)
                if not should_retry:
                    raise

                retry_at = (datetime.now() + timedelta(
                    seconds=RETRY_DELAY_SECONDS)).strftime('%a %b %d %H:%M:%S %Z %Y')
                clean_err = format_error_message(exc)
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(
                    f"{timestamp} [WARN ] [{section}] Attempt {attempt} failed with retryable error: {clean_err}",
                    file=sys.stderr,
                )
                print(
                    f"{timestamp} [INFO ] [{section}] Retrying after {RETRY_DELAY_SECONDS} seconds "
                    f"(retry {attempt} of {MAX_RETRIES}) at {retry_at}",
                    file=sys.stderr,
                )
                time.sleep(RETRY_DELAY_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        clean_err = format_error_message(exc)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{timestamp} [ERROR] [system] {clean_err}", file=sys.stderr)
        sys.exit(1)
