# 204- 240
def wait_for_exclusive_locks(self, conn, max_wait_seconds=600, check_interval_seconds=10):

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
            # Log to stderr and execution log file
            print(
                f"WARN: [{self.section}] Active AccessExclusiveLock detected on relations: {locked_relations}. "
                f"Waiting {check_interval_seconds}s (elapsed: {int(elapsed)}s)...",
                file=sys.stderr
            )
            self.log.write(
                f"WARN: Active AccessExclusiveLock on: {locked_relations}. Waiting {check_interval_seconds}s..."
            )
            time.sleep(check_interval_seconds)

# 291
# Pre-emptively wait for active exclusive locks on metadata target tables
self.wait_for_exclusive_locks(conn)