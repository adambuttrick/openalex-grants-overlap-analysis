import os
import sys
import argparse
import duckdb
from pathlib import Path
from grants_db_common import (
    create_metadata_table,
    create_indexes,
    save_metadata,
    get_database_statistics,
    format_statistics_output
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Build DuckDB database from grants CSV file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build database from grants CSV:
  %(prog)s --grants-csv grants.csv --db-output grants.db
  
  # Build with verbose output:
  %(prog)s --grants-csv grants.csv --db-output grants.db --verbose
  
  # Build with custom chunk size:
  %(prog)s --grants-csv grants.csv --db-output grants.db --chunk-size 50000
        """
    )

    parser.add_argument('-g', '--grants-csv', required=True,
                        help='Path to grants.csv file')
    parser.add_argument('-d', '--db-output', default='grants.db',
                        help='Output database file path (default: grants.db)')
    parser.add_argument('-c', '--chunk-size', type=int, default=100000,
                        help='Chunk size for reading CSV (default: 100000)')
    parser.add_argument('-v', '--verbose',
                        action='store_true', help='Verbose output')
    parser.add_argument('-f', '--force', action='store_true',
                        help='Force overwrite existing database')

    return parser.parse_args()


def build_database(grants_csv_path, db_path, chunk_size=100000, verbose=False, force=False):
    if not os.path.exists(grants_csv_path):
        print(f"Error: Input file not found: {grants_csv_path}")
        return False

    if os.path.exists(db_path):
        if not force:
            response = input(f"Database {db_path} already exists. Overwrite? (y/n): ")
            if response.lower() != 'y':
                print("Operation cancelled.")
                return False
        print(f"Removing existing database: {db_path}")
        os.remove(db_path)

    print(f"Building database from {grants_csv_path}")
    print(f"Output database: {db_path}")

    conn = duckdb.connect(db_path)

    try:
        if verbose:
            conn.execute("SET enable_progress_bar = true")

        create_metadata_table(conn)

        print("Loading and processing grants CSV file with DuckDB...")

        conn.execute("""
            CREATE TABLE grants AS
            SELECT 
                work_id,
                LOWER(TRIM(doi)) as doi,
                field_name,
                subfield_path,
                TRY_CAST(json_extract_string(value, '$.funder') AS VARCHAR) as funder,
                TRY_CAST(json_extract_string(value, '$.award_id') AS VARCHAR) as award_id,
                source_id,
                doi_prefix,
                source_file_path
            FROM read_csv(?, 
                auto_detect=true,
                parallel=true,
                sample_size=100000)
        """, [grants_csv_path])

        total_rows = conn.execute("SELECT COUNT(*) FROM grants").fetchone()[0]
        parsed_rows = conn.execute(
            "SELECT COUNT(*) FROM grants WHERE funder IS NOT NULL").fetchone()[0]

        print(f"Loaded {total_rows:,} total rows")
        print(f"Parsed {parsed_rows:,} rows with valid funder information")

        create_indexes(conn)

        save_metadata(conn, grants_csv_path, total_rows, parsed_rows)

        stats = get_database_statistics(conn)
        print(format_statistics_output(stats))

        conn.commit()

        print(f"\n✓ Database built successfully: {db_path}")
        print(f"  File size: {os.path.getsize(db_path) / (1024*1024):.2f} MB")

        return True

    except Exception as e:
        print(f"\n✗ Error building database: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return False
    finally:
        conn.close()


def verify_database(db_path, verbose=False):
    if not os.path.exists(db_path):
        print(f"Error: Database file not found: {db_path}")
        return False

    try:
        conn = duckdb.connect(db_path, read_only=True)

        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]

        required_tables = ['grants', 'db_metadata']
        for table in required_tables:
            if table not in table_names:
                print(f"Error: Required table '{table}' not found in database")
                return False

        count = conn.execute("SELECT COUNT(*) FROM grants").fetchone()[0]
        if count == 0:
            print("Warning: Grants table is empty")
            return False

        if verbose:
            print(f"  Database verification passed")
            print(f"  Tables: {', '.join(table_names)}")
            print(f"  Grants records: {count:,}")

        conn.close()
        return True

    except Exception as e:
        print(f"Error verifying database: {e}")
        return False


def main():
    args = parse_arguments()
    success = build_database(
        args.grants_csv,
        args.db_output,
        args.chunk_size,
        args.verbose,
        args.force
    )
    if success:
        if not verify_database(args.db_output, args.verbose):
            return 1

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
