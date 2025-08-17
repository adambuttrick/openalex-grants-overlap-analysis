import duckdb
from datetime import datetime
from pathlib import Path


def connect_to_database(db_path, read_only=False):
    return duckdb.connect(db_path, read_only=read_only)


def create_grants_schema(conn):
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
        FROM (SELECT * FROM '__temp_csv_data')
    """)


def create_metadata_table(conn):
    conn.execute("""
        CREATE TABLE db_metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR,
            created_at TIMESTAMP
        )
    """)


def create_indexes(conn):
    print("Creating indexes...")
    conn.execute("CREATE INDEX idx_grants_doi ON grants(doi)")
    conn.execute("CREATE INDEX idx_grants_funder ON grants(funder)")
    conn.execute("CREATE INDEX idx_grants_award ON grants(award_id)")
    conn.execute("CREATE INDEX idx_grants_funder_doi ON grants(funder, doi)")


def save_metadata(conn, grants_csv_path, total_rows, parsed_rows):
    conn.execute("""
        INSERT INTO db_metadata (key, value, created_at)
        VALUES 
            ('source_file', ?, NOW()),
            ('total_rows', ?, NOW()),
            ('parsed_rows', ?, NOW()),
            ('build_date', ?, NOW())
    """, [grants_csv_path, str(total_rows), str(parsed_rows), datetime.now().isoformat()])


def get_database_statistics(conn):
    stats = {}

    result = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT doi) as unique_dois,
            COUNT(DISTINCT funder) as unique_funders,
            COUNT(DISTINCT award_id) as unique_awards
        FROM grants
    """).fetchone()

    stats['total_records'] = result[0]
    stats['unique_dois'] = result[1]
    stats['unique_funders'] = result[2]
    stats['unique_awards'] = result[3]

    parsed_rows = conn.execute(
        "SELECT COUNT(*) FROM grants WHERE funder IS NOT NULL").fetchone()[0]
    stats['parsed_rows'] = parsed_rows

    return stats


def get_funder_statistics(conn, funder_id):
    result = conn.execute("""
        SELECT 
            COUNT(DISTINCT doi) as unique_dois,
            COUNT(DISTINCT award_id) as unique_awards,
            COUNT(*) as total_records
        FROM grants
        WHERE funder = ?
    """, [funder_id]).fetchone()

    return {
        'unique_dois': result[0],
        'unique_awards': result[1],
        'total_records': result[2]
    }


def get_top_funders(conn, limit=10):
    return conn.execute("""
        SELECT funder, COUNT(*) as count
        FROM grants
        WHERE funder IS NOT NULL
        GROUP BY funder
        ORDER BY count DESC
        LIMIT ?
    """, [limit]).fetchall()


def format_statistics_output(stats):
    lines = []
    lines.append("\nDatabase Statistics:")
    lines.append(f"  Total records: {stats.get('total_records', 0):,}")
    lines.append(f"  Unique DOIs: {stats.get('unique_dois', 0):,}")
    lines.append(f"  Unique funders: {stats.get('unique_funders', 0):,}")
    lines.append(f"  Unique awards: {stats.get('unique_awards', 0):,}")
    if 'parsed_rows' in stats:
        lines.append(f"  Rows with valid funder: {stats['parsed_rows']:,}")
    return "\n".join(lines)
