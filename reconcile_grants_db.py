import os
import sys
import argparse
import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
from grants_db_common import (
    connect_to_database,
    get_database_statistics,
    get_funder_statistics,
    get_top_funders,
    format_statistics_output
)
from utils.award_id_matcher import (
    awards_match,
    get_match_type,
    get_similarity_score,
    normalize_award_id,
    check_substring_match,
    check_normalized_match
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Grant reconciliation using DuckDB database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Query existing database with input file:
  %(prog)s query --db grants.db -i input.csv -f https://openalex.org/F4320306577
  
  # Show database information:
  %(prog)s info --db grants.db
  
  Note: To build the database, use build_grants_db.py
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    query_parser = subparsers.add_parser(
        'query', help='Query existing database with input file')
    query_parser.add_argument('--db', required=True,
                              help='Path to existing database file')
    query_parser.add_argument(
        '-i', '--input-file', required=True, help='Path to input funding CSV file')
    query_parser.add_argument('-f', '--funder-id', required=True,
                              help='OpenAlex Funder ID to match (e.g., https://openalex.org/F4320306577)')
    query_parser.add_argument('-a', '--award-field', default='award_id',
                              help='Column name for award ID in input file (default: award_id)')
    query_parser.add_argument('-o', '--output-dir', default='output',
                              help='Directory for output files (default: output)')
    query_parser.add_argument(
        '-v', '--verbose', action='store_true', help='Verbose output')
    query_parser.add_argument('-e', '--excel', action='store_true',
                              help='Create consolidated Excel file with all output categories')

    info_parser = subparsers.add_parser(
        'info', help='Show database information')
    info_parser.add_argument('--db', required=True,
                             help='Path to database file')

    return parser.parse_args()


def create_excel_report(results, input_file, output_dir, stats=None):
    input_basename = Path(input_file).stem
    excel_file = Path(output_dir) / f"{input_basename}_grants_overlap_analysis.xlsx"

    sheet_names = {
        'funder_work_and_grant_id_match_in_openalex': 'funder_work_and_grant_id_match',
        'funder_work_matched_in_openalex_grant_id_differs': 'funder_work_matched_grant_differs',
        'funder_grants_not_in_openalex': 'funder_grants_not_in_openalex',
        'openalex_grants_not_in_funder': 'openalex_grants_not_in_funder'
    }

    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        if stats:
            stats_data = []

            stats_data.append(['RECONCILIATION STATISTICS', ''])
            stats_data.append(['', ''])
            stats_data.append(['Generated', stats['timestamp']])
            stats_data.append(['Input File', input_file])
            stats_data.append(['Funder ID', stats['funder_id']])
            stats_data.append(['', ''])

            stats_data.append(['INPUT FILE STATISTICS', ''])
            for key, value in stats['input_file_stats'].items():
                label = key.replace('_', ' ').title()
                stats_data.append([label, f'{value:,}'])
            stats_data.append(['', ''])

            stats_data.append(
                ['OPENALEX GRANTS STATISTICS (for this funder)', ''])
            for key, value in stats['grants_db_stats'].items():
                label = key.replace('_', ' ').title().replace('Funder ', '')
                stats_data.append([label, f'{value:,}'])
            stats_data.append(['', ''])

            stats_data.append(['RECONCILIATION RESULTS', ''])
            for key, value in stats['reconciliation_results'].items():
                label = key.replace('_', ' ').title()
                stats_data.append([label, f'{value:,}'])
            stats_data.append(['', ''])
            
            if stats.get('match_type_breakdown'):
                stats_data.append(['MATCH TYPE BREAKDOWN', ''])
                for key, value in stats['match_type_breakdown'].items():
                    label = key.replace('_', ' ').title()
                    stats_data.append([label, f'{value:,}'])
                stats_data.append(['', ''])

            if stats.get('percentages'):
                stats_data.append(['PERCENTAGES (of input file)', ''])
                for key, value in stats['percentages'].items():
                    label = key.replace('pct_', '').replace('_', ' ').title()
                    stats_data.append([label, f'{value:.2f}%'])

            stats_df = pd.DataFrame(stats_data, columns=['Metric', 'Value'])
            stats_df.to_excel(
                writer, sheet_name='Statistics Summary', index=False)

        for category, df in results.items():
            sheet_name = sheet_names.get(category, category)[:31]
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    return excel_file


def query_database(db_path, input_file, funder_id, award_field='award_id',
                   output_dir='output', verbose=False, excel=False):

    if not os.path.exists(db_path):
        print(f"Error: Database file not found: {db_path}")
        return False

    print(f"Connecting to database: {db_path}")
    conn = connect_to_database(db_path, read_only=False)

    try:
        print(f"Loading input file: {input_file}")
        input_df = pd.read_csv(input_file)

        if award_field in input_df.columns and award_field != 'award_id':
            input_df.rename(columns={award_field: 'award_id'}, inplace=True)

        if 'doi' in input_df.columns:
            input_df['doi'] = input_df['doi'].str.lower().str.strip()

        print(f"Loaded {len(input_df):,} records from input file")

        conn.execute("DROP TABLE IF EXISTS input_data")
        conn.execute("CREATE TEMP TABLE input_data AS SELECT * FROM input_df")
        
        conn.create_function('awards_match', awards_match, return_type=bool)
        conn.create_function('get_match_type', get_match_type, return_type=str)
        conn.create_function('get_similarity_score', get_similarity_score, return_type=float)
        conn.create_function('normalize_award_id', normalize_award_id, return_type=str)
        conn.create_function('check_substring_match', check_substring_match, return_type=bool)
        conn.create_function('check_normalized_match', check_normalized_match, return_type=bool)

        funder_stats = get_funder_statistics(conn, funder_id)

        if funder_stats['total_records'] == 0:
            print(f"Warning: No records found for funder {funder_id} in database")
        else:
            print(f"Found {funder_stats['total_records']:,} records for funder {funder_id}")
            print(f"  Unique DOIs: {funder_stats['unique_dois']:,}")
            print(f"  Unique awards: {funder_stats['unique_awards']:,}")

        print("\nPerforming reconciliation...")

        with_both = conn.execute("""
            SELECT DISTINCT 
                i.*,
                i.award_id as funder_award_id,
                g.award_id as openalex_award_id,
                g.work_id,
                get_match_type(CAST(i.award_id AS VARCHAR), CAST(g.award_id AS VARCHAR)) as match_type,
                ROUND(get_similarity_score(CAST(i.award_id AS VARCHAR), CAST(g.award_id AS VARCHAR)), 3) as similarity_score
            FROM input_data i
            INNER JOIN grants g ON i.doi = g.doi 
                AND awards_match(CAST(i.award_id AS VARCHAR), CAST(g.award_id AS VARCHAR))
                AND g.funder = ?
        """, [funder_id]).df()

        with_funder_only = conn.execute("""
            SELECT DISTINCT 
                i.*,
                i.award_id as funder_award_id,
                g.award_id as openalex_award_id,
                g.work_id,
                CASE 
                    WHEN i.award_id IS NULL OR g.award_id IS NULL THEN 'missing'
                    ELSE 'no_match'
                END as match_type,
                ROUND(get_similarity_score(CAST(i.award_id AS VARCHAR), CAST(g.award_id AS VARCHAR)), 3) as similarity_score
            FROM input_data i
            INNER JOIN grants g ON i.doi = g.doi AND g.funder = ?
            WHERE NOT awards_match(CAST(i.award_id AS VARCHAR), CAST(g.award_id AS VARCHAR))
        """, [funder_id]).df()

        with_neither = conn.execute("""
            SELECT DISTINCT i.*,
                   (SELECT work_id FROM grants g 
                    WHERE g.doi = i.doi 
                    LIMIT 1) as work_id
            FROM input_data i
            WHERE NOT EXISTS (
                SELECT 1 FROM grants g 
                WHERE i.doi = g.doi AND g.funder = ?
            )
        """, [funder_id]).df()

        dois_not_in_input = conn.execute("""
            SELECT DISTINCT 
                g.work_id,
                g.doi,
                g.award_id
            FROM grants g
            LEFT JOIN input_data i ON g.doi = i.doi
            WHERE g.funder = ?
                AND i.doi IS NULL
        """, [funder_id]).df()

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        input_basename = Path(input_file).stem
        
        if 'award_id' in with_both.columns and 'funder_award_id' in with_both.columns:
            with_both = with_both.drop('award_id', axis=1)

        results = {
            'funder_work_and_grant_id_match_in_openalex': with_both,
            'funder_work_matched_in_openalex_grant_id_differs': with_funder_only,
            'funder_grants_not_in_openalex': with_neither,
            'openalex_grants_not_in_funder': dois_not_in_input
        }

        print("\nReconciliation Results:")
        for category, df in results.items():
            if not df.empty:
                filename = f"{input_basename}_{category}_{timestamp}.csv"
                filepath = Path(output_dir) / filename
                df.to_csv(filepath, index=False)
                print(f"  {category}: {len(df):,} records -> {filepath}")

        stats = generate_statistics(conn, input_df, results, funder_id)
        print_statistics(stats)

        stats_file = Path(output_dir) / f"reconciliation_stats_{input_basename}_{timestamp}.txt"
        save_statistics(stats, stats_file, input_file)

        if excel:
            excel_file = create_excel_report(
                results, input_file, output_dir, stats)
            print(f"\nExcel report created: {excel_file}")

        print(f"\nReconciliation complete! Results saved to {output_dir}")
        return True

    except Exception as e:
        print(f"Error during query: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return False
    finally:
        conn.close()


def show_database_info(db_path):
    if not os.path.exists(db_path):
        print(f"Error: Database file not found: {db_path}")
        return False

    print(f"Database: {db_path}")
    print(f"File size: {os.path.getsize(db_path) / (1024*1024):.2f} MB")
    print()

    conn = connect_to_database(db_path, read_only=True)

    try:
        metadata = conn.execute(
            "SELECT key, value FROM db_metadata").fetchall()
        if metadata:
            print("Database Metadata:")
            for key, value in metadata:
                print(f"  {key}: {value}")
            print()

        stats = get_database_statistics(conn)
        print(format_statistics_output(stats))
        print()

        print("Top 10 Funders by Record Count:")
        top_funders = get_top_funders(conn, limit=10)

        for funder, count in top_funders:
            print(f"  {funder}: {count:,}")

        return True

    except Exception as e:
        print(f"Error reading database: {e}")
        return False
    finally:
        conn.close()


def generate_statistics(conn, input_df, results, funder_id):
    funder_stats = get_funder_statistics(conn, funder_id)

    match_type_breakdown = {}
    if 'funder_work_and_grant_id_match_in_openalex' in results:
        df_matched = results['funder_work_and_grant_id_match_in_openalex']
        if not df_matched.empty and 'match_type' in df_matched.columns:
            match_type_counts = df_matched['match_type'].value_counts().to_dict()
            match_type_breakdown = {
                'exact_matches': match_type_counts.get('exact', 0),
                'substring_matches': match_type_counts.get('substring', 0),
                'normalized_matches': match_type_counts.get('normalized', 0),
                'fuzzy_matches': match_type_counts.get('fuzzy', 0)
            }

    stats = {
        'timestamp': datetime.now().isoformat(),
        'funder_id': funder_id,
        'input_file_stats': {
            'total_records': len(input_df),
            'unique_dois': input_df['doi'].nunique() if 'doi' in input_df.columns else 0,
            'unique_award_ids': input_df['award_id'].nunique() if 'award_id' in input_df.columns else 0
        },
        'grants_db_stats': {
            'funder_unique_dois': funder_stats['unique_dois'],
            'funder_unique_awards': funder_stats['unique_awards'],
            'funder_total_mappings': funder_stats['total_records']
        },
        'reconciliation_results': {
            'funder_work_and_grant_id_match_in_openalex': len(results.get('funder_work_and_grant_id_match_in_openalex', [])),
            'funder_work_matched_in_openalex_grant_id_differs': len(results.get('funder_work_matched_in_openalex_grant_id_differs', [])),
            'funder_grants_not_in_openalex': len(results.get('funder_grants_not_in_openalex', [])),
            'openalex_grants_not_in_funder': len(results.get('openalex_grants_not_in_funder', []))
        },
        'match_type_breakdown': match_type_breakdown,
        'percentages': {}
    }

    total_input = len(input_df)
    if total_input > 0:
        stats['percentages']['pct_work_and_award_matched'] = (
            100.0 *
            len(results.get('funder_work_and_grant_id_match_in_openalex', [])) / total_input
        )
        stats['percentages']['pct_work_matched_award_differs'] = (
            100.0 *
            len(results.get(
                'funder_work_matched_in_openalex_grant_id_differs', [])) / total_input
        )
        stats['percentages']['pct_records_not_in_openalex'] = (
            100.0 *
            len(results.get('funder_grants_not_in_openalex', [])) / total_input
        )

    return stats


def print_statistics(stats):
    print("\n" + "="*60)
    print("RECONCILIATION STATISTICS")
    print("="*60)
    print(f"Timestamp: {stats['timestamp']}")
    print(f"Funder ID: {stats['funder_id']}")

    print("\nInput File Statistics:")
    for key, value in stats['input_file_stats'].items():
        print(f"  {key}: {value:,}")

    print("\nGrants Database Statistics (for this funder):")
    for key, value in stats['grants_db_stats'].items():
        print(f"  {key}: {value:,}")

    print("\nReconciliation Results:")
    for key, value in stats['reconciliation_results'].items():
        print(f"  {key}: {value:,}")
    
    if stats['match_type_breakdown']:
        print("\nMatch Type Breakdown (for work and grant ID matches):")
        for key, value in stats['match_type_breakdown'].items():
            print(f"  {key}: {value:,}")

    if stats['percentages']:
        print("\nPercentages (of input file):")
        for key, value in stats['percentages'].items():
            print(f"  {key}: {value:.2f}%")

    print("="*60)


def save_statistics(stats, stats_file, input_file):
    with open(stats_file, 'w') as f:
        f.write("GRANT RECONCILIATION STATISTICS\n")
        f.write("="*60 + "\n")
        f.write(f"Generated: {stats['timestamp']}\n")
        f.write(f"Input file: {input_file}\n")
        f.write(f"Funder ID: {stats['funder_id']}\n")
        f.write("\n")

        f.write("Input File Statistics:\n")
        for key, value in stats['input_file_stats'].items():
            f.write(f"  {key}: {value:,}\n")
        f.write("\n")

        f.write("Grants Database Statistics (for this funder):\n")
        for key, value in stats['grants_db_stats'].items():
            f.write(f"  {key}: {value:,}\n")
        f.write("\n")

        f.write("Reconciliation Results:\n")
        for key, value in stats['reconciliation_results'].items():
            f.write(f"  {key}: {value:,}\n")
        f.write("\n")
        
        if stats.get('match_type_breakdown'):
            f.write("Match Type Breakdown (for work and grant ID matches):\n")
            for key, value in stats['match_type_breakdown'].items():
                f.write(f"  {key}: {value:,}\n")
            f.write("\n")

        if stats['percentages']:
            f.write("Percentages (of input file):\n")
            for key, value in stats['percentages'].items():
                f.write(f"  {key}: {value:.2f}%\n")

    print(f"Statistics saved to: {stats_file}")


def main():
    args = parse_arguments()

    if not args.command:
        print("Error: Please specify a command (query or info)")
        print("Run with --help for usage information")
        print("\nTo build a database, use: build_grants_db.py")
        return 1

    if args.command == 'query':
        success = query_database(
            args.db,
            args.input_file,
            args.funder_id,
            args.award_field,
            args.output_dir,
            args.verbose,
            args.excel
        )
        return 0 if success else 1

    elif args.command == 'info':
        success = show_database_info(args.db)
        return 0 if success else 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
