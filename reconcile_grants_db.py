import os
import sys
import argparse
import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
from collections import defaultdict
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
    extract_segments
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
            
            if stats.get('award_overlap_analysis'):
                stats_data.append(['AWARD OVERLAP ANALYSIS (OpenAlex not matched by DOI)', ''])
                overlap_stats = stats['award_overlap_analysis']
                stats_data.append(['Total not matched by DOI', f'{overlap_stats.get("total_not_matched_by_doi", 0):,}'])
                stats_data.append(['With award ID overlap', f'{overlap_stats.get("with_award_overlap", 0):,}'])
                stats_data.append(['Truly missing from input', f'{overlap_stats.get("truly_missing", 0):,}'])
                
                if 'overlap_match_types' in overlap_stats:
                    stats_data.append(['', ''])
                    stats_data.append(['Overlap Match Types:', ''])
                    for match_type, count in overlap_stats['overlap_match_types'].items():
                        stats_data.append([f'  {match_type}', f'{count:,}'])
                
                if 'records_without_doi' in stats['input_file_stats'] and stats['input_file_stats']['records_without_doi'] > 0:
                    stats_data.append(['', ''])
                    stats_data.append(['Note:', f"Award overlap includes {stats['input_file_stats']['records_without_doi']:,} entries without DOIs"])
                stats_data.append(['', ''])

            if stats.get('percentages'):
                stats_data.append(['PERCENTAGES (of entries WITH DOIs)', ''])
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


def unified_award_id_matching(oa_grants_df, input_with_doi_df, input_without_doi_df):
    print("Building unified inverted index for award ID matching...")
    
    oa_awards = oa_grants_df['award_id'].dropna().unique()
    print(f"Processing {len(oa_awards):,} unique OpenAlex awards")
    
    all_input_awards = []
    
    awards_with_doi = input_with_doi_df['award_id'].dropna().unique()
    all_input_awards.extend(awards_with_doi)
    
    if not input_without_doi_df.empty:
        awards_without_doi = input_without_doi_df['award_id'].dropna().unique()
        all_input_awards.extend(awards_without_doi)
        print(f"Including {len(awards_without_doi):,} awards from entries without DOIs")
    
    all_input_awards = list(set(all_input_awards))
    print(f"Total unique input awards to check: {len(all_input_awards):,}")
    
    input_index = defaultdict(set)
    for award in all_input_awards:
        segments = extract_segments(award)
        for seg in segments:
            if len(seg) > 2 or not seg.isdigit():
                input_index[seg].add(award)
    
    print(f"Matching {len(oa_awards):,} OpenAlex awards against {len(all_input_awards):,} input awards...")
    oa_to_input_matches = {}
    matched_count = 0
    
    for i, oa_award in enumerate(oa_awards):
        if (i + 1) % 1000 == 0:
            print(f"  Processed {i+1:,}/{len(oa_awards):,}...")
        
        candidates = set()
        segments = extract_segments(oa_award)
        for seg in segments:
            if seg in input_index:
                candidates.update(input_index[seg])
        
        for input_award in candidates:
            if awards_match(oa_award, input_award, match_types=['substring', 'normalized']):
                oa_to_input_matches[oa_award] = input_award
                matched_count += 1
                break
    
    print(f"Found {matched_count:,} OpenAlex awards with matches in input")
    
    oa_not_matched_by_doi = oa_grants_df[oa_grants_df['not_matched_by_doi'] == True].copy() if 'not_matched_by_doi' in oa_grants_df.columns else oa_grants_df.copy()
    
    oa_not_matched_by_doi['matching_input_award_id'] = oa_not_matched_by_doi['award_id'].map(oa_to_input_matches)
    oa_not_matched_by_doi['has_award_overlap'] = oa_not_matched_by_doi['matching_input_award_id'].notna()
    
    matched_rows = oa_not_matched_by_doi['has_award_overlap']
    if matched_rows.any():
        oa_not_matched_by_doi.loc[matched_rows, 'match_type'] = oa_not_matched_by_doi.loc[matched_rows].apply(
            lambda row: get_match_type(row['award_id'], row['matching_input_award_id']), axis=1
        )
        oa_not_matched_by_doi.loc[matched_rows, 'similarity_score'] = oa_not_matched_by_doi.loc[matched_rows].apply(
            lambda row: round(get_similarity_score(row['award_id'], row['matching_input_award_id']), 3), axis=1
        )
    
    overlap_from_no_doi = 0
    if not input_without_doi_df.empty:
        awards_without_doi_set = set(input_without_doi_df['award_id'].dropna().unique())
        overlap_from_no_doi = oa_not_matched_by_doi[
            oa_not_matched_by_doi['matching_input_award_id'].isin(awards_without_doi_set)
        ]['has_award_overlap'].sum()
    
    print(f"Total OpenAlex grants with award overlap: {oa_not_matched_by_doi['has_award_overlap'].sum():,}")
    if overlap_from_no_doi > 0:
        print(f"  (including {overlap_from_no_doi:,} matched via entries without DOIs)")
    
    return oa_not_matched_by_doi


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
            input_df['doi'] = input_df['doi'].replace('', pd.NA)

        print(f"Loaded {len(input_df):,} records from input file")
        
        entries_with_doi = input_df[input_df['doi'].notna()].copy()
        entries_without_doi = input_df[input_df['doi'].isna()].copy()
        
        print(f"  - Records with DOI: {len(entries_with_doi):,}")
        print(f"  - Records without DOI: {len(entries_without_doi):,}")

        conn.execute("DROP TABLE IF EXISTS input_data")
        conn.execute("CREATE TEMP TABLE input_data AS SELECT * FROM input_df")
        
        conn.execute("DROP TABLE IF EXISTS input_with_doi")
        conn.execute("CREATE TEMP TABLE input_with_doi AS SELECT * FROM entries_with_doi")
        
        conn.execute("DROP TABLE IF EXISTS input_without_doi")
        conn.execute("CREATE TEMP TABLE input_without_doi AS SELECT * FROM entries_without_doi")
        
        def awards_match_udf(id1, id2):
            return awards_match(id1, id2)

        def get_match_type_udf(id1, id2):
            return get_match_type(id1, id2)

        conn.create_function('awards_match', awards_match_udf, return_type=bool)
        conn.create_function('get_match_type', get_match_type_udf, return_type=str)
        
        conn.create_function('get_similarity_score', get_similarity_score, return_type=float)

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
            FROM input_with_doi i
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
            FROM input_with_doi i
            INNER JOIN grants g ON i.doi = g.doi AND g.funder = ?
            WHERE NOT awards_match(CAST(i.award_id AS VARCHAR), CAST(g.award_id AS VARCHAR))
        """, [funder_id]).df()

        with_neither = conn.execute("""
            SELECT DISTINCT i.*,
                   (SELECT work_id FROM grants g 
                    WHERE g.doi = i.doi 
                    LIMIT 1) as work_id
            FROM input_with_doi i
            WHERE NOT EXISTS (
                SELECT 1 FROM grants g 
                WHERE i.doi = g.doi AND g.funder = ?
            )
        """, [funder_id]).df()
        
        print("\nPerforming unified award ID matching...")
        
        all_oa_grants = conn.execute("""
            SELECT DISTINCT
                g.work_id,
                g.doi,
                g.award_id
            FROM grants g
            WHERE g.funder = ?
              AND g.award_id IS NOT NULL
        """, [funder_id]).df()
        
        dois_not_matched_raw = conn.execute("""
            SELECT DISTINCT g.doi
            FROM grants g
            LEFT JOIN input_with_doi i ON g.doi = i.doi
            WHERE g.funder = ?
              AND i.doi IS NULL
        """, [funder_id]).df()
        
        all_oa_grants['not_matched_by_doi'] = all_oa_grants['doi'].isin(dois_not_matched_raw['doi'])
        
        dois_not_in_input = unified_award_id_matching(
            all_oa_grants, 
            entries_with_doi, 
            entries_without_doi
        )
        
        dois_not_in_input = dois_not_in_input[dois_not_in_input['not_matched_by_doi'] == True].drop('not_matched_by_doi', axis=1)

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

    entries_with_doi_count = input_df['doi'].notna().sum() if 'doi' in input_df.columns else 0
    entries_without_doi_count = input_df['doi'].isna().sum() if 'doi' in input_df.columns else 0

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
    
    award_overlap_stats = {}
    if 'openalex_grants_not_in_funder' in results:
        df_not_in_funder = results['openalex_grants_not_in_funder']
        if not df_not_in_funder.empty and 'has_award_overlap' in df_not_in_funder.columns:
            total_not_by_doi = len(df_not_in_funder)
            with_overlap = df_not_in_funder[df_not_in_funder['has_award_overlap'] == True]
            overlap_count = len(with_overlap)
            
            award_overlap_stats = {
                'total_not_matched_by_doi': total_not_by_doi,
                'with_award_overlap': overlap_count,
                'truly_missing': total_not_by_doi - overlap_count
            }
            
            if overlap_count > 0 and 'match_type' in with_overlap.columns:
                overlap_match_types = with_overlap['match_type'].value_counts().to_dict()
                award_overlap_stats['overlap_match_types'] = {
                    'exact': overlap_match_types.get('exact', 0),
                    'substring': overlap_match_types.get('substring', 0),
                    'normalized': overlap_match_types.get('normalized', 0),
                    'fuzzy': overlap_match_types.get('fuzzy', 0)
                }
    
    stats = {
        'timestamp': datetime.now().isoformat(),
        'funder_id': funder_id,
        'input_file_stats': {
            'total_records': len(input_df),
            'records_with_doi': entries_with_doi_count,
            'records_without_doi': entries_without_doi_count,
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
        'award_overlap_analysis': award_overlap_stats,
        'percentages': {}
    }

    total_input = len(input_df)
    if total_input > 0:
        if entries_with_doi_count > 0:
            stats['percentages']['pct_work_and_award_matched'] = (
                100.0 *
                len(results.get('funder_work_and_grant_id_match_in_openalex', [])) / entries_with_doi_count
            )
            stats['percentages']['pct_work_matched_award_differs'] = (
                100.0 *
                len(results.get(
                    'funder_work_matched_in_openalex_grant_id_differs', [])) / entries_with_doi_count
            )
            stats['percentages']['pct_records_not_in_openalex'] = (
                100.0 *
                len(results.get('funder_grants_not_in_openalex', [])) / entries_with_doi_count
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
    
    if stats.get('award_overlap_analysis'):
        print("\nAward Overlap Analysis (OpenAlex grants not matched by DOI):")
        overlap_stats = stats['award_overlap_analysis']
        print(f"  Total not matched by DOI: {overlap_stats.get('total_not_matched_by_doi', 0):,}")
        print(f"  With award ID overlap: {overlap_stats.get('with_award_overlap', 0):,}")
        print(f"  Truly missing from input: {overlap_stats.get('truly_missing', 0):,}")
        
        if 'overlap_match_types' in overlap_stats:
            print("\n  Overlap match types:")
            for match_type, count in overlap_stats['overlap_match_types'].items():
                print(f"    {match_type}: {count:,}")
        
        if 'records_without_doi' in stats['input_file_stats'] and stats['input_file_stats']['records_without_doi'] > 0:
            print(f"\n  Note: Award overlap includes {stats['input_file_stats']['records_without_doi']:,} entries without DOIs")

    if stats['percentages']:
        print("\nPercentages (of entries WITH DOIs):")
        for key, value in stats['percentages'].items():
            label = key.replace('pct_', '').replace('_', ' ').title()
            print(f"  {label}: {value:.2f}%")

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
        
        if stats.get('award_overlap_analysis'):
            f.write("Award Overlap Analysis (OpenAlex grants not matched by DOI):\n")
            overlap_stats = stats['award_overlap_analysis']
            f.write(f"  Total not matched by DOI: {overlap_stats.get('total_not_matched_by_doi', 0):,}\n")
            f.write(f"  With award ID overlap: {overlap_stats.get('with_award_overlap', 0):,}\n")
            f.write(f"  Truly missing from input: {overlap_stats.get('truly_missing', 0):,}\n")
            
            if 'overlap_match_types' in overlap_stats:
                f.write("\n  Overlap match types:\n")
                for match_type, count in overlap_stats['overlap_match_types'].items():
                    f.write(f"    {match_type}: {count:,}\n")
            
            if 'records_without_doi' in stats['input_file_stats'] and stats['input_file_stats']['records_without_doi'] > 0:
                f.write(f"\n  Note: Award overlap includes {stats['input_file_stats']['records_without_doi']:,} entries without DOIs\n")
            f.write("\n")

        if stats['percentages']:
            f.write("Percentages (of entries WITH DOIs):\n")
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