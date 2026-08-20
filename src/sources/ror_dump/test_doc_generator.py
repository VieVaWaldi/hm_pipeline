"""
ROR Schema Documentation Generator
Produces comprehensive schema docs from actual DuckDB data
"""

import duckdb
import json
from pathlib import Path
from typing import Dict, Any

ROR_DB = Path("/work/lu72hip/data/duckdb/sources/ror_raw.duckdb")


def get_table_schema(con: duckdb.DuckDBPyConnection, table: str) -> Dict[str, Any]:
    """Get detailed schema info for a table"""
    # Get column info using DESCRIBE
    schema = con.execute(f"DESCRIBE {table}").df()

    # Get sample values to understand nested structures better
    sample = con.execute(f"SELECT * FROM {table} LIMIT 1").fetchone()

    return {"columns": schema.to_dict("records"), "sample": sample}


def analyze_nested_field(
    con: duckdb.DuckDBPyConnection, field: str, sample_size: int = 100
) -> Dict:
    """Analyze a nested field (LIST/STRUCT) to understand its structure"""

    # Get length distribution for LIST fields
    if "LIST" in field.upper():
        lengths = con.execute(f"""
            SELECT 
                len({field}) as length,
                COUNT(*) as count
            FROM organizations
            WHERE {field} IS NOT NULL
            GROUP BY length
            ORDER BY count DESC
            LIMIT 10
        """).df()

        # Get a few non-null samples
        samples = con.execute(f"""
            SELECT {field}
            FROM organizations
            WHERE {field} IS NOT NULL 
                AND len({field}) > 0
            LIMIT 3
        """).fetchall()

        return {
            "length_distribution": lengths.to_dict("records"),
            "samples": [s[0] for s in samples],
        }

    return {}


def main():
    con = duckdb.connect(str(ROR_DB), read_only=True)

    print("=" * 80)
    print("ROR DUCKDB SCHEMA DOCUMENTATION")
    print("=" * 80)
    print()

    # List all tables
    tables = con.execute("SHOW TABLES").df()
    print(f"Tables in database: {tables['name'].tolist()}")
    print()

    # For each table, get schema
    for table_name in tables["name"]:
        print("=" * 80)
        print(f"TABLE: {table_name}")
        print("=" * 80)

        # Get schema
        schema_info = get_table_schema(con, table_name)

        print("\nCOLUMNS:")
        print("-" * 80)
        for col in schema_info["columns"]:
            print(
                f"  {col['column_name']:20} {col['column_type']:30} {'NULL' if col['null'] == 'YES' else 'NOT NULL'}"
            )

        print("\n\nFIELD DETAILS:")
        print("-" * 80)

        # Analyze each column
        for col in schema_info["columns"]:
            col_name = col["column_name"]
            col_type = col["column_type"]

            print(f"\n{col_name} ({col_type}):")

            # Count non-null
            non_null = con.execute(f"""
                SELECT COUNT(*) 
                FROM {table_name} 
                WHERE {col_name} IS NOT NULL
            """).fetchone()[0]

            total = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            pct = (non_null / total * 100) if total > 0 else 0

            print(f"  Populated: {non_null:,} / {total:,} ({pct:.1f}%)")

            # For nested types, analyze structure
            if "STRUCT" in col_type or "LIST" in col_type:
                print(f"  Structure analysis:")

                # Get a sample to show the actual structure
                sample = con.execute(f"""
                    SELECT {col_name}
                    FROM {table_name}
                    WHERE {col_name} IS NOT NULL
                    LIMIT 1
                """).fetchone()

                if sample and sample[0]:
                    print(f"  Example value:")
                    print(
                        f"    {json.dumps(sample[0], indent=4, default=str)[:500]}..."
                    )

                # For lists, show length distribution
                if "LIST" in col_type:
                    try:
                        lengths = con.execute(f"""
                            SELECT 
                                len({col_name}) as length,
                                COUNT(*) as count
                            FROM {table_name}
                            WHERE {col_name} IS NOT NULL
                            GROUP BY length
                            ORDER BY count DESC
                            LIMIT 5
                        """).df()

                        if not lengths.empty:
                            print(f"  Length distribution:")
                            for _, row in lengths.iterrows():
                                print(
                                    f"    {row['length']} items: {row['count']:,} records"
                                )
                    except Exception as e:
                        print(f"  Could not analyze length: {e}")

        print("\n")

    # Additional ROR-specific analysis
    print("=" * 80)
    print("ROR-SPECIFIC ANALYSIS")
    print("=" * 80)

    print("\n1. ORGANIZATION TYPES")
    print("-" * 80)
    # Get unique type combinations
    type_combos = con.execute("""
        SELECT 
            list_sort(types) as type_combo,
            COUNT(*) as count
        FROM organizations
        GROUP BY type_combo
        ORDER BY count DESC
        LIMIT 20
    """).df()
    print(type_combos.to_string(index=False))

    print("\n\n2. RELATIONSHIP TYPES")
    print("-" * 80)
    # Sample relationships to understand structure
    rel_sample = con.execute("""
        SELECT relationships
        FROM organizations
        WHERE relationships IS NOT NULL 
            AND len(relationships) > 0
        LIMIT 5
    """).fetchall()

    print("Sample relationships:")
    for i, (rels,) in enumerate(rel_sample, 1):
        print(f"\nExample {i}:")
        for rel in rels[:2]:  # Show first 2 relationships
            print(f"  - Type: {rel['type']}, Label: {rel['label']}")

    print("\n\n3. EXTERNAL ID SYSTEMS")
    print("-" * 80)
    # Get all unique external ID types
    ext_id_sample = con.execute("""
        SELECT external_ids
        FROM organizations
        WHERE external_ids IS NOT NULL 
            AND len(external_ids) > 0
        LIMIT 10
    """).fetchall()

    id_types = set()
    for (ext_ids,) in ext_id_sample:
        for ext_id in ext_ids:
            id_types.add(ext_id["type"])

    print(f"External ID systems found: {sorted(id_types)}")

    print("\n\n4. NAME TYPES")
    print("-" * 80)
    # Sample name structures
    name_sample = con.execute("""
        SELECT names
        FROM organizations
        WHERE names IS NOT NULL 
            AND len(names) > 1
        LIMIT 3
    """).fetchall()

    print("Organizations with multiple names:")
    for i, (names,) in enumerate(name_sample, 1):
        print(f"\nExample {i}: {len(names)} names")
        for name in names[:3]:  # Show first 3 names
            name_types = name.get("types", [])
            print(
                f"  - '{name['value']}' [{', '.join(name_types) if name_types else 'no type'}]"
            )

    print("\n" + "=" * 80)
    print("✓ SCHEMA DOCUMENTATION COMPLETE")
    print("=" * 80)

    con.close()


if __name__ == "__main__":
    main()
