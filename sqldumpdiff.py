import re
import sys
import io
import csv

# Regex Patterns
# Note: INSERT statements are assumed to be single-line. Multi-line INSERT statements
# may not be parsed correctly.
TABLE_NAME_RE = re.compile(r"CREATE TABLE `(.+?)`", re.IGNORECASE)
INSERT_RE = re.compile(r"INSERT INTO `(.+?)` \((.+?)\) VALUES \((.+?)\);", re.IGNORECASE)

def get_table_schemas(filepath):
    """Parses the SQL file to find Primary Keys (including composite).
    Handles multi-line CREATE TABLE statements."""
    schema_map = {}
    current_table = None
    table_content = []
    in_create_table = False
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            # Check if this line starts a CREATE TABLE
            table_match = TABLE_NAME_RE.search(line)
            if table_match:
                # If we were already in a table, process it first
                if in_create_table and current_table:
                    _process_table_definition(current_table, ''.join(table_content), schema_map)
                current_table = table_match.group(1)
                table_content = [line]
                in_create_table = True
                continue
            
            # If we're inside a CREATE TABLE, accumulate lines
            if in_create_table:
                table_content.append(line)
                # Check if this line ends the CREATE TABLE statement
                if line.strip().endswith(';'):
                    if current_table:
                        _process_table_definition(current_table, ''.join(table_content), schema_map)
                    current_table = None
                    table_content = []
                    in_create_table = False
    
    # Handle case where file ends without semicolon
    if in_create_table and current_table:
        _process_table_definition(current_table, ''.join(table_content), schema_map)
    
    return schema_map

def _process_table_definition(table_name, content, schema_map):
    """Extracts PRIMARY KEY from a CREATE TABLE statement (may be multi-line)."""
    # Use DOTALL flag to allow . to match newlines
    pk_match = re.search(r'PRIMARY\s+KEY\s*\((.+?)\)', content, re.IGNORECASE | re.DOTALL)
    if pk_match:
        # Extract columns, handling multi-line and whitespace
        pk_def = pk_match.group(1)
        # Remove newlines and normalize whitespace
        pk_def = ' '.join(pk_def.split())
        # Extract column names
        cols = [c.strip(' `') for c in pk_def.split(',')]
        schema_map[table_name] = cols

def parse_sql_values(values_str):
    """Handles CSV-style parsing of SQL values, respecting quotes and escapes."""
    f = io.StringIO(values_str)
    reader = csv.reader(f, quotechar="'", skipinitialspace=True, escapechar='\\')
    try:
        return next(reader)
    except StopIteration:
        return []

def get_row_data(line):
    """Extracts table name, columns, and a data dictionary from an INSERT statement."""
    match = INSERT_RE.search(line)
    if not match: return None, None, None
    table = match.group(1)
    columns = [c.strip(' `') for c in match.group(2).split(',')]
    values = parse_sql_values(match.group(3))
    # Safety check: ensure columns and values match in count
    if len(columns) != len(values):
        return None, None, None
    return table, columns, dict(zip(columns, values))

def normalize_null(val):
    """Normalizes None and 'NULL' string to None for consistent comparison."""
    if val is None:
        return None
    if isinstance(val, str) and val.upper() == 'NULL':
        return None
    return val

def build_where_clause(pk_cols, data):
    """Builds a SQL WHERE clause handling composite keys and NULL values."""
    where_parts = []
    for c in pk_cols:
        val = data.get(c)
        if val is None or (isinstance(val, str) and val.upper() == 'NULL'):
            where_parts.append(f"`{c}` IS NULL")
        else:
            safe_val = str(val).replace("'", "''")
            where_parts.append(f"`{c}`='{safe_val}'")
    return " AND ".join(where_parts)

def generate_delta(old_file, new_file, delta_out):
    print("Step 1: Mapping Schemas from DDL...")
    pk_map = get_table_schemas(new_file)

    # 2. Index Old Data
    old_records = {} 
    print("Step 2: Indexing old records...")
    with open(old_file, 'r', encoding='utf-8') as f:
        for line in f:
            table, cols, data = get_row_data(line)
            if table and table in pk_map:
                pk_cols = pk_map[table]
                # Skip if any primary key column is missing from the data
                if not all(c in data for c in pk_cols):
                    continue
                # Tuple used as dict key for composite key support
                pk_values = tuple(data.get(c) for c in pk_cols)
                old_records[(table, pk_values)] = data

    matched_old_keys = set()

    # 3. Compare for Inserts and Updates
    print("Step 3: Comparing for Updates and Inserts...")
    with open(new_file, 'r', encoding='utf-8') as f_new, \
         open(delta_out, 'w', encoding='utf-8') as f_out:
        
        f_out.write("-- Full Delta Update Script\n")
        f_out.write("SET FOREIGN_KEY_CHECKS = 0;\n\n")

        for line in f_new:
            table, cols, new_data = get_row_data(line)
            if not table or table not in pk_map:
                continue

            pk_cols = pk_map[table]
            # Skip if any primary key column is missing from the data
            if not all(c in new_data for c in pk_cols):
                continue
            pk_values = tuple(new_data.get(c) for c in pk_cols)
            old_data = old_records.get((table, pk_values))

            if not old_data:
                # This record is entirely new
                f_out.write(f"-- NEW RECORD IN {table}\n{line}\n")
            else:
                matched_old_keys.add((table, pk_values))
                updates = []
                comments = []
                for col in cols:
                    ov, nv = old_data.get(col), new_data.get(col)
                    # Normalize NULL values for proper comparison
                    ov_norm, nv_norm = normalize_null(ov), normalize_null(nv)
                    if ov_norm != nv_norm:
                        comments.append(f"-- {col} old value: {ov}")
                        if nv_norm is None:
                            updates.append(f"`{col}`=NULL")
                        else:
                            safe_nv = str(nv).replace("'", "''")
                            updates.append(f"`{col}`='{safe_nv}'")

                if updates:
                    where_str = build_where_clause(pk_cols, new_data)
                    f_out.writelines(c + "\n" for c in comments)
                    f_out.write(f"UPDATE `{table}` SET {', '.join(updates)} WHERE {where_str};\n\n")

        # 4. Handle Deletions
        print("Step 4: Identifying Deletions...")
        f_out.write("-- DELETIONS\n")
        for key, old_data in old_records.items():
            if key not in matched_old_keys:
                table, pk_values = key
                pk_cols = pk_map[table]
                where_str = build_where_clause(pk_cols, old_data)
                f_out.write(f"-- DELETED FROM {table}: {pk_values}\n")
                f_out.write(f"DELETE FROM `{table}` WHERE {where_str};\n\n")

        f_out.write("SET FOREIGN_KEY_CHECKS = 1;\n")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python sqldiff.py <old_dump.sql> <new_dump.sql>")
    else:
        generate_delta(sys.argv[1], sys.argv[2], "full_delta_update.sql")
        print("Done! Check 'full_delta_update.sql' for the results.")
