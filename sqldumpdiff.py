import re
import sys
import io
import csv
import os

# Regex Patterns
# Note: Both CREATE TABLE and INSERT statements now support multi-line parsing.
# Note: INSERT_RE only matches single-row INSERTs. Multi-row INSERTs (VALUES (...), (...))
# are not currently supported and will be skipped.
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

def parse_insert_statements(file_handle):
    """Generator that yields complete INSERT statements (handles multi-line).
    Yields the full INSERT statement line(s) as a string.
    Properly handles quoted strings and nested parentheses."""
    current_insert = None
    in_insert = False
    paren_depth = 0
    in_single_quote = False
    in_double_quote = False
    escape_next = False
    
    for line in file_handle:
        # Check if this line starts an INSERT statement
        if not in_insert and 'INSERT INTO' in line.upper():
            current_insert = line
            in_insert = True
            # Reset quote and escape state
            in_single_quote = False
            in_double_quote = False
            escape_next = False
            paren_depth = 0
            
            # Process the line to track state
            for char in line:
                if escape_next:
                    escape_next = False
                    continue
                if char == '\\':
                    escape_next = True
                    continue
                if char == "'" and not in_double_quote:
                    in_single_quote = not in_single_quote
                elif char == '"' and not in_single_quote:
                    in_double_quote = not in_double_quote
                elif not in_single_quote and not in_double_quote:
                    if char == '(':
                        paren_depth += 1
                    elif char == ')':
                        paren_depth -= 1
            
            # If statement is complete on one line
            if line.strip().endswith(';') and paren_depth == 0:
                yield current_insert
                current_insert = None
                in_insert = False
            continue
        
        # If we're accumulating an INSERT statement
        if in_insert:
            current_insert += line
            # Process the line to track quote state and parentheses
            for char in line:
                if escape_next:
                    escape_next = False
                    continue
                if char == '\\':
                    escape_next = True
                    continue
                if char == "'" and not in_double_quote:
                    in_single_quote = not in_single_quote
                elif char == '"' and not in_single_quote:
                    in_double_quote = not in_double_quote
                elif not in_single_quote and not in_double_quote:
                    if char == '(':
                        paren_depth += 1
                    elif char == ')':
                        paren_depth -= 1
            
            # Check if statement is complete (ends with ; and parentheses balanced)
            if line.strip().endswith(';') and paren_depth == 0:
                yield current_insert
                current_insert = None
                in_insert = False
                in_single_quote = False
                in_double_quote = False
                escape_next = False
    
    # Handle case where file ends without semicolon (shouldn't happen in dumps, but be safe)
    if in_insert and current_insert:
        yield current_insert

def get_row_data(insert_statement):
    """Extracts table name, columns, and a data dictionary from an INSERT statement.
    Now accepts a complete INSERT statement (may be multi-line).
    Returns: (table, columns, data_dict, original_stmt) or (None, None, None, None) on error."""
    # Normalize whitespace for regex matching (but preserve the original for output)
    normalized = ' '.join(insert_statement.split())
    match = INSERT_RE.search(normalized)
    if not match: 
        return None, None, None, None
    table = match.group(1)
    columns = [c.strip(' `') for c in match.group(2).split(',')]
    values = parse_sql_values(match.group(3))
    # Safety check: ensure columns and values match in count
    if len(columns) != len(values):
        return None, None, None, None
    return table, columns, dict(zip(columns, values)), insert_statement

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
    """Generates a delta SQL script comparing two SQL dump files."""
    # Validate input files exist
    if not os.path.exists(old_file):
        raise FileNotFoundError(f"Old file not found: {old_file}")
    if not os.path.exists(new_file):
        raise FileNotFoundError(f"New file not found: {new_file}")
    
    print("Step 1: Mapping Schemas from DDL...")
    try:
        pk_map = get_table_schemas(new_file)
    except Exception as e:
        raise RuntimeError(f"Error parsing schema from {new_file}: {e}") from e
    
    if not pk_map:
        print("Warning: No tables with PRIMARY KEY found in new file schema.")

    # 2. Index Old Data
    old_records = {} 
    print("Step 2: Indexing old records...")
    try:
        with open(old_file, 'r', encoding='utf-8') as f:
            for insert_stmt in parse_insert_statements(f):
                table, cols, data, _ = get_row_data(insert_stmt)
                if table and table in pk_map:
                    pk_cols = pk_map[table]
                    # Skip if any primary key column is missing from the data
                    if not all(c in data for c in pk_cols):
                        continue
                    # Tuple used as dict key for composite key support
                    pk_values = tuple(data.get(c) for c in pk_cols)
                    old_records[(table, pk_values)] = data
    except Exception as e:
        raise RuntimeError(f"Error processing old file {old_file}: {e}") from e

    matched_old_keys = set()

    # 3. Compare for Inserts and Updates
    print("Step 3: Comparing for Updates and Inserts...")
    try:
        with open(new_file, 'r', encoding='utf-8') as f_new, \
             open(delta_out, 'w', encoding='utf-8') as f_out:
            
            f_out.write("-- Full Delta Update Script\n")
            f_out.write("SET FOREIGN_KEY_CHECKS = 0;\n\n")

            for insert_stmt in parse_insert_statements(f_new):
                table, cols, new_data, original_stmt = get_row_data(insert_stmt)
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
                    f_out.write(f"-- NEW RECORD IN {table}\n{original_stmt}\n")
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
                    # Skip if table no longer exists in new schema
                    if table not in pk_map:
                        continue
                    pk_cols = pk_map[table]
                    where_str = build_where_clause(pk_cols, old_data)
                    f_out.write(f"-- DELETED FROM {table}: {pk_values}\n")
                    f_out.write(f"DELETE FROM `{table}` WHERE {where_str};\n\n")

            f_out.write("SET FOREIGN_KEY_CHECKS = 1;\n")
    except Exception as e:
        raise RuntimeError(f"Error processing new file or writing output: {e}") from e

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python sqldiff.py <old_dump.sql> <new_dump.sql>")
    else:
        generate_delta(sys.argv[1], sys.argv[2], "full_delta_update.sql")
        print("Done! Check 'full_delta_update.sql' for the results.")
