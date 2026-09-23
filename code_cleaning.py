
import json
import re
def clean_code_blocks(text):
    return re.sub(r"```python\s*|```", "", text)

def clean_json_block(text: str) -> str:
    return text.replace("```json", "").replace("```", "").strip()

def remove_block_metadata(text: str) -> str:
    return re.sub(
        r"^(blockid:.*|code:)\s*$",
        "",
        text,
        flags=re.MULTILINE
    ).strip()

def remove_sas_fence(text):
    return re.sub(r'```sas|```', '', text)

def strip_imports(code):
    lines = code.splitlines()
    extracted_imports = []
    seen_imports = set()
    clean_lines = []

    # Regex breakdown:
    # ^\s* -> Start of line, allowing for indentation
    # (import\s+)   -> Matches "import "
    # |             -> OR
    # (from\s+\S+\s+import\s+) -> Matches "from <something> import "
    import_pattern = r'^\s*(import\s+|from\s+\S+\s+import\s+)'
    for line in lines:
        # We use re.match because it checks from the beginning of the string
        if re.match(import_pattern, line):
            # Each chunk is converted independently, so the same import line (e.g.
            # "from pyspark.sql import SparkSession") gets re-emitted by nearly every
            # chunk. Dedupe on the normalized statement, keeping the first occurrence's
            # position so import order stays stable.
            normalized = line.strip()
            if normalized not in seen_imports:
                seen_imports.add(normalized)
                extracted_imports.append(normalized)
        else:
            clean_lines.append(line)

    clean_code = "\n".join(clean_lines)
    return extracted_imports, clean_code


# Matches a `<var> = SparkSession.builder[.method(...)]*.getOrCreate()` statement, including
# variants where the fluent chain is spread across multiple lines.
_SPARK_SESSION_PATTERN = re.compile(
    r'^[ \t]*(?P<var>\w+)\s*=\s*SparkSession\s*\.\s*builder\s*'
    r'(?:\.\s*\w+\([^)]*\)\s*)*'
    r'\.\s*getOrCreate\(\s*\)[ \t]*\n?',
    re.MULTILINE
)


def dedupe_spark_session_init(code: str) -> str:
    '''
    Each chunk is converted independently, so nearly every chunk re-emits its own
    `spark = SparkSession.builder...getOrCreate()` boilerplate. getOrCreate() is idempotent
    (it just hands back the already-active session), so keep only the first creation per
    variable name and drop the repeats instead of rebuilding the same session dozens of
    times in one generated script. A creation under a different variable name is left
    alone, since later code in that chunk may reference that name specifically.
    '''
    matches = list(_SPARK_SESSION_PATTERN.finditer(code))
    if len(matches) <= 1:
        return code

    seen_vars = set()
    result = []
    last_end = 0
    for m in matches:
        var = m.group('var')
        result.append(code[last_end:m.start()])
        if var not in seen_vars:
            seen_vars.add(var)
            result.append(m.group(0))
        last_end = m.end()
    result.append(code[last_end:])
    return "".join(result)


def normalize_stray_sql_comments(code: str) -> str:
    '''
    LLM-converted chunks occasionally slip into SQL-comment syntax ("-- comment") for a line that sits
    directly in the Python driver script rather than inside a spark.sql("""...""") string - that's a
    Python SyntaxError, not just a style nit. Rewrite any "--" line that starts outside a triple-quoted
    block to a "#" comment; "--" comments genuinely inside a triple-quoted SQL string are left untouched.

    Only call this on Python/notebook output - a pure .sql file's "--" comments are correct SQL syntax.
    '''
    in_triple = False
    triple_quote = None
    result = []

    for line in code.splitlines(keepends=True):
        stripped = line.lstrip()
        body = stripped.rstrip("\n")
        if not in_triple and body.startswith("--"):
            indent = line[: len(line) - len(stripped)]
            newline = "\n" if stripped.endswith("\n") else ""
            comment_text = body[2:].strip()
            line = f"{indent}# {comment_text}{newline}" if comment_text else f"{indent}#{newline}"

        # Track triple-quote state (naive - doesn't handle escaped quotes, but generated code always
        # uses cleanly closed spark.sql('''...''') / f'''...''' blocks).
        for marker in ('"""', "'''"):
            if line.count(marker) % 2 == 1:
                if not in_triple:
                    in_triple, triple_quote = True, marker
                elif marker == triple_quote:
                    in_triple, triple_quote = False, None

        result.append(line)

    return "".join(result)


def ensure_non_empty_conversion(converted_code: str, original_sas_code: str, label: str = "") -> str:
    '''
    Guarantees a chunk/subchunk never disappears silently from the output. If the LLM decided a piece of
    SAS logic didn't need converting (e.g. pure reporting/formatting with no data-pipeline equivalent) and
    returned empty/whitespace-only content, replace it with a deterministic comment that preserves the
    original SAS source instead of vanishing without a trace - the same outcome the prompts ask the model
    to produce itself, enforced here so it holds even when the model doesn't comply.
    '''
    if converted_code and converted_code.strip():
        return converted_code

    tag = f"{label}: " if label else ""
    original_sas_code = (original_sas_code or "").strip()
    if not original_sas_code:
        return f"# NOTE: {tag}no source code or conversion output was found for this block.".strip()

    commented_source = "\n".join(f"# {line}" for line in original_sas_code.splitlines())
    header = f"# NOTE: {tag}no conversion was produced for this block - original SAS source preserved below:"
    return f"{header}\n{commented_source}"


'''
Modified by : Rohit Prashant
Description : Extra metadata characters in the file
Date : 15-May
'''
def remove_block_metadata(text: str) -> str:
    return re.sub(
        r"^(blockid:.|code:)\s$",
        "",
        text,
        flags=re.MULTILINE
    ).strip()


def extract_fields_from_block(text: str, keys_to_extract: list) -> dict:
    clean_to_orig = {k.strip().lower(): k for k in keys_to_extract}
    clean_keys = sorted(clean_to_orig.keys(), key=len, reverse=True)
    
    escaped_keys = "|".join([re.escape(k) for k in clean_keys])
    
    # This is the "Anchor". It ensures we only catch keys at the start of a line.
    # It handles "key":, 'key':, or key:
    pattern_str = rf'(?:^|\n)\s*(?P<quote>["\']?)(?P<key>{escaped_keys})(?P=quote)\s*[:=]'
    
    matches = list(re.finditer(pattern_str, text, flags=re.IGNORECASE | re.MULTILINE))
    result = {orig_key: None for orig_key in keys_to_extract}
    
    for i in range(len(matches)):
        match = matches[i]
        found_key_norm = match.group('key').lower()
        orig_key = clean_to_orig[found_key_norm]
        
        val_start = match.end()
        val_end = matches[i+1].start() if i + 1 < len(matches) else len(text)
        
        # Slicing is literal - it doesn't care about .* or B.*
        value = text[val_start:val_end].strip()
        
        # Clean up only the outer edges of the slice
        value = value.rstrip(',').strip()
        if (value.startswith('"') and value.endswith('"')) or \
           (value.startswith("'") and value.endswith("'")):
            value = value[1:-1].strip()
            
        if result[orig_key] is None:
            result[orig_key] = value

    return result

def remove_single_sql_line(code):
    cleaned_text = re.sub(r'(?m)^\s*sql\s*$\n?', '', code)
    return cleaned_text

def format_subchunks(text):
    """
    Finds lines containing BEGIN SUBCHUNK or END OF SUBCHUNK 
    and prepends a # to the start of the line.
    """
    # Pattern explanation:
    # (?mi) -> m: multiline mode, i: case-insensitive
    # ^     -> start of a line
    # .*    -> any characters (like /* or spaces)
    # (BEGIN SUBCHUNK|END OF SUBCHUNK) -> the target phrases
    # .*    -> any characters following (like */)
    pattern = r"(?mi)^(.*(?:BEGIN SUBCHUNK|END OF SUBCHUNK).*)$"
    
    # We replace the line with # followed by the original content
    return re.sub(pattern, r"# \1", text)

def remove_subchunk_lines(file_path):
    # Fixed regex: Removed the mandatory '#' and allowed for potential leading spaces/quotes
    pattern = r'/?\*?\s*/\*(BEGIN|END)(?:\s+OF)?\s+SUBCHUNK\s+\d+\*/'
    
    # Alternatively, if they are ALWAYS just standard comments, use this cleaner pattern:
    # pattern = r'/\*(BEGIN|END)(?:\s+OF)?\s+SUBCHUNK\s+\d+\*/'
    
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        
    # Keep only the lines that DO NOT match the subchunk pattern
    filtered_lines = [line for line in lines if not re.search(pattern, line)]
    
    # Write the clean content back to the file
    with open(file_path, 'w', encoding='utf-8') as file:
        file.writelines(filtered_lines)


# Matches ONLY the LLM's own bare, description-less echo of the subchunk marker in Python-comment
# form ("# SUBCHUNK 1", "#BEGIN SUBCHUNK 7", "# END OF SUBCHUNK 6", ...) despite the prompt telling
# it not to include one. Deliberately does NOT match "/*BEGIN SUBCHUNK N*/" / "/*END OF SUBCHUNK
# N*/" - those are inserted deterministically by _enforce_subchunk_markers and MUST survive intact
# for downstream parsing. Also deliberately does NOT match "#BEGIN CHUNK N#" / "#END OF CHUNK N#" -
# those are separate structural boundary tags (written in sas_to_pysparkconvertor_*file.py) that
# auto_fix_master_notebook.py explicitly relies on and must never be stripped. Anchored so a real
# explanatory comment like "# SUBCHUNK 14: Create WinningModels by merging..." is left alone - only
# a line that is nothing BUT "SUBCHUNK <number>" (no trailing description) gets dropped.
_BARE_SUBCHUNK_COMMENT_LINE_PATTERN = re.compile(
    r"(?i)^\s*#\s*(?:BEGIN\s+|END(?:\s+OF)?\s+)?SUBCHUNK\s+\d+\s*#?\s*$"
)


def remove_subchunk_marker_lines(code: str) -> str:
    '''
    Strips the LLM's bare "# SUBCHUNK N" / "#BEGIN SUBCHUNK N" / "#END OF SUBCHUNK N" echo lines
    from converted output when they carry no description after the number. Leaves the deterministic
    "/*BEGIN|END OF SUBCHUNK N*/" markers and the "#BEGIN|END OF CHUNK N#" structural tags untouched -
    both are relied on elsewhere and must never be removed. Operates on a string in memory - unlike
    remove_subchunk_lines(), which reads/writes a file on disk - so it can run as one more step in
    the new_code cleanup chain before the result is written out.
    '''
    lines = code.splitlines(keepends=True)
    kept = [line for line in lines if not _BARE_SUBCHUNK_COMMENT_LINE_PATTERN.match(line)]
    return "".join(kept)


# The deterministic marker inserted by _enforce_subchunk_markers is carried over verbatim from the
# original SAS-style "/* ... */" comment syntax. That's valid in a .sql output file, but landing
# outside a triple-quoted string in a .py/.ipynb file it's a SyntaxError ('/' is not a comment
# character in Python). Matches both "/*BEGIN SUBCHUNK N*/" and "/*END OF SUBCHUNK N*/".
_SUBCHUNK_BLOCK_MARKER_PATTERN = re.compile(
    r"(?i)/\*\s*(BEGIN|END)(?:\s+OF)?\s+SUBCHUNK\s+(\d+)\s*\*/"
)


def convert_subchunk_markers_to_python_comments(code: str) -> str:
    '''
    Rewrites the deterministic "/*BEGIN SUBCHUNK N*/" / "/*END OF SUBCHUNK N*/" markers into
    Python "#"-comment equivalents - "# BEGIN SUBCHUNK N" / "# END OF SUBCHUNK N" - so they read as
    valid Python instead of a stray C-style block comment. Only call this for non-SQL output; a
    .sql file should keep the original /* */ markers, which are valid SQL syntax there.

    Call this AFTER remove_subchunk_marker_lines(), not before: that function tells the real
    deterministic marker apart from the LLM's own bogus "#"-style echo precisely because the real
    one is still in "/* */" form at that point. Converting first would make the two indistinguishable
    and risk stripping the real marker along with the echo.
    '''
    def _replace(match):
        keyword = match.group(1).upper()
        number = match.group(2)
        label = "BEGIN" if keyword == "BEGIN" else "END OF"
        return f"# {label} SUBCHUNK {number}"

    return _SUBCHUNK_BLOCK_MARKER_PATTERN.sub(_replace, code)


# Splits on the "#BEGIN CHUNK N#" / "#END OF CHUNK N#" structural tags the converters themselves
# write around each converted chunk (see sas_to_pysparkconvertor_*file.py), so a notebook cell
# boundary lines up with a pipeline chunk boundary instead of dumping everything into one cell.
_CHUNK_BLOCK_PATTERN = re.compile(r"(?s)#BEGIN CHUNK \d+#\r?\n(.*?)\r?\n#END OF CHUNK \d+#")


def build_notebook_json(imports_code: str, chunks_code: str) -> str:
    '''
    Wraps generated Python source into a minimal but valid Jupyter notebook (nbformat 4, one JSON
    document with "cells"/"metadata"/etc.) instead of writing raw script text into a file merely
    NAMED "*.ipynb" - Jupyter/Colab/BigQuery Studio would fail to parse the latter as a notebook.

    `imports_code` (the deduped import lines) becomes its own leading cell. `chunks_code` (the rest
    of the converted output, still wrapped in "#BEGIN CHUNK N#"/"#END OF CHUNK N#" tags at this
    point) is split into one code cell per chunk; if no chunk tags are found it falls back to a
    single cell holding everything. Call this AFTER all other new_code cleanup steps (subchunk
    marker handling, comment normalization, etc.) - it's the last step before writing the file.
    '''
    cells_source = []
    if imports_code and imports_code.strip():
        cells_source.append(imports_code.strip())

    chunk_bodies = _CHUNK_BLOCK_PATTERN.findall(chunks_code)
    if chunk_bodies:
        cells_source.extend(body.strip() for body in chunk_bodies if body.strip())
    elif chunks_code.strip():
        cells_source.append(chunks_code.strip())

    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": source.splitlines(keepends=True),
            }
            for source in cells_source
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    return json.dumps(notebook, indent=1)