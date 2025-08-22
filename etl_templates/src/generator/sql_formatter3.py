import re
from typing import List, Literal, Dict, Optional

function_format = {
    "CONCAT": "multiline",
    "CHECKSUM": "multiline",
    "COALESCE": "multiline",
    "SUM": "inline",
    "COUNT": "inline",
}

class SQLFormatter:
    CLAUSES = {
        "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "ON", "JOIN",
        "CREATE", "CONSTRAINT", "WITH", "HAVING"
    }

    FUNCTIONS = {"CONCAT", "CHECKSUM", "CAST", "SUM", "COUNT", "MIN", "MAX", "AVG"}

    def __init__(
        self,
        sql: str,
        comma_style: Literal["end", "leading"] = "end",
        indent_size: int = 4,
        clause_indents: Optional[Dict[str, int]] = None,
        function_format: Optional[Dict[str, Literal["inline", "multiline"]]] = None,
    ):
        self.sql = sql
        self.comma_style = comma_style
        self.indent_size = indent_size
        self.clause_indents = clause_indents or {}
        self.function_format = function_format or {}

    def tokenize(self, sql: str) -> List[str]:
        sql = re.sub(r"\s+", " ", sql.strip())
        # schema.table of alias.column samenhouden
        pattern = r"\[?[A-Za-z0-9_]+\]?(?:\s*\.\s*\[?[A-Za-z0-9_]+\]?)*"
        tokens = []
        pos = 0
        while pos < len(sql):
            if match := re.match(pattern, sql[pos:]):
                tokens.append(match[0].replace(" ", ""))
                pos += len(match[0])
                continue
            if sql[pos] in "(),":
                tokens.append(sql[pos])
                pos += 1
                continue
            tokens.append(sql[pos])
            pos += 1
        return tokens

    def to_list_structure(self) -> List[List[str]]:
        """Zet de hele SQL-string om naar een list-of-lists (per regel tokens)."""
        lines = self.sql.splitlines()
        structured = [self.tokenize(line.strip()) for line in lines if line.strip()]
        # structured = [token for line in structured for token in line if token != " "]
        return structured

    def _align_columns(self, lines: List[List[str]]) -> List[str]:
        """Zorgt dat kolomnamen en types netjes uitgelijnd staan."""
        if not lines:
            return []

        max_lengths = []
        for tokens in lines:
            for i, token in enumerate(tokens):
                if token == ",":
                    continue
                if len(max_lengths) <= i:
                    max_lengths.append(len(token))
                else:
                    max_lengths[i] = max(max_lengths[i], len(token))

        aligned_lines = []
        for idx, tokens in enumerate(lines):
            parts = []
            if self.comma_style == "leading" and idx > 0 and tokens[-1] == ",":
                parts.append(",")
                tokens = tokens[:-1]

            for i, token in enumerate(tokens):
                if token == "," and self.comma_style == "end":
                    parts[-1] = f"{parts[-1].rstrip()},"
                elif token != ",":
                    width = max_lengths[i] if i < len(max_lengths) else len(token)
                    parts.append(token.ljust(width + 1))
            aligned_lines.append("".join(parts).rstrip())
        return aligned_lines

    def _get_indent(self, clause: Optional[str], default_multiplier: int = 1) -> int:
        """Bepaal indent op basis van configuratie."""
        if clause and clause in self.clause_indents:
            return self.clause_indents[clause]
        return self.indent_size * default_multiplier

    def _format_case(self, tokens: List[str], base_indent: int) -> List[str]:
        """Formatteer CASE WHEN blok."""
        lines = []
        indent_when = " " * (base_indent + self.indent_size)
        indent_else = " " * (base_indent + self.indent_size)
        indent_end = " " * base_indent

        buffer = []
        for token in tokens:
            if token.upper() == "WHEN":
                if buffer:
                    lines.append(" ".join(buffer))
                    buffer = []
                lines.append(f"{indent_when}WHEN")
            elif token.upper() == "ELSE":
                if buffer:
                    lines.append(" ".join(buffer))
                    buffer = []
                lines.append(f"{indent_else}ELSE")
            elif token.upper() == "END":
                if buffer:
                    lines.append(" ".join(buffer))
                    buffer = []
                lines.append(f"{indent_end}END")
            else:
                buffer.append(token)
        if buffer:
            lines.append(" ".join(buffer))
        return lines

    def _format_function(self, func_name: str, tokens: List[str], base_indent: int) -> List[str]:
        """Format functie volgens configuratie: inline of multiline."""
        style = self.function_format.get(func_name.upper(), "inline")

        args = []
        current = []
        for token in tokens:
            if token == ",":
                args.append(" ".join(current))
                current = []
            else:
                current.append(token)
        if current:
            args.append(" ".join(current))

        if style == "inline" or len(args) <= 1:
            return [f"{func_name}(" + ", ".join(args) + ")"]

        # multiline
        lines = [f"{func_name}("]
        for idx, arg in enumerate(args):
            prefix = " " * (base_indent + self.indent_size)
            if self.comma_style == "leading" and idx > 0:
                lines.append(f"{prefix}, {arg}")
            elif self.comma_style == "end" and idx < len(args) - 1:
                lines.append(prefix + arg + ",")
            else:
                lines.append(prefix + arg)
        lines.append(" " * base_indent + ")")
        return lines

    def format(self) -> str:
        """Formatter: uitlijnen en inspringen o.b.v. configuratie."""
        structured = self.to_list_structure()
        formatted_lines = []
        current_clause = None
        buffer = []

        def flush_buffer(clause: Optional[str] = None, default_mult: int = 1):
            nonlocal buffer
            if buffer:
                aligned = self._align_columns(buffer)
                indent = self._get_indent(clause, default_mult)
                formatted_lines.extend(" " * indent + line for line in aligned)
                buffer = []

        for tokens in structured:
            if not tokens:
                continue

            first = tokens[0].upper()

            # Detecteer clauses
            if first in self.CLAUSES or first in {"(", ")"}:
                flush_buffer(current_clause)
                if first == "SELECT":
                    formatted_lines.append("SELECT")
                    current_clause = "SELECT"
                elif first in {"FROM", "WHERE", "GROUP", "ORDER", "HAVING"}:
                    formatted_lines.append(f"{first} " + " ".join(tokens[1:]))
                    current_clause = first
                else:
                    formatted_lines.append(" ".join(tokens))
                    current_clause = first
            elif "CASE" in [t.upper() for t in tokens]:
                indent = self._get_indent(current_clause, 1)
                formatted_lines.extend(self._format_case(tokens, indent))
            elif tokens[0].upper() in self.FUNCTIONS:
                func_name = tokens[0].upper()
                args = tokens[2:-1]  # alles tussen ( en )
                indent = self._get_indent(current_clause, 1)
                formatted_lines.extend(self._format_function(func_name, args, indent))
            elif current_clause in {"SELECT", "CREATE"}:
                buffer.append(tokens)
            elif current_clause in {"FROM", "JOIN"}:
                indent = self._get_indent(current_clause, 1)
                formatted_lines.append(" " * indent + " ".join(tokens))
            elif current_clause in {"WHERE", "ON", "HAVING"}:
                indent = self._get_indent(current_clause, 2)
                formatted_lines.append(" " * indent + " ".join(tokens))
            else:
                indent = self._get_indent(current_clause, 1)
                formatted_lines.append(" " * indent + " ".join(tokens))

        flush_buffer(current_clause)
        return "\n".join(formatted_lines)


sql = """
CREATE VIEW [DA_Central].[vw_src_SL_DMS_Declaration] AS
SELECT
            [DeclarationVersion] = o1255.[VERSIONNUMBER],
            [Declaration] = o1255.[REFERENCE],
            [DeclarationProcedureCategory] = o1255.[PROCEDURECATEGORY],
            [AcceptanceDate] = o1292.[FirstAcceptanceDate],
            [DeclarationTID] = o1255.[TID],
            [IsDeclarationCurrentVersion] = CASE WHEN o1255.VERSIONNUMBER = o1300.DeclarationVersion THEN 1 ELSE 0 END,

            [DeclarationCurrentStatus] = o1268.[TYPE],
            [ProcessingStatusCode] = o1268.[TYPE],
    [X_StartDate] = CAST(GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time' AS DATE),
    [X_EndDate] = CAST('2099-12-31' AS DATE),
    [X_HashKey] = CHECKSUM(CONCAT(N'',o1255.[VERSIONNUMBER],o1255.[REFERENCE],o1255.[PROCEDURECATEGORY],o1292.[FirstAcceptanceDate],o1255.[TID],CASE WHEN o1255.VERSIONNUMBER = o1300.DeclarationVersion THEN 1 ELSE 0 END,o1268.[TYPE],o1268.[TYPE],'DMS')),
    [X_IsCurrent] = 1,
    [X_IsReplaced] = 0,
    [X_RunId] = '',
    [X_LoadDateTime] = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time',
    [X_Bron] = 'DMS'
            FROM
            [SL_DMS].[DECLARTN] AS o1255
            LEFT JOIN
            [DA_Central].[AggrLastStatusVersion] AS o1259
            ON                     o1259.[Declaration] = o1255.[reference]
            LEFT JOIN
            [SL_DMS].[PRCSSTUS] AS o1268
            ON                     o1268.[declaration_reference] = o1255.[REFERENCE]
AND
                    o1268.[versionNumber] = o1259.[StatusVersionNumber]
            LEFT JOIN
            [DA_Central].[AggrFirstTimeStatus] AS o1292
            ON                     o1292.[Declaration] = o1255.[reference]
            LEFT JOIN
            [DA_Central].[AggrDeclarationMaxVersion] AS o1300
            ON                     o1300.[Declaration] = o1255.[reference]
AND
                    o1300.[DeclarationVersion] = o1255.[versionNumber]
WHERE
    1 = 1             AND                 o1268.[TYPE]

            NOT IN ('1','2')
"""
formatter = SQLFormatter(
    sql,
    comma_style="leading",
    function_format={
        "CONCAT": "multiline",
        "COALESCE": "multiline",
        "SUM": "inline"
    }
)

print(formatter.format())