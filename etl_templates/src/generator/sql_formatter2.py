import re
from typing import List, Literal, Dict, Optional


class SQLFormatter:
    CLAUSES = {
        "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "ON", "JOIN",
        "CREATE", "CONSTRAINT", "WITH", "HAVING"
    }

    def __init__(
        self,
        sql: str,
        comma_style: Literal["end", "leading"] = "end",
        indent_size: int = 4,
        clause_indents: Optional[Dict[str, int]] = None,
    ):
        self.sql = sql
        self.comma_style = comma_style
        self.indent_size = indent_size
        self.clause_indents = clause_indents or {}

    def tokenize(self, text: str) -> List[str]:
        """Splits SQL in tokens: identifiers, operators, haakjes, komma’s, keywords."""
        token_pattern = r"(\[.*?\]|'.*?'|\w+|,|\(|\)|=|\.)"
        tokens = re.findall(token_pattern, text)
        return tokens

    def to_list_structure(self) -> List[List[str]]:
        """Zet de hele SQL-string om naar een list-of-lists (per regel tokens)."""
        lines = self.sql.splitlines()
        structured = [self.tokenize(line.strip()) for line in lines if line.strip()]
        return structured

    def _align_columns(self, lines: List[List[str]]) -> List[str]:
        """Zorgt dat kolomnamen en types netjes uitgelijnd staan."""
        if not lines:
            return []

        # bepaal max kolombreedtes
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
                # komma aan begin van regel
                parts.append(",")
                tokens = tokens[:-1]

            for i, token in enumerate(tokens):
                if token == "," and self.comma_style == "end":
                    # plak komma direct achter het vorige token
                    parts[-1] = parts[-1].rstrip() + ","
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

            # Detecteer clause
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
            elif current_clause in {"SELECT", "CREATE"}:
                buffer.append(tokens)
            elif current_clause in {"FROM", "JOIN"}:
                indent = self._get_indent(current_clause, 1)
                formatted_lines.append(" " * indent + " ".join(tokens))
            elif current_clause in {"WHERE", "ON"}:
                indent = self._get_indent(current_clause, 2)
                formatted_lines.append(" " * indent + " ".join(tokens))
            else:
                indent = self._get_indent(current_clause, 1)
                formatted_lines.append(" " * indent + " ".join(tokens))

        flush_buffer(current_clause)
        return "\n".join(formatted_lines)
