import re
from typing import List


class SqlCreateViewFormatter:
    def __init__(
        self,
        base_indent: int = 4,
        new_line_keywords: List[str] = None,
    ):
        self.base_indent = base_indent
        self.new_line_keywords = new_line_keywords or [
            r"\bSELECT\b",
            r"\bFROM\b",
            r"\bLEFT JOIN\b",
            r"\bRIGHT JOIN\b",
            r"\bINNER JOIN\b",
            r"\bWHERE\b",
            r"\bAND\b",
            r"\bOR\b",
            r"\bON\b",
            r"\bCASE\b",
            r"\bEND\b",
            r"\bTHEN\b",
            r"\bELSE\b",
        ]

    def _normalize_whitespace(self, sql: str) -> str:
        """
        Normalizes whitespace in the SQL statement for consistency.

        Removes leading and trailing spaces and replaces multiple spaces with a single space.

        Args:
            sql (str): The SQL statement to process.

        Returns:
            str: The SQL statement with normalized whitespace.
        """
        return re.sub(r"\s+", " ", sql.strip())

    def _uppercase_keywords(self, sql: str) -> str:
        """
        Converts SQL keywords in the statement to uppercase for consistency.

        Searches for each keyword in the configured list and replaces it with its uppercase form.

        Args:
            sql (str): The SQL statement to process.

        Returns:
            str: The SQL statement with keywords in uppercase.
        """
        for kw in self.new_line_keywords:
            sql = re.sub(kw, lambda m: m.group(0).upper(), sql, flags=re.IGNORECASE)
        return sql

    def _newline_before_as(self, sql: str) -> str:
        """
        Inserts a newline before the 'AS' keyword in CREATE VIEW statements.

        Improves readability by separating the view name from the 'AS' clause.

        Args:
            sql (str): The SQL statement to process.

        Returns:
            str: The SQL statement with a newline before 'AS' in CREATE VIEW.
        """
        return re.sub(
            r"CREATE VIEW ([^\s]+)\s+AS",
            r"CREATE VIEW \1\nAS",
            sql,
            flags=re.IGNORECASE,
        )

    def _insert_newlines(self, sql: str) -> str:
        """
        Inserts newlines before specific SQL keywords to improve readability.

        Processes the SQL statement and adds a newline before each keyword in the configured list.

        Args:
            sql (str): The SQL statement to process.

        Returns:
            str: The SQL statement with newlines inserted before keywords.
        """
        for kw in self.new_line_keywords:
            sql = re.sub(kw, lambda m: "\n" + m.group(0), sql, flags=re.IGNORECASE)
        return sql

    def _split_columns(self, sql: str) -> str:
        """
        Splits columns in the SQL statement onto separate lines for readability.

        Adds a newline after each top-level comma, ignoring commas inside parentheses.

        Args:
            sql (str): The SQL statement to process.

        Returns:
            str: The SQL statement with columns split onto new lines.
        """
        result = ""
        depth = 0
        for char in sql:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            result += ",\n" if char == "," and depth == 0 else char
        return result

    def _apply_indentation(self, sql: str) -> str:
        """
        Applies indentation to each line of the SQL statement for improved readability.

        Processes each line, adjusting indentation based on SQL structure and keywords.

        Args:
            sql (str): The SQL statement to format.

        Returns:
            str: The formatted SQL statement with proper indentation.
        """
        lines = sql.split("\n")
        result_lines = []
        indent_level = 0
        extra_indent_for_and = False
        in_select_block = False

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            indent_level = self._update_indent_before(
                stripped=stripped, indent_level=indent_level
            )
            current_indent = self._calculate_current_indent(
                stripped=stripped,
                indent_level=indent_level,
                extra_indent_for_and=extra_indent_for_and,
                in_select_block=in_select_block,
            )
            result_lines.append(" " * (self.base_indent * current_indent) + stripped)
            indent_level = self._update_indent_after(
                stripped=stripped, indent_level=indent_level
            )
            extra_indent_for_and = self._update_extra_indent_for_and(
                stripped=stripped, extra_indent_for_and=extra_indent_for_and
            )
            in_select_block = self._update_in_select_block(
                stripped=stripped, in_select_block=in_select_block
            )

        return "\n".join(result_lines)

    def _update_indent_before(self, stripped: str, indent_level: int) -> int:
        """
        Adjusts the indentation level before processing the current SQL line.

        Decreases the indentation if the line starts with 'END' or ')'.

        Args:
            stripped (str): The current line stripped of whitespace.
            indent_level (int): The current indentation level.

        Returns:
            int: The updated indentation level.
        """
        if re.match(r"^END\b", stripped) or stripped.startswith(")"):
            indent_level -= 1
        return indent_level

    def _calculate_current_indent(
        self,
        stripped: str,
        indent_level: int,
        extra_indent_for_and: bool,
        in_select_block: bool,
    ) -> int:
        """
        Calculates the current indentation level for a SQL line.

        Adjusts indentation based on keywords and block context.

        Args:
            stripped (str): The current line stripped of whitespace.
            indent_level (int): The current indentation level.
            extra_indent_for_and (bool): Whether to add extra indentation for 'AND' after a JOIN.
            in_select_block (bool): Whether the line is within a SELECT block.

        Returns:
            int: The calculated indentation level for the line.
        """
        current_indent = indent_level
        if stripped.startswith("AND") and extra_indent_for_and:
            current_indent += 1
        if in_select_block and not stripped.startswith("SELECT"):
            current_indent += 1
        return current_indent

    def _update_indent_after(self, stripped: str, indent_level: int) -> int:
        """
        Updates the indentation level after processing the current SQL line.

        Increases the indentation if the line starts with 'CASE' or ends with '('.

        Args:
            stripped (str): The current line stripped of whitespace.
            indent_level (int): The current indentation level.

        Returns:
            int: The updated indentation level.
        """
        if re.match(r"^CASE\b", stripped):
            indent_level += 1
        if stripped.endswith("("):
            indent_level += 1
        return indent_level

    def _update_extra_indent_for_and(
        self, stripped: str, extra_indent_for_and: bool
    ) -> bool:
        """
        Determines whether to apply extra indentation for 'AND' lines following a JOIN.

        Returns True if the current line contains 'JOIN', otherwise resets for non-'AND' lines.

        Args:
            stripped (str): The current line stripped of whitespace.
            extra_indent_for_and (bool): Current state of extra indentation for 'AND'.

        Returns:
            bool: Updated state for extra indentation for 'AND'.
        """
        if re.search(r"\bJOIN\b", stripped):
            return True
        elif not stripped.startswith("AND"):
            return False
        return extra_indent_for_and

    def _update_in_select_block(self, stripped: str, in_select_block: bool) -> bool:
        """
        Determines whether the current line is within a SELECT block in the SQL statement.

        Returns True if inside a SELECT block, and False when a FROM clause is encountered.

        Args:
            stripped (str): The current line stripped of whitespace.
            in_select_block (bool): Current state indicating if inside a SELECT block.

        Returns:
            bool: Updated state for being inside a SELECT block.
        """
        if stripped.startswith("SELECT"):
            return True
        elif stripped.startswith("FROM"):
            return False
        else:
            return in_select_block

    def format(self, sql: str) -> str:
        """
        Formats a raw SQL statement for improved readability and structure.

        Applies normalization, keyword uppercasing, newline insertion, column splitting, and indentation.

        Args:
            sql (str): The raw SQL statement to format.

        Returns:
            str: The formatted SQL statement.
        """
        sql = self._normalize_whitespace(sql)
        sql = self._uppercase_keywords(sql)
        sql = self._newline_before_as(sql)
        sql = self._insert_newlines(sql)
        sql = self._split_columns(sql)
        sql = self._apply_indentation(sql)
        return sql


# Voorbeeld
if __name__ == "__main__":
    raw_sql = """
    CREATE VIEW [DA_Central].[vw_src_SL_DMS_Declaration] AS  SELECT [DeclarationVersion] = o1255.[VERSIONNUMBER], [Declaration] = o1255.[REFERENCE],
           [DeclarationProcedureCategory] = o1255.[PROCEDURECATEGORY],
           [AcceptanceDate] = o1292.[FirstAcceptanceDate], [DeclarationTID] = o1255.[TID],
           [IsDeclarationCurrentVersion] = CASE WHEN o1255.VERSIONNUMBER = o1300.DeclarationVersion THEN 1
                                                ELSE 0 END, [DeclarationCurrentStatus] = o1268.[TYPE],
           [ProcessingStatusCode] = o1268.[TYPE],
           [X_StartDate] = CAST(GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. EUROPE STANDARD TIME' AS DATE),
           [X_EndDate] = CAST('2099-12-31' AS DATE), NONE, [X_IsCurrent] = 1, [X_IsReplaced] = 0, [X_RunId] = '',
           [X_LoadDateTime] = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. EUROPE STANDARD TIME', [X_Bron] = 'DMS'
    FROM [SL_DMS].[DECLARTN] AS o1255
    LEFT JOIN [DA_Central].[AggrLastStatusVersion] AS o1259 ON o1259.[Declaration] = o1255.[reference]
    LEFT JOIN [SL_DMS].[PRCSSTUS] AS o1268 ON o1268.[declaration_reference] = o1255.[REFERENCE]
      AND o1268.[versionNumber] = o1259.[StatusVersionNumber]
    LEFT JOIN [DA_Central].[AggrFirstTimeStatus] AS o1292 ON o1292.[Declaration] = o1255.[reference]
    LEFT JOIN [DA_Central].[AggrDeclarationMaxVersion] AS o1300 ON o1300.[Declaration] = o1255.[reference]
      AND o1300.[DeclarationVersion] = o1255.[versionNumber]
    WHERE 1 = 1
      AND o1268.[TYPE] NOT IN ('1', '2')
    """
    print(SqlCreateViewFormatter().format(raw_sql))
