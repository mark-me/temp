import re

from logtools import get_logger

logger = get_logger(__name__)

class SqlFormatter:
    """
    Formatteert SQL statements voor betere leesbaarheid en structuur.

    Biedt methoden om witruimte te normaliseren, sleutelwoorden te kapitaliseren, nieuwe regels toe te voegen, kolommen te splitsen en inspringing toe te passen op SQL-code.
    """

    def __init__(self, indent_size: int = 4):
        """
        Initialiseert een nieuwe SqlFormatter instantie met de opgegeven inspringgrootte.

        Zet de standaardwaarden voor inspringing en de te herkennen SQL sleutelwoorden.

        Args:
            indent_size (int): Het aantal spaties per inspringniveau.
        """
        self.indent_size = indent_size
        self.new_line_keywords = [
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
            r"\bGROUP BY\b",
            r"\bORDER BY\b",
            r"\bHAVING\b",
        ]
        self.keywords = [
            "SELECT",
            "FROM",
            "WHERE",
            "AND",
            "OR",
            "JOIN",
            "LEFT",
            "RIGHT",
            "INNER",
            "ON",
            "AS",
            "CASE",
            "END",
            "THEN",
            "ELSE",
            "CREATE",
            "TABLE",
            "VIEW",
            "PRIMARY",
            "KEY",
            "NOT",
            "NULL",
            "GROUP BY",
            "ORDER BY",
            "HAVING",
        ]

    def format(self, sql: str) -> str:
        """
        Formatteert een SQL-statement voor betere leesbaarheid en structuur.

        Past normalisatie van witruimte, hoofdletters voor sleutelwoorden, nieuwe regels, kolomsplitsing en inspringing toe.

        Args:
            sql (str): Het SQL-statement dat geformatteerd moet worden.

        Returns:
            str: Het geformatteerde SQL-statement.
        """
        sql = self._normalize_whitespace(sql)
        sql = self._uppercase_keywords(sql)

        if sql.upper().startswith("CREATE TABLE"):
            return self._format_create_table(sql)
        elif sql.upper().startswith("CREATE VIEW"):
            return self._format_create_view(sql)
        else:
            return self._apply_indentation(self._insert_newlines(sql))

    def _normalize_whitespace(self, sql: str) -> str:
        """
        Normaliseert witruimte in het SQL-statement voor consistentie.

        Verwijdert overtollige spaties en zorgt dat het statement begint en eindigt zonder witruimte.

        Args:
            sql (str): Het SQL-statement dat verwerkt moet worden.

        Returns:
            str: Het SQL-statement met genormaliseerde witruimte.
        """
        return re.sub(r"\s+", " ", sql).strip()

    def _uppercase_keywords(self, sql: str) -> str:
        """
        Zet SQL-sleutelwoorden in het statement om naar hoofdletters voor consistentie.

        Zoekt naar elk sleutelwoord in de lijst en vervangt deze door de hoofdlettervariant.

        Args:
            sql (str): Het SQL-statement dat verwerkt moet worden.

        Returns:
            str: Het SQL-statement met sleutelwoorden in hoofdletters.
        """
        for kw in sorted(self.keywords, key=len, reverse=True):
            sql = re.sub(kw, kw.upper(), sql, flags=re.IGNORECASE)
        return sql

    def _insert_newlines(self, sql: str) -> str:
        """
        Voegt nieuwe regels toe vóór bepaalde SQL-sleutelwoorden om de leesbaarheid te verbeteren.

        Doorzoekt het statement en plaatst een nieuwe regel voor elk sleutelwoord in de ingestelde lijst.

        Args:
            sql (str): Het SQL-statement dat verwerkt moet worden.

        Returns:
            str: Het SQL-statement met nieuwe regels voor sleutelwoorden.
        """
        for kw in self.new_line_keywords:
            sql = re.sub(rf"\s*({kw})\s*", r"\n\1 ", sql, flags=re.IGNORECASE)
        return sql

    def _split_top_level_commas(self, sql: str) -> str:
        """
        Splitst het SQL-statement op komma's op het hoogste niveau voor betere leesbaarheid.

        Voegt een nieuwe regel toe na elke komma die niet binnen haakjes staat.

        Args:
            sql (str): Het SQL-statement dat verwerkt moet worden.

        Returns:
            str: Het SQL-statement met nieuwe regels na top-level komma's.
        """
        parts = []
        current = []
        depth = 0
        for ch in sql:
            if ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                current.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            parts.append("".join(current).strip())
        return parts

    def _apply_indentation(self, sql: str) -> str:
        """
        Past inspringing toe op elke regel van het SQL-statement voor betere leesbaarheid.

        Verhoogt of verlaagt het inspringniveau op basis van haakjes, CASE-structuren en subqueries.

        Args:
            sql (str): Het SQL-statement dat verwerkt moet worden.

        Returns:
            str: Het SQL-statement met correcte inspringing.
        """
        lines = sql.splitlines()
        indent_level = 0
        formatted_lines = []

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            indent_level = self._decrease_indent(stripped, indent_level)
            formatted_lines.append(" " * (indent_level * self.indent_size) + stripped)
            indent_level = self._increase_indent(stripped, indent_level)

        return "\n".join(formatted_lines)

    def _decrease_indent(self, stripped: str, indent_level: int) -> int:
        """
        Verlaagt het inspringniveau als de regel een sluitend haakje of END bevat.

        Controleert of de regel eindigt met ')' of 'END' en verlaagt indien nodig het inspringniveau.

        Args:
            stripped (str): De huidige regel zonder witruimte.
            indent_level (int): Het huidige inspringniveau.

        Returns:
            int: Het aangepaste inspringniveau.
        """
        import re

        # Decrease indent if line starts with ')' or 'END', or contains them as standalone tokens
        if (
            re.match(r"^\s*\)\b", stripped)
            or re.match(r"^\s*END\b", stripped, re.IGNORECASE)
            or re.search(r"\b\)\b", stripped)
            or re.search(r"\bEND\b", stripped, re.IGNORECASE)
        ):
            return max(indent_level - 1, 0)
        return indent_level

    def _increase_indent(self, stripped: str, indent_level: int) -> int:
        """
        Verhoogt het inspringniveau bij een openend haakje, CASE, THEN of subquery.

        Controleert of de regel eindigt met '(', begint met 'CASE' of 'THEN', of een subquery bevat, en verhoogt indien nodig het inspringniveau.

        Args:
            stripped (str): De huidige regel zonder witruimte.
            indent_level (int): Het huidige inspringniveau.

        Returns:
            int: Het aangepaste inspringniveau.
        """
        if stripped.endswith("("):
            if "SELECT" in stripped.upper() or stripped.strip() == "(":
                return indent_level + 1
        elif stripped.upper().startswith(("CASE", "THEN")):
            return indent_level + 1
        if re.match(r"\(SELECT\b", stripped.upper()):
            return indent_level + 1
        return indent_level

    def _format_create_view(self, sql: str) -> str:
        """
        Formatteert een CREATE VIEW-statement voor betere leesbaarheid en structuur.

        Voegt nieuwe regels toe, splitst kolommen en past inspringing toe op het statement.

        Args:
            sql (str): Het CREATE VIEW-statement dat verwerkt moet worden.

        Returns:
            str: Het geformatteerde CREATE VIEW-statement.
        """
        select_lines, from_clause, join_clauses, where_lines = self._parse_create_view(sql)
        sql = self._insert_newlines(sql)
        sql = self._split_top_level_commas(sql)
        sql = self._apply_indentation(sql)
        return sql

    def _parse_create_view(self, sql: str):
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        select_block = select_match.group(1).strip() if select_match else None
        select_lines = [line.strip().rstrip(',') for line in select_block.splitlines() if line.strip()]

        from_match = re.search(r'FROM\s+(.*?)\s+(?:LEFT|RIGHT|FULL\s+OUTER|OUTER|INNER)\s+JOIN', sql, re.IGNORECASE | re.DOTALL)
        from_clause = from_match.group(1).strip() if from_match else None

        join_matches = re.findall(
        r'((?:LEFT|RIGHT|FULL\s+OUTER|OUTER|INNER)\s+JOIN\s+.*?(?=(?:LEFT|RIGHT|FULL\s+OUTER|OUTER|INNER)\s+JOIN|WHERE|$))',
        sql,
        re.IGNORECASE | re.DOTALL
        )
        join_clauses = [j.strip() for j in join_matches]

        where_match = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE | re.DOTALL)
        where_block = where_match.group(1).strip() if where_match else None
        where_lines = [line.strip() for line in where_block.splitlines() if line.strip()] if where_block else []
        return select_lines, from_clause, join_clauses, where_lines

    def _format_create_table(self, sql_content: str) -> str:
        """
        Formatteert een CREATE TABLE SQL-statement voor leesbaarheid en uitlijning.

        Deze methode zorgt ervoor dat elke kolom en constraint duidelijk gescheiden en uitgelijnd wordt,
        zodat het statement eenvoudiger te lezen en te onderhouden is.

        Args:
            sql_content (str): Het CREATE TABLE SQL-statement dat geformatteerd moet worden.

        Returns:
            str: Het geformatteerde CREATE TABLE SQL-statement.

        Raises:
            ValueError: Als de CREATE TABLE-structuur niet geparsed kan worden.
        """
        sql_content = self._normalize_whitespace(sql_content)
        table_name, cols_block, with_block = self._parse_create_table(sql_content)
        columns, constraints = self._split_columns_and_constraints(cols_block)
        formatted_cols = self._format_columns(columns)
        formatted_constraints = [f"    {c}" for c in constraints]
        formatted_with = self._format_with_block(with_block)
        formatted_sql = (
            f"CREATE TABLE {table_name.strip()} (\n"
            + "\n,".join(formatted_cols + formatted_constraints)
            + "\n)\nWITH (\n"
            + ",\n".join(formatted_with)
            + "\n);"
        )
        return formatted_sql

    def _parse_create_table(self, sql_content: str) -> tuple[str, str, str] | None:
        """
        Parseert een CREATE TABLE SQL-statement en haalt de tabelnaam, kolomdefinities en WITH-blok op.

        Deze methode retourneert de verschillende delen van het CREATE TABLE statement als aparte strings.

        Args:
            sql_content (str): Het CREATE TABLE SQL-statement dat geparsed moet worden.

        Returns:
            tuple[str, str, str]: Een tuple bestaande uit (tabelnaam, kolomdefinities, with-blok) als strings.
        """
        if match := re.match(
            r"CREATE TABLE\s+([^\(]+)\((.*)\)\s*WITH\s*\((.*)\);?",
            sql_content,
            re.IGNORECASE | re.DOTALL,
        ):
            return match.groups()
        logger.error(f"Kon CREATE TABLE structuur niet parsen {sql_content}")
        return None

    def _split_columns_and_constraints(
        self, cols_block: str
    ) -> tuple[list[str], list[str]]:
        """
        Splitst kolom- en constraint-definities uit een CREATE TABLE statement.

        Deze methode scheidt kolomdefinities van constraints, inclusief inline en tabelniveau constraints.

        Args:
            cols_block (str): De string met kolom- en constraint-definities.

        Returns:
            tuple[list[str], list[str]]: Een tuple met een lijst van kolomdefinities en een lijst van constraints.
        """
        parts = self._split_top_level_commas(cols_block)
        columns = []
        constraints = []
        constraint_keywords = [
            "CONSTRAINT",
            "PRIMARY KEY",
            "UNIQUE",
            "CHECK",
            "FOREIGN KEY",
        ]
        for part in parts:
            if self._is_table_constraint(
                part=part, constraint_keywords=constraint_keywords
            ):
                constraints.append(part)
            elif not self._has_inline_constraint(
                part, constraint_keywords[1:], columns, constraints
            ):
                columns.append(part)
        return columns, constraints

    def _is_table_constraint(self, part: str, constraint_keywords: list[str]) -> bool:
        """
        Bepaalt of een string een tabelconstraint is in een CREATE TABLE statement.

        Deze methode controleert of het tekstdeel begint met een van de constraint-keywords.

        Args:
            part (str): Het tekstdeel dat gecontroleerd moet worden.
            constraint_keywords (list[str]): Lijst met constraint-keywords.

        Returns:
            bool: True als het een tabel-constraint is, anders False.
        """
        part_upper = part.upper()
        return any(part_upper.startswith(kw) for kw in constraint_keywords)

    def _has_inline_constraint(
        self,
        part: str,
        inline_keywords: list[str],
        columns: list[str],
        constraints: list[str],
    ) -> bool:
        """
        Controleert of een kolomdefinitie een inline constraint bevat en splitst deze indien nodig.

        Deze methode voegt het kolomdeel en het constraint-deel toe aan de respectievelijke lijsten als een inline constraint wordt gevonden.

        Args:
            part (str): De kolomdefinitie die gecontroleerd moet worden.
            inline_keywords (list[str]): Lijst met inline constraint-keywords.
            columns (list[str]): Lijst waarin kolomdefinities worden verzameld.
            constraints (list[str]): Lijst waarin constraints worden verzameld.

        Returns:
            bool: True als er een inline constraint is gevonden, anders False.
        """
        part_upper = part.upper()
        for kw in inline_keywords:
            if kw in part_upper:
                idx = part_upper.index(kw)
                col_def = part[:idx].strip()
                constraint_def = part[idx:].strip()
                if col_def:
                    columns.append(col_def)
                if constraint_def:
                    constraints.append(part)
                return True
        return False

    def _format_columns(self, columns: list[str]) -> list[str]:
        """
        Formatteert een lijst van kolomdefinities tot uitgelijnde strings.

        Deze methode bepaalt de maximale breedte van kolomnamen en types en zorgt voor nette uitlijning van alle kolommen.

        Args:
            columns (list[str]): Lijst met kolomdefinities als strings.

        Returns:
            list[str]: Lijst met geformatteerde en uitgelijnde kolomdefinities.
        """
        parsed_cols = []
        max_name_len = 0
        max_type_len = 0
        for col in columns:
            name, col_type, rest = self._split_column_parts(col)
            col_type = col_type.upper()
            if col_type == "DATETIME" and rest == "2":
                col_type = "DATETIME2"
                rest = ""
            max_name_len = max(max_name_len, len(name))
            max_type_len = max(max_type_len, len(col_type))
            parsed_cols.append((name, col_type, rest))

        column_lines = [
            f"    {name.ljust(max_name_len)}  {col_type.ljust(max_type_len)}  {rest}".rstrip()
            for name, col_type, rest in parsed_cols
        ]
        return column_lines

    def _split_column_parts(self, col: str) -> tuple[str, str, str]:
        """
        Splitst een kolomdefinitie in naam, type en overige specificaties.

        Deze methode retourneert de kolomnaam, het kolomtype en de rest van de kolomdefinitie als aparte strings.

        Args:
            col (str): De kolomdefinitie als string.

        Returns:
            tuple[str, str, str]: Een tuple bestaande uit (naam, type, rest).
        """
        # Regex: first word = name, type = everything up to first constraint keyword or end, rest = remainder
        # This regex assumes constraints start with NOT|NULL|DEFAULT|PRIMARY|UNIQUE|CHECK|REFERENCES|CONSTRAINT
        match = re.match(r"^(\S+)\s+((?:[A-Za-z]+\s*)+)(.*)$", col)
        if not match:
            # fallback: only name present
            return col.strip(), "", ""
        col_name = match[1]
        col_type = match[2].strip()
        col_rest = match[3].strip()
        return col_name, col_type, col_rest

    def _format_with_block(self, with_block: str):
        with_parts = self._split_top_level_commas(with_block)
        return [f"    {p}" for p in with_parts]


if __name__ == "__main__":
#     raw_sql_view = """
# CREATE VIEW [DA_Central].[vw_src_SL_DMS_Declaration] AS
# SELECT
#     [DeclarationVersion] = o1255.[VERSIONNUMBER],
#     [Declaration] = o1255.[REFERENCE],
#     [DeclarationProcedureCategory] = o1255.[PROCEDURECATEGORY],
#     [AcceptanceDate] = o1292.[FirstAcceptanceDate],
#     [DeclarationTID] = o1255.[TID],
#     [IsDeclarationCurrentVersion] = CASE WHEN o1255.VERSIONNUMBER = o1300.DeclarationVersion THEN 1 ELSE 0 END,
#     [DeclarationCurrentStatus] = o1268.[TYPE],
#     [ProcessingStatusCode] = o1268.[TYPE],
#     [X_StartDate] = CAST(GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time' AS DATE),
#     [X_EndDate] = CAST('2099-12-31' AS DATE),
#     None,
#     [X_IsCurrent] = 1,
#     [X_IsReplaced] = 0,
#     [X_RunId] = '',
#     [X_LoadDateTime] = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time',
#     [X_Bron] = 'DMS'
#     FROM [SL_DMS].[DECLARTN] AS o1255
#     LEFT JOIN [DA_Central].[AggrLastStatusVersion] AS o1259
#         ON                 o1259.[Declaration] = o1381.[reference]
#     LEFT JOIN [SL_DMS].[PRCSSTUS] AS o1268
#         ON                 o1268.[declaration_reference] = o1381.[REFERENCE]
# AND
#                 o1268.[versionNumber] = o1259.[StatusVersionNumber]
#     LEFT JOIN [DA_Central].[AggrFirstTimeStatus] AS o1292
#         ON                 o1292.[Declaration] = o1381.[reference]
#     LEFT JOIN [DA_Central].[AggrDeclarationMaxVersion] AS o1300
#         ON                 o1300.[Declaration] = o1381.[reference]
# AND
#                 o1300.[DeclarationVersion] = o1381.[versionNumber]
# WHERE
#     1 = 1             AND                 o1268.[TYPE]

#             NOT IN ('1','2')
#     """
#     print(SqlFormatter().format(raw_sql_view))


    raw_sql_view2 = """
    CREATE VIEW [DA_Central].[vw_src_SL_DTO_ActivityCode] AS
    SELECT [ActivityCode] = o4562.[ELEMKD]
        ,  [ActivityCodeStartDate] = o4562.[INGDAT]
        ,  [ActivityCodeEndDate] = o4562.[LDGDAT]
        ,  [ActivityCodeRegistrationDate] = o4562.[REGDAT]
        ,  [ActivityCodeTimestamp] = o4562.[TIMESTMP]
        ,  [ActivityCodeDescriptionShort] = o4562.[OMSCHR_K]
        ,  [ActivityCodeDescriptionLong] = o4562.[OMSCHR_L]
        ,  [ActivityCodeDescriptionLegal] = o4562.[OMSCHR_W]
        ,  [X_StartDate] = CAST(GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. EUROPE STANDARD TIME' AS DATE)
        ,  [X_EndDate] = CAST('2099-12-31' AS DATE)
        ,  [X_HashKey] = CHECKSUM(CONCAT(N'', o4562.[ELEMKD], o4562.[INGDAT], o4562.[LDGDAT], o4562.[REGDAT], o4562.[TIMESTMP], o4562.[OMSCHR_K], o4562.[OMSCHR_L], o4562.[OMSCHR_W], 'DTO'))
        ,  [X_IsCurrent] = 1
        ,  [X_IsReplaced] = 0
        ,  [X_RunId] = ''
        ,  [X_LoadDateTime] = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. EUROPE STANDARD TIME', [X_Bron] = 'DTO'
    FROM [SL_DTO].[ELEMENT] AS o4562
    WHERE 1 = 1
    AND o4562.[TABNR_8] = 'F26'
    """
    print(SqlFormatter().format(raw_sql_view2))

    raw_sql_table = """
        CREATE TABLE [DA_Central].[AdditionalReference] (
        [AdditionalReferenceKey]               BIGINT IDENTITY  (1, 1) NOT NULL
    ,    [AdditionalReferenceCode]              NVARCHAR         (8)
    ,    [AdditionalReferenceStartDate]         DATE
    ,    [AdditionalReferenceEndDate]           DATE
    ,    [AdditionalReferenceRegistrationDate]  DATE
    ,    [AdditionalReferenceTimestamp]         DATETIME2
    ,    [AdditionalReferenceDescriptionShort]  NVARCHAR         (20)
    ,    [AdditionalReferenceDescriptionLong]   NVARCHAR         (70)
    ,    [AdditionalReferenceDescriptionLegal]  NVARCHAR         (1024)
    ,    [X_Startdate]                          DATE
    ,    [X_EndDate]                            DATE
    ,    [X_HashKey]                            INT
    ,    [X_IsCurrent]                          BIT
    ,    [X_IsReplaced]                         BIT
    ,    [X_RunId]                              NVARCHAR         (100)
    ,    [X_LoadDateTime]                       DATETIME
    ,    [X_Bron]                               NVARCHAR         (10)
    ,    CONSTRAINT [AdditionalReference_PK] PRIMARY KEY NONCLUSTERED ([AdditionalReferenceKey])NOT ENFORCED
    ,    CONSTRAINT [AdditionalReference_UK] UNIQUE NONCLUSTERED([AdditionalReferenceCode]) NOT ENFORCED
    )
    WITH (
        DISTRIBUTION = ROUND_ROBIN,
        HEAP
    );"""
    print(SqlFormatter().format(raw_sql_table))