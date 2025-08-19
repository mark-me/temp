import re


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
        result = []
        level = 0
        buffer = ""
        for char in sql:
            if char == "(":
                level += 1
            elif char == ")":
                level -= 1
            if char == "," and level == 0:
                buffer += char + "\n"
                result.append(buffer)
                buffer = ""
            else:
                buffer += char
        if buffer:
            result.append(buffer)
        return "".join(result)

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
        sql = self._insert_newlines(sql)
        sql = self._split_top_level_commas(sql)
        sql = self._apply_indentation(sql)
        return sql

    def _format_create_table(self, sql: str) -> str:
        """
        Formatteert een CREATE TABLE-statement voor betere leesbaarheid en structuur.

        Voegt nieuwe regels toe, splitst kolommen en past inspringing toe op het statement.

        Args:
            sql (str): Het CREATE TABLE-statement dat verwerkt moet worden.

        Returns:
            str: Het geformatteerde CREATE TABLE-statement.
        """
        sql = self._insert_newlines(sql)
        sql = self._split_top_level_commas(sql)
        sql = self._apply_indentation(sql)
        return sql

if __name__ == "__main__":
    raw_sql = """
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
    None,
    [X_IsCurrent] = 1,
    [X_IsReplaced] = 0,
    [X_RunId] = '',
    [X_LoadDateTime] = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time',
    [X_Bron] = 'DMS'
    FROM [SL_DMS].[DECLARTN] AS o1255
    LEFT JOIN [DA_Central].[AggrLastStatusVersion] AS o1259
        ON                 o1259.[Declaration] = o1381.[reference]
    LEFT JOIN [SL_DMS].[PRCSSTUS] AS o1268
        ON                 o1268.[declaration_reference] = o1381.[REFERENCE]
AND
                o1268.[versionNumber] = o1259.[StatusVersionNumber]
    LEFT JOIN [DA_Central].[AggrFirstTimeStatus] AS o1292
        ON                 o1292.[Declaration] = o1381.[reference]
    LEFT JOIN [DA_Central].[AggrDeclarationMaxVersion] AS o1300
        ON                 o1300.[Declaration] = o1381.[reference]
AND
                o1300.[DeclarationVersion] = o1381.[versionNumber]
WHERE
    1 = 1             AND                 o1268.[TYPE]

            NOT IN ('1','2')
    """
    print(SqlFormatter().format(raw_sql))