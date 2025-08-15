from enum import Enum
from pathlib import Path
import re

import sqlparse
from jinja2 import Environment, FileSystemLoader
from logtools import get_logger

logger = get_logger(__name__)


class DDLType(Enum):
    """Enumerates the types of vertices in the graph.

    Provides distinct identifiers for each type of node in the graph, including entities, mappings, and files.
    """

    SCHEMA = "create_schema.sql"
    TABLE = "create_table.sql"
    ENTITY = "create_entity.sql"
    VIEW = "create_view.sql"
    PROCEDURE = "create_procedure.sql"
    SOURCE_VIEW = "create_source_view.sql"
    SOURCE_VIEW_AGGR = "create_source_view_agg.sql"


class DDLGeneratorBase:
    def __init__(self, path_output: Path, platform: str, ddl_type: DDLType):
        """
        Initialiseert een DDLViews instantie voor het genereren van DDL-bestanden voor views.

        Deze constructor stelt de outputdirectory en de te gebruiken Jinja2-template in voor het genereren van DDL's.

        Args:
            dir_output (str): De directory waarin de DDL-bestanden worden opgeslagen.
            ddl_template (Template): De Jinja2-template die gebruikt wordt voor het renderen van de DDL.
        """
        self.platform = platform
        self.dir_templates = Path(__file__).parent / "templates" / platform
        self.ddl_type = ddl_type
        self.path_output = path_output
        self.template = self._get_template()
        self.files_generated = []

    def _get_template(self):
        environment = Environment(
            loader=FileSystemLoader(self.dir_templates),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        return environment.get_template(self.ddl_type.value)

    def save_generated_object(self, content: str, path_file_output: str) -> None:
        """
        Slaat de gegenereerde source view DDL op in het opgegeven pad en registreert het bestand in de DDL-lijst.

        Deze methode schrijft de geformatteerde SQL naar een bestand en voegt het bestand toe aan de lijst van aangemaakte DDL's.

        Args:
            content (str): De te schrijven SQL-inhoud.
            path_file_output (str): Het volledige pad waar de source view wordt opgeslagen.
        """
        if self.platform in ["dedicated-pool"]:
            content = self.format_sql(sql_content=content)

        with open(path_file_output, mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)

    def format_sql(self, sql_content: str) -> str:
        """Formatteert SQL statement(s)

        Args:
            sql_content (str): SQL statement(s) die geformatteerd dienen te worden

        Returns:
            str: Geformatteerd(e) SQL statement(s)
        """
        formatted = sqlparse.format(
            sql_content, reindent=True, comma_first=True, keyword_case="upper"
        )
        if self.ddl_type == DDLType.ENTITY:
            formatted = self._format_create_table(sql_content=formatted)
        return formatted

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

    def _normalize_whitespace(self, sql_content: str) -> str:
        return re.sub(r"\s+", " ", sql_content).strip()

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
        Splitst kolom- en constraintdefinities uit een CREATE TABLE statement.

        Deze methode scheidt kolomdefinities van constraints, inclusief inline en tabelniveau constraints.

        Args:
            cols_block (str): De string met kolom- en constraintdefinities.

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
            bool: True als het een tabelconstraint is, anders False.
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

        Deze methode voegt het kolomdeel en het constraintdeel toe aan de respectievelijke lijsten als een inline constraint wordt gevonden.

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

    def _split_top_level_commas(self, text: str) -> list[str]:
        """
        Splitst een string op komma's die zich op het hoogste niveau van haakjes bevinden.

        Deze methode zorgt ervoor dat komma's binnen geneste haakjes genegeerd worden, zodat alleen de buitenste elementen gesplitst worden.

        Args:
            text (str): De tekst die gesplitst moet worden.

        Returns:
            list[str]: Een lijst van gesplitste stringdelen.
        """
        parts = []
        current = []
        depth = 0
        for ch in text:
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
