from pathlib import Path

import sqlparse
from jinja2 import Template
from logtools import get_logger

from .ddl_views_base import DDLViewBase

logger = get_logger(__name__)


class DDLSourceViews(DDLViewBase):
    def __init__(self, dir_output: str, ddl_template: Template):
        super().__init__(dir_output=dir_output, ddl_template=ddl_template)

    def generate_ddls(self, mappings: dict, identifiers: dict):
        """
        Creëert alle source views van de verschillende niet-aggregatie entiteiten die in models zijn opgenomen en schrijft deze weg naar een folder in de repository.
        De source views bevatten de ETL om de doeltabel te vullen met data.

        Args:
            mappings (dict): Bevat alle mappings uit een RETW bestand
            identifiers (dict): De JSON (RETW Output) geconverteerd naar een dictionary
        """
        for mapping in mappings:
            if mapping["EntityTarget"]["Stereotype"] == "mdde_AggregateBusinessRule":
                continue

            self._set_datasource_code(mapping)
            mapping = self.__build_bkeys_load(identifiers=identifiers, mapping=mapping)
            content = self.__render_source_view(mapping)
            dir_output, file_output, path_file_output = self.__get_source_view_paths(
                mapping
            )
            self.save_generated_object(
                content=content, path_file_output=path_file_output
            )
            logger.info(f"Written Source view DDL {Path(path_file_output).resolve()}")

    def __render_source_view(self, mapping: dict) -> str:
        """
        Genereert en formatteert de SQL voor een source view op basis van de mapping.

        Deze methode rendert de source view met behulp van de Jinja2 template en formatteert de SQL met sqlparse.

        Args:
            mapping (dict): De mapping die gebruikt wordt om de source view te genereren.

        Returns:
            str: De geformatteerde SQL-string voor de source view.
        """
        content = self.template.render(mapping=mapping)
        return sqlparse.format(content, reindent=True, keyword_case="upper")

    def __get_source_view_paths(self, mapping: dict) -> tuple:
        """
        Bepaalt de outputpaden voor het opslaan van een gegenereerde source view DDL.

        Deze methode genereert het outputdirectorypad, de bestandsnaam en het volledige pad voor de source view op basis van de mapping.

        Args:
            mapping (dict): De mapping die gebruikt wordt om de paden te bepalen.

        Returns:
            tuple: (dir_output, file_output, path_file_output)
        """
        dir_output = Path(
            f"{self.dir_output}/{mapping['EntityTarget']['CodeModel']}/Views/"
        )
        dir_output.mkdir(parents=True, exist_ok=True)
        file_output = f"vw_src_{mapping['Name']}.sql"
        path_file_output = f"{dir_output}/{file_output}"
        return dir_output, file_output, path_file_output

    def __build_bkeys_load(self, identifiers: dict, mapping: dict):
        """
        Bouwt de business keys (BKeys) en de X_HashKey voor een mapping op basis van de identifiers en attributen.

        Deze methode genereert de benodigde BKey-strings en de hashkey voor de mapping, zodat deze gebruikt kunnen worden in de DDL-templates.

        Args:
            identifiers (dict): Alle identifiers definities.
            mapping (dict): De mapping waarvoor de BKeys en hashkey worden opgebouwd.

        Returns:
            dict: De aangepaste mapping met toegevoegde BKeys en X_HashKey.
        """
        mapping["Identifiers"] = self.__get_identifier_mapped(identifiers, mapping)
        mapping["AttributeMapping"], mapping["X_Hashkey"] = self.__build_x_hashkey(
            mapping
        )
        return mapping

    def __get_identifier_mapped(self, identifiers: dict, mapping: dict):
        """
        Bepaalt de gemapte identifiers voor de mapping.

        Args:
            identifiers (dict): Alle identifiers definities.
            mapping (dict): De mapping waarvoor de identifiers worden bepaald.

        Returns:
            list: Een lijst met gemapte identifier strings.
        """
        mapped_identifiers = []
        identifier_mapped = []
        for identifier in mapping["EntityTarget"]["Identifiers"]:
            identifier_id = identifier["Id"]
            if identifier_id in identifiers:
                identifier_mapped.append(
                    identifiers[identifier_id]["IdentifierStringSourceView"]
                )
                mapped_identifiers.append(identifiers[identifier_id]["IdentifierName"])
            else:
                logger.error(
                    f"identifier voor {mapping['EntityTarget']['Code']} niet gevonden in identifiers"
                )
        return identifier_mapped

    def __build_x_hashkey(self, mapping: dict) -> tuple:
        """
        Bouwt de X_HashKey string en retourneert deze samen met de aangepaste AttributeMapping.

        Args:
            mapping (dict): De mapping waarvoor de X_HashKey wordt opgebouwd.

        Returns:
            tuple: (AttributeMapping, X_Hashkey)
        """

        def build_hash_attrib(attr_mapping: list, separator: str) -> str:
            hash_attrib = f"{separator}DA_MDDE.fn_IsNull("
            if "Expression" in attr_mapping:
                return f"{hash_attrib}{attr_mapping['Expression']})"
            else:
                return f"{hash_attrib}{attr_mapping['AttributesSource']['IdEntity']}.[{attr_mapping['AttributesSource']['Code']}])"

        x_hashkey = "[X_HashKey] = HASHBYTES('SHA2_256', CONCAT("
        attr_mappings = []
        for i, attr_mapping in enumerate(mapping["AttributeMapping"]):
            separator = "" if i == 0 else ","
            hash_attrib = build_hash_attrib(
                attr_mapping=attr_mapping, separator=separator
            )
            x_hashkey = x_hashkey + hash_attrib
            attr_mappings.append(attr_mapping)
        x_hashkey = f"{x_hashkey},'{mapping['DataSource']}'))"
        return attr_mappings, x_hashkey
