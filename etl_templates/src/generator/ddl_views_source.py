from pathlib import Path

import sqlparse
from jinja2 import Template
from logtools import get_logger

from .ddl_views_base import DDLViewBase

logger = get_logger(__name__)


class DDLSourceViews(DDLViewBase):
    def __init__(self, dir_output: str, ddl_template: Template):
        super().__init__(dir_output=dir_output, ddl_template=ddl_template)

    def generate_ddls(self, mappings: list):
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
            identifiers = self.__collect_identifiers(mapping=mapping)
            mapping = self.__build_bkeys_load(identifiers=identifiers, mapping=mapping)
            mapping = self.__remove_identifier_attributes(
                identifiers=identifiers, mapping=mapping
            )
            content = self.__render_source_view(mapping)
            dir_output, file_output, path_file_output = self.__get_source_view_paths(
                mapping
            )
            self.save_generated_object(
                content=content, path_file_output=path_file_output
            )
            logger.info(f"Written Source view DDL {Path(path_file_output).resolve()}")

    def __collect_identifiers(self, mapping: dict) -> dict:
        """
        Verzamelt identifier-informatie uit de mappingconfiguratie.

        Doorloopt alle mappings en attribute mappings, en genereert een dictionary met identifierdetails voor gebruik in DDL-generatie.

        Args:
            mappings (dict): De mappingconfiguratie met entity- en attributemappinginformatie.

        Returns:
            dict: Een dictionary met identifierdetails per identifier.
        """
        # TODO: in __select_identifiers zit nu opbouw van strings die platform specifiek zijn (SSMS). Om de generator ook platform onafhankelijk te maken kijken of we dit wellicht in een template kunnen gieten.
        identifiers = {}

        def get_name_business_key(identifier):
            return (
                identifier["EntityCode"]
                if identifier["IsPrimary"]
                else identifier["Code"]
            )

        def get_identifier_def(name_business_key, mapping, attr_map):
            if "AttributesSource" in attr_map:
                id_entity = attr_map["AttributesSource"]["EntityAlias"]
                attribute_source = attr_map["AttributesSource"]["Code"]
                return f"[{name_business_key}BKey] = '{mapping['DataSource']}'+ '-' + CAST({id_entity}.[{attribute_source}] AS NVARCHAR(50))"
            else:
                return f"[{name_business_key}BKey] = '{mapping['DataSource']}'+  '-' + {attr_map['Expression']}"

        if (
            "Identifiers" not in mapping["EntityTarget"]
            and mapping["EntityTarget"]["Stereotype"] != "mdde_AggregateBusinessRule"
        ):
            logger.error(
                f"Geen identifiers aanwezig voor doel entiteit '{mapping['EntityTarget']['Name']}'"
            )
        if (
            "AttributeMapping" not in mapping
            and mapping["EntityTarget"]["Stereotype"] != "mdde_AggregateBusinessRule"
        ):
            logger.error(
                f"Geen attribute mapping aanwezig voor entity '{mapping['EntityTarget']['Name']}'"
            )
        for identifier in mapping["EntityTarget"]["Identifiers"]:
            for attr_map in mapping["AttributeMapping"]:
                if (
                    attr_map["AttributeTarget"]["IdEntity"] == identifier["EntityID"]
                    and attr_map["AttributeTarget"]["Code"] == identifier["Name"]
                ):
                    name_business_key = get_name_business_key(identifier)
                    identifier_def = get_identifier_def(
                        name_business_key, mapping, attr_map
                    )

                    identifiers[identifier["Id"]] = {
                        "IdentifierID": identifier["Id"],
                        "IdentifierName": identifier["Name"],
                        "IdentifierCode": identifier["Code"],
                        "EntityId": identifier["EntityID"],
                        "EntityCode": identifier["EntityCode"],
                        "IsPrimary": identifier["IsPrimary"],
                        "IdentifierStringSourceView": identifier_def,
                    }
        return identifiers

    def __remove_identifier_attributes(self, mapping: dict, identifiers: dict):
        """
            Verwijdert alle identifier kolommen uit de attribute mapping om alleen de bkeys over te houden

        Args:
            identifiers (dict): Alle identifiers definities
            mapping (dict): mapping
        """
        mapped_identifiers = []
        for identifier in mapping["EntityTarget"]["Identifiers"]:
            if "Id" not in identifier:
                logger.error("Geen identifier gevonden!")
                continue
            identifier_id = identifier["Id"]
            if identifier_id in identifiers:
                # voeg de code van de identifier toe aan een controlelijst. De attributen in deze lijst worden verwijderd uit entity[Attributes]
                mapped_identifiers.append(identifiers[identifier_id]["IdentifierName"])
            elif "Stereotype" not in mapping:
                """
                We doen niks met eventuele identifiers van Aggregators. Dit moet geen error opleveren.
                Alleen identifiers van echte entiteiten worden gebruikt en moet aanwezig zijn.
                Deze entiteiten hebben hier geen Stereotype
                """
                logger.error(
                    f"Identifier voor entiteit '{mapping['EntityTarget']['Code']}' niet gevonden in identifiers"
                )

        attributes = []
        # voor alle attributen in de entity gaan we controleren of de code voorkomt als gemapte identifier. Indien dit het geval is, dan wordt het
        # attribuut verwijderd uit Attributes. Hiermee krijgen we geen dubbelingen in de entiteit.
        attributes.extend(
            attribute
            for attribute in mapping["AttributeMapping"]
            if attribute["AttributeTarget"]["Name"] not in mapped_identifiers
        )
        mapping.pop("AttributeMapping")
        mapping["AttributeMapping"] = attributes
        return mapping

    def __render_source_view(self, mapping: dict) -> str:
        """
        Genereert en formatteert de SQL voor een source view op basis van de mapping.

        Deze methode rendert de source view met behulp van de Jinja2 template en formatteert de SQL met sqlparse.

        Args:
            mapping (dict): De mapping die gebruikt wordt om de source view te genereren.

        Returns:
            str: De geformatteerde SQL-string voor de source view.
        """
        mapping["Name"] = f"{mapping['Name'].replace(' ', '_')}"
        content = self.template.render(mapping=mapping)
        content = sqlparse.format(content, reindent=True, keyword_case="upper")
        return content

    def __get_source_view_paths(self, mapping: dict) -> tuple:
        """
        Bepaalt de outputpaden voor het opslaan van een gegenereerde source view DDL.

        Deze methode genereert het output directory-pad, de bestandsnaam en het volledige pad voor de source view op basis van de mapping.

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

        Deze methode genereert de benodigde BKey-strings en de hash-key voor de mapping, zodat deze gebruikt kunnen worden in de DDL-templates.

        Args:
            identifiers (dict): Alle identifiers definities.
            mapping (dict): De mapping waarvoor de BKeys en hash-key worden opgebouwd.

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
                return f"{hash_attrib}{attr_mapping['AttributesSource']['EntityAlias']}.[{attr_mapping['AttributesSource']['Code']}])"

        x_hashkey = "[X_HashKey] = CHECKSUM(CONCAT(N'',"
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
