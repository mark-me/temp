import json
import os
from pathlib import Path

import sqlparse
from jinja2 import Environment, FileSystemLoader

from log_config import logging

logger = logging.getLogger(__name__)


class DDLGenerator:
    """Class DDLGenerator genereert DDL en ETL vanuit de door RETW gemaakte Json."""

    def __init__(self, params: dict):
        """Initialiseren van de Class DDLGenerator. Hiervoor wordt de config.yml uitgelezen om parameters
        mee te kunnen geven. Ook wordt de flow georkestreerd waarmee het Json-bestand uitgelezen wordt
        en omgezet kan worden naar DDL en ETL bestanden

        Args:
            params (dict): Bevat alle parameters vanuit config.yml
        """
        logger.info("Initializing Class: 'DDLGenerator'.")
        self.params = params
        self.dir_generator = self.params.dir_generate
        self.source_layer_prefix = "SL_"
        self.schema_post_deploy = "DA_MDDE"
        self.templates = self.__get_templates(dir_templates=params.dir_templates)
        self.created_ddls = {
            "Folder Include": [],
            "Build Include": [],
            "None Include": [],
        }

    def __read_model_file(self, file_RETW: str) -> dict:
        """Leest het in  de config opgegeven Json-bestand in en slaat de informatie op in een dictionary

        Returns:
            dict_models (dict): De JSON (RETW Output) geconverteerd naar een dictionary
        """
        p = Path(file_RETW).resolve()
        logger.info(f"Filepath MDDE Json file: {p}")
        # Function not yet used, but candidate for reading XML file
        with open(file_RETW) as json_file:
            dict_model = json.load(json_file)
        return dict_model

    def __get_templates(self, dir_templates: str) -> dict:
        """
        Haal alle templates op uit de template folder. De locatie van deze folder is opgeslagen in de config.yml

        Return:
            dict_templates (dict): Bevat alle beschikbare templates en de locatie waar de templates te vinden zijn
        """
        # Loading templates
        environment = Environment(
            loader=FileSystemLoader(dir_templates),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        dict_templates = {
            "schema": environment.get_template("create_schema.sql"),
            "Tables": environment.get_template("create_table.sql"),
            "Entities": environment.get_template("create_entity.sql"),
            "Views": environment.get_template("create_view.sql"),
            "Procedures": environment.get_template("create_procedure.sql"),
            "SourceViews": environment.get_template("create_source_view.sql"),
            "SourceViewsaggr": environment.get_template("create_source_view_agg.sql"),
            "PostDeploy_Config": environment.get_template(
                "Create MDDE_PostDeployScript_Config.sql"
            ),
            "PostDeploy_CodeList": environment.get_template(
                "Create MDDE_PostDeployScript_CodeList.sql"
            ),
        }
        return dict_templates

    def generate_code(self, file_RETW: dict, mapping_order: list):
        """
        Overkoepelende functie waarin alle functions worden gestart om de diverse objecttypes op te bouwen

        Args:
            model (dict): De JSON (RETW Output) geconverteerd naar een dictionary
            templates (dict): Bevat alle beschikbare templates en de locatie waar de templates te vinden zijn
        """
        # self.__copy_mdde_scripts()\
        dict_RETW = self.__read_model_file(file_RETW=file_RETW)
        identifiers = {}
        if "Mappings" in dict_RETW:
            mappings = dict_RETW["Mappings"]
            identifiers = self.__select_identifiers(mappings=mappings)
        self.__write_ddl_entities(models=dict_RETW["Models"], identifiers=identifiers)
        if "Mappings" in dict_RETW:
            self.__write_ddl_source_view_aggr(mappings=mappings)
            self.__write_ddl_source_view(mappings=mappings, identifiers=identifiers)
        self.__write_ddl_MDDE_PostDeploy_Config(mapping_order=mapping_order)
        self.__write_ddl_MDDE_PostDeploy_CodeTable()

    def __select_identifiers(self, mappings: dict) -> dict:
        """
        Haalt alle identifiers op uit het model ten behoeve van de aanmaken van BKeys in de entiteiten en DDL's

        Args:
            models (dict): de JSON (RETW Output) geconverteerd naar een dictionary
        Returns:
            identifiers (dict): een dictionary met daarin alle informatie van de identifier benodigd voor het aanmaken van BKeys
        """
        # TODO: in __select_identifiers zit nu opbouw van strings die platform specifiek zijn (SSMS). Om de generator ook platform onafhankelijk te maken kijken of we dit wellicht in een template kunnen gieten.
        identifiers = {}
        for mapping in mappings:
            if "Identifiers" not in mapping["EntityTarget"]:
                logger.error(
                    f"Geen identifiers aanwezig voor entitytarget {mapping['EntityTarget']['Name']}"
                )
                continue
            for identifier in mapping["EntityTarget"]["Identifiers"]:
                if "AttributeMapping" not in mapping:
                    logger.error(
                        f"Geen attribute mapping aanwezig voor entity {mapping['EntityTarget']['Name']}"
                    )
                    continue
                for attr_map in mapping["AttributeMapping"]:
                    # selecteer alleen het attribuut uit AttributeTarget die is aangemerkt als identifier
                    # TODO: Aangepaste filter, omdat deze issue gaf bij de regels 213+214 in de opbouw, doordat de Key niet bestond.
                    if (
                        attr_map["AttributeTarget"]["IdEntity"]
                        == identifier["EntityID"]
                        and attr_map["AttributeTarget"]["Code"] == identifier["Name"]
                    ):
                        name_business_key = (
                            identifier["EntityCode"]
                            if identifier["IsPrimary"]
                            else identifier["Code"]
                        )
                        identifier_def_primary = (
                            f"[{name_business_key}BKey] nvarchar(200) NOT NULL"
                        )
                        if "AttributesSource" in attr_map:
                            # Als het een primary key is dan nemen we als naamgeving van de BKey de code van de Entity over en zetten hier de postfix BKey achter
                            # Is de identifier geen primary key, dan nemen we als naamgeving van de BKey de code van de AttributeTarget over en zetten hier de postfix Bkey achter
                            id_entity = attr_map["AttributesSource"]["IdEntity"]
                            attribute_source = attr_map["AttributesSource"]["Code"]
                            identifier_def = f"[{name_business_key}BKey] = '{mapping['DataSource']}'+ '-' + CAST({id_entity}.[{attribute_source}] AS NVARCHAR(50))"
                        # Als er geen AttributeSource beschikbaar is in AttributeMapping hebben we de maken met een expression. De Bkey wordt hiervoor anders opgebouwd
                        else:
                            # Als het een primary key is dan nemen we als naamgeving van de BKey de code van de Entity over en zetten hier de postfix  BKey achter
                            # Is de identifier geen primary key, dan nemen we als naamgeving van de BKey de code van de AttributeTarget over en zetten hier de postfix BKey achter
                            identifier_def = f"[{name_business_key}BKey] = '{mapping['DataSource']}'+  '-' + {attr_map['Expression']}"

                        identifiers[identifier["Id"]] = {
                            "IdentifierID": identifier["Id"],
                            "IdentifierName": identifier["Name"],
                            "IdentifierCode": identifier["Code"],
                            "EntityId": identifier["EntityID"],
                            "EntityCode": identifier["EntityCode"],
                            "IsPrimary": identifier["IsPrimary"],
                            "IdentifierStringEntity": identifier_def_primary,
                            "IdentifierStringSourceView": identifier_def,
                        }
        return identifiers

    def __write_ddl_entities(self, models: dict, identifiers: dict):
        """
        Creëert alle DDL's van de verschillende entiteiten die in models zijn opgenomen en schrijft deze weg naar een folder in de repository

        Args:
            identifiers (dict): Bevat alle business keys definities
        """
        for model in models:
            if not model["IsDocumentModel"]:
                continue
            dir_output = Path(
                f"{self.dir_generator}/CentralLayer/{model['Code']}/Tables/"
            )
            for entity in model["Entities"]:
                if "Number" not in entity:
                    # Entity always needs attribute number. This is used for distribution
                    logger.warning(
                        f"Entiteit '{entity['Name']}' heeft geen property number, standaard distributie wordt gebruikt."
                    )
                    entity["Number"] = 0
                # TODO: Add filter out filter in entity
                # TODO: Add CodeModel to Json and not here
                entity["CodeModel"] = model["Code"]
                # Bouw de Business keys op op basis van de identifiers bij de entity
                entity = self.__replace_entity_keys_with_bkeys(
                    entity=entity, identifiers=identifiers
                )
                # Bepaal welke template we gaan gebruiken en maak de DDL's aan voor de entity
                content = self.templates["Entities"].render(entity=entity)

                # Schrijf DDL naar bestand
                dir_output.mkdir(parents=True, exist_ok=True)
                file_output = f"{entity['Code']}.sql"
                path_output_file = f"{dir_output}/{file_output}"
                with open(path_output_file, mode="w", encoding="utf-8") as file_ddl:
                    file_ddl.write(content)
                # Add used folders to dict_created_ddls to be later used to add to the VS Project file
                self.__add_object_to_ddl(
                    code_model=model["Code"],
                    type_objects="Tables",
                    file_output=file_output,
                )
                logger.info(
                    f"Entity DDL weggeschreven naar {Path(path_output_file).resolve()}"
                )

    def __replace_entity_keys_with_bkeys(self, entity: dict, identifiers: dict):
        """Vervangt alle key kolommen met business key kolommen.

        Args:
            identifiers (dict): Alle identifiers definities
            entity (dict): Entiteit
        """
        mapped_identifiers = []
        identifier_mapped = []
        for identifier in entity["Identifiers"]:
            identifier_id = identifier["Id"]
            if identifier_id in identifiers:
                identifier_mapped.append(
                    identifiers[identifier_id]["IdentifierStringEntity"]
                )
                # voeg de code van de identifier toe aan een controlelijst. De attributen in deze lijst worden verwijderd uit entity[Attributes]
                mapped_identifiers.append(identifiers[identifier_id]["IdentifierName"])
            else:
                logger.error(
                    f"Identifier voor entiteit '{entity['Code']}' niet gevonden in identifiers"
                )
                # Voeg de complete lijst van identifiers toe aan de entity
        entity["Identifiers"] = identifier_mapped
        attributes = []
        # voor alle attributen in de entity gaan we controleren of de code voorkomt als gemapte identifier. Indien dit het geval is, dan wordt het
        # attribuut verwijderd uit Attributes. Hiermee krijgen we geen dubbelingen in de entiteit.
        for attribute in entity["Attributes"]:
            if attribute["Code"] not in mapped_identifiers:
                attributes.append(attribute)
        return entity

    def __add_object_to_ddl(self, code_model: str, type_objects: str, file_output: str):
        folder_model = code_model
        if folder_model not in self.created_ddls["Folder Include"]:
            self.created_ddls["Folder Include"].append(folder_model)
        folder_tables = f"{code_model}\\{type_objects}"
        if folder_tables not in self.created_ddls["Folder Include"]:
            self.created_ddls["Folder Include"].append(folder_tables)
        table_file = f"{folder_tables}\\{file_output}"
        if table_file not in self.created_ddls["Build Include"]:
            self.created_ddls["Build Include"].append(table_file)

    def __write_ddl_source_view_aggr(self, mappings: dict):
        """
        Creëert alle source views van de verschillende aggregatie entiteiten die in models zijn opgenomen en schrijft deze weg naar een folder in de repository.
        De source views bevatten de ETL om de doeltabel te vullen met data.

        Args:
            mappings (dict): Bevat alle mappings uit een RETW bestand
        """
        for mapping in mappings:
            if mapping["EntityTarget"]["Stereotype"] != "mdde_AggregateBusinessRule":
                continue

            if "DataSource" in mapping:
                datasource = mapping["DataSource"]
                mapping["DataSourceCode"] = (
                    datasource[3:]
                    if datasource[0:3] == self.source_layer_prefix
                    else datasource
                )
            else:
                logger.error(
                    f"Geen datasource opgegeven voor mapping {mapping['Name']}"
                )
            # Generate the DDL
            content = self.templates["SourceViewsaggr"].render(mapping=mapping)
            content = sqlparse.format(content, reindent=True, keyword_case="upper")

            # Saving the generated DDL
            dir_output = Path(
                f"{self.dir_generator}/CentralLayer/{mapping['EntityTarget']['CodeModel']}/Views/"
            )
            dir_output.mkdir(parents=True, exist_ok=True)
            file_output = f"vw_src_{mapping['Name']}.sql"
            self.__add_object_to_ddl(
                code_model=mapping["EntityTarget"]["CodeModel"],
                type_objects="Views",
                file_output=file_output,
            )
            path_file_output = f"{dir_output}/{file_output}"
            with open(path_file_output, mode="w", encoding="utf-8") as file_ddl:
                file_ddl.write(content)
            logger.info(
                f"Written Source view aggregation DDL {Path(path_file_output).resolve()}"
            )

    def __write_ddl_source_view(self, mappings: dict, identifiers: dict):
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

            if "DataSource" in mapping:
                datasource = mapping["DataSource"]
                mapping["DataSourceCode"] = (
                    datasource[3:]
                    if datasource[0:3] == self.source_layer_prefix
                    else datasource
                )
            else:
                logger.warning(
                    f"Geen datasource opgegeven voor mapping {mapping['Name']}"
                )

            # bouw de string voor de BKey op en geef deze mee aan de render voor de SourceView
            mapping = self.__build_bkeys_load(identifiers=identifiers, mapping=mapping)
            # Genereer DDL
            content = self.templates["SourceViews"].render(mapping=mapping)
            content = sqlparse.format(content, reindent=True, keyword_case="upper")

            # Saving the generated DDL
            dir_output = Path(
                f"{self.dir_generator}/CentralLayer/{mapping['EntityTarget']['CodeModel']}/Views/"
            )
            dir_output.mkdir(parents=True, exist_ok=True)
            file_output = f"vw_src_{mapping['Name']}.sql"
            self.__add_object_to_ddl(
                code_model=mapping["EntityTarget"]["CodeModel"],
                type_objects="Views",
                file_output=file_output,
            )
            path_file_output = f"{dir_output}/{file_output}"
            with open(path_file_output, mode="w", encoding="utf-8") as file_ddl:
                file_ddl.write(content)
            logger.info(f"Written Source view DDL {Path(path_file_output).resolve()}")

    def __build_bkeys_load(self, identifiers: dict, mapping: dict):
        mapped_identifiers = []
        identifier_mapped = []
        for identifier in mapping["EntityTarget"]["Identifiers"]:
            identifier_id = identifier["Id"]
            if identifier_id in identifiers:
                identifier_mapped.append(
                    identifiers[identifier_id]["IdentifierStringSourceView"]
                )
                # voeg de code van de identifier toe aan een controlelijst. De attributen in deze lijst worden verwijderd uit entity[Attributes]
                mapped_identifiers.append(identifiers[identifier_id]["IdentifierName"])
            else:
                logger.error(
                    f"identifier voor {mapping['EntityTarget']['Code']} niet gevonden in identifiers"
                )
            # Voeg de complete lijst van identifiers toe aan de entity
        mapping["Identifiers"] = identifier_mapped
        # De identifiers die zijn toegevoegd aan PrimaryKeys willen we niet meer in de mapping toevoegen. Omdat deze velden nog wel meegaan in de x_hashkey bouwen we tegelijkertijd dit attribuut op. Doen we dit niet, dan hebben we een incomplete hashkey in de template
        attr_mappings = []
        # TODO: afhandeling van x_hashkey bevat platform specifieke logica (in dit geval SSMS). Dit platform onafhankelijk maken door het bijvoorbeeld te gieten in een string_template
        # string van x_hashkey in basis. Hieraan voegen wij de hashattributen toe in de for loop die volgt
        x_hashkey = "[X_HashKey] = HASHBYTES('SHA2_256', CONCAT("
        for i, attr_mapping in enumerate(mapping["AttributeMapping"]):
            # eerste ronde van de for-loop, dan seperator leeg, anders komma (,)
            separator = "" if i == 0 else ","
            hash_attrib = f"{separator}DA_MDDE.fn_IsNull("
            if "Expression" in attr_mapping:
                hash_attrib = f"{hash_attrib}{attr_mapping['Expression']})"
            else:
                hash_attrib = f"{hash_attrib}{attr_mapping['AttributesSource']['IdEntity']}.[{attr_mapping['AttributesSource']['Code']}])"
            x_hashkey = x_hashkey + hash_attrib
            i += 1
            # we nemen alleen de non-identifier velden mee naar de attributemapping die de template ingaat.
            # TODO: Checken of dit problemen opleverd.
            attr_mappings.append(attr_mapping)
            # verwijder uit mapping het gedeelte van AttributeMapping. Deze voegen we opnieuw toe met de zojuist aangemaakte attributemappings
        mapping.pop("AttributeMapping")
        mapping["AttributeMapping"] = attr_mappings
        # voeg de x_hashkey als kenmerk toe aan de mapping
        mapping["X_Hashkey"] = x_hashkey + ",'" + mapping["DataSource"] + "'))"
        return mapping

    def __write_ddl_MDDE_PostDeploy_Config(self, mapping_order: list):
        """
        Creëert het post deploy script voor alle mappings opgenomen in de modellen. Voor elke mapping wordt een insert statement aangemaakt
        waarmee een record aangemaakt wordt in de tabel [DA_MDDE].[Config].
        de basis hiervoor is de DAG functie mapping_order

        Args:
            mapping_order (list) bevat alle mappingen en de volgorde van laden.
        """
        dir_output = f"{self.dir_generator}/CentralLayer/{self.schema_post_deploy}/PostDeployment/"
        file_output = "PostDeploy_MetaData_Config_MappingOrder.sql"
        file_output_master = "PostDeploy.sql"
        path_output_master = Path(
            f"{self.dir_generator}/CentralLayer/PostDeployment/{file_output_master}"
        )

        # Add used folders to self.dict_created_ddls to be later used to add to the VS Project file
        self.__add_post_deploy_to_ddl(
            file_output=file_output, file_output_master=file_output_master
        )

        # Fill Path with the destination directory. Path is used for file system operations
        directory = Path(dir_output)
        # Make directory if not exist.
        directory.mkdir(parents=True, exist_ok=True)
        content = self.templates["PostDeploy_Config"].render(config=mapping_order)
        with open(f"{dir_output}{file_output}", mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
        logger.info(
            f"Written MDDE PostDeploy_Config file {Path(dir_output + file_output).resolve()}"
        )

        # Add file to master file.
        if not path_output_master.is_file():
            f = open(path_output_master, "a")
            f.write("/* Post deploy master file. */\n")
            f.close()
        if path_output_master.is_file():
            fr = open(path_output_master, "r")
            if f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n" not in fr.read():
                fr.close()
                f = open(path_output_master, "a")
                f.write(
                    f"\nPRINT N'Running PostDeploy: ..\\DA_MDDE\\PostDeployment\\{file_output}'\n"
                )
                f.write(f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n")
                f.close()

    def __write_ddl_MDDE_PostDeploy_CodeTable(self):
        """
        Creëert het post deploy script voor de CodeTable. Voor elk record in de CodeList wordt een select
        statement gemaakt waarmee de data geladen kan worden in [DA_MDDE].[CodeList]

        Args:
            templates (dict): Bevat alle beschikbare templates en de locatie waar de templates te vinden zijn
        """
        # Opening JSON file
        codelist = Path(
            f"{self.params.dir_codelist}/{self.params.codelist_config.codeList_json}"
        )
        with open(codelist) as json_file:
            codeList = json.load(json_file)

        if codelist.exists():
            dir_output = f"{self.params.dir_repository}/CentralLayer/DA_MDDE"
            dir_output_type = f"{dir_output}/PostDeployment/"
            file_output = "PostDeploy_MetaData_Config_CodeList.sql"
            file_output_full = Path(os.path.join(dir_output_type, file_output))
            file_output_master = "PostDeploy.sql"
            file_output_master_full = Path(
                f"{self.params.dir_repository}/CentralLayer/PostDeployment/{file_output_master}"
            )

            # Add used folders to dict_created_ddls to be later used to add to the VS Project file
            self.__add_post_deploy_to_ddl(
                file_output=file_output, file_output_master=file_output_master
            )

            content = self.templates["PostDeploy_CodeList"].render(codeList=codeList)

            Path(dir_output_type).mkdir(parents=True, exist_ok=True)
            with open(file_output_full, mode="w", encoding="utf-8") as file_ddl:
                file_ddl.write(content)
            logger.info(
                f"Written CodeTable Post deploy script: {file_output_full.resolve()}"
            )

            # Add file to master file.
            if not file_output_master_full.is_file():
                f = open(file_output_master_full, "a+")
                f.write("/* Post deploy master file. */\n")
                f.close()
            if file_output_master_full.is_file():
                fr = open(file_output_master_full, "r")
                if f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n" not in fr.read():
                    fr.close()
                    f = open(file_output_master_full, "a")
                    f.write(
                        f"\nPRINT N'Running PostDeploy: ..\\DA_MDDE\\PostDeployment\\{file_output}\n"
                    )
                    f.write(f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n")
                    f.close()

    def __add_post_deploy_to_ddl(self, file_output, file_output_master):
        if self.schema_post_deploy not in self.created_ddls["Folder Include"]:
            self.created_ddls["Folder Include"].append(self.schema_post_deploy)
        folder_model = f"{self.schema_post_deploy}\\PostDeployment"
        if folder_model not in self.created_ddls["Folder Include"]:
            self.created_ddls["Folder Include"].append(folder_model)
        file_codelist = f"{folder_model}\\{file_output}"
        if file_codelist not in self.created_ddls["None Include"]:
            self.created_ddls["None Include"].append(file_codelist)
        file_master = f"PostDeployment\\{file_output_master}"
        if file_master not in self.created_ddls["None Include"]:
            self.created_ddls["None Include"].append(file_master)

    def write_json_created_ddls(self):
        """
        Creëert een Json file met daarin alle DDL's en ETL's die zijn gemaakt vanuit het model. Dit JSON-bestand
        is input voor de Publisher
        """
        out_file = Path(
            f"{self.params.dir_generate}/{self.params.generator_config.created_ddls_json}"
        )
        with open(out_file, mode="w+", encoding="utf-8") as file_ddl:
            json.dump(self.created_ddls, file_ddl, indent=4)
        logger.info(f"""Written dict_created_ddls to JSON file: {out_file.resolve()}""")
