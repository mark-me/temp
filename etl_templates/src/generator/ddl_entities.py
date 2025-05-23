from pathlib import Path

from jinja2 import Template
from log_config import logging

from .ddl_base import DDLGeneratorBase

logger = logging.getLogger(__name__)


class DDLEntities(DDLGeneratorBase):
    def __init__(self, dir_output: str, ddl_template: Template):
        super().__init__(dir_output=dir_output, ddl_template=ddl_template)

    def generate_ddls(self, models: dict, identifiers: dict):
        """
        Creëert alle DDL's van de verschillende entiteiten die in models zijn opgenomen en schrijft deze weg naar een folder in de repository

        Args:
            identifiers (dict): Bevat alle business keys definities
        """
        for model in models:
            if not model["IsDocumentModel"]:
                continue
            if "Entities" not in model:
                logger.warning(f"Model '{model['Name']} heeft geen entiteiten'")
                continue
            for entity in model["Entities"]:
                self.__process_entity(
                    code_model=model["Code"], entity=entity, identifiers=identifiers
                )

    def __process_entity(self, code_model: str, entity: dict, identifiers: dict):
        """
        Verwerkt een enkele entiteit: vult ontbrekende waarden aan, bouwt business keys, rendert de DDL en schrijft deze weg.

        Args:
            entity (dict): De entiteit die verwerkt wordt.
            model (dict): Het model waartoe de entiteit behoort.
            dir_output (Path): De outputdirectory voor de DDL-bestanden.
            identifiers (dict): Bevat alle business keys definities.
        """
        self.__set_entity_defaults(entity=entity, code_model=code_model)
        entity = self.__replace_entity_keys_with_bkeys(
            entity=entity, identifiers=identifiers
        )
        content = self.__render_entity_ddl(entity)
        path_output_file = self.__get_entity_ddl_paths(entity)
        self.save_generated_object(content, path_output_file)
        logger.info(f"Entity DDL weggeschreven naar {Path(path_output_file).resolve()}")

    def __set_entity_defaults(self, code_model: str, entity: dict):
        """
        Stelt standaardwaarden in voor een entiteit indien deze ontbreken.

        Args:
            entity (dict): De entiteit die wordt gecontroleerd en aangevuld.
            model (dict): Het model waartoe de entiteit behoort.
        """
        if "Number" not in entity:
            logger.warning(
                f"Entiteit '{entity['Name']}' heeft geen property number, standaard distributie wordt gebruikt."
            )
            entity["Number"] = 0
        entity["CodeModel"] = code_model

    def __render_entity_ddl(self, entity: dict) -> str:
        """
        Rendert de DDL voor een entiteit met behulp van de Jinja2 template.

        Args:
            entity (dict): De entiteit waarvoor de DDL wordt gegenereerd.

        Returns:
            str: De gerenderde DDL-string voor de entiteit.
        """
        content = self.template.render(entity=entity)
        return content  # sqlparse.format(content, reindent=True, keyword_case="upper")

    def __get_entity_ddl_paths(self, entity: dict) -> Path:
        """
        Bepaalt het outputpad en de bestandsnaam voor de DDL van een entiteit.

        Args:
            entity (dict): De entiteit waarvoor het pad wordt bepaald.
            dir_output (Path): De outputdirectory voor de DDL-bestanden.

        Returns:
            tuple: (path_output_file, file_output)
        """
        dir_output = Path(
            f"{self.dir_output}/CentralLayer/{entity['CodeModel']}/Tables/"
        )
        dir_output.mkdir(parents=True, exist_ok=True)
        file_output = f"{entity['Code']}.sql"
        path_output_file = Path(f"{dir_output}/{file_output}")
        return path_output_file

    def __replace_entity_keys_with_bkeys(self, entity: dict, identifiers: dict):
        """Vervangt alle key kolommen met business key kolommen.

        Args:
            identifiers (dict): Alle identifiers definities
            entity (dict): Entiteit
        """
        mapped_identifiers = []
        identifier_mapped = []
        for identifier in entity["Identifiers"]:
            if "Id" not in identifier:
                logger.error("Geen identifier gevonden!")
                continue
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
        attributes.extend(
            attribute
            for attribute in entity["Attributes"]
            if attribute["Code"] not in mapped_identifiers
        )
        entity.pop("Attributes")
        entity["Attributes"] = attributes
        return entity
