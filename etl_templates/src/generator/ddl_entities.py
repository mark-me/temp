from pathlib import Path
import re

from logtools import get_logger
from tqdm import tqdm

from .ddl_base import DDLGeneratorBase, DDLType

logger = get_logger(__name__)


class DDLEntities(DDLGeneratorBase):
    def __init__(self, path_output: Path, platform: str):
        super().__init__(
            path_output=path_output, platform=platform, ddl_type=DDLType.ENTITY
        )

    def generate_ddls(self, entities: list) -> None:
        """
        Genereert DDL-bestanden voor alle entiteiten die binnen een document zijn gedefinieerd.

        Deze functie selecteert entiteiten die daadwerkelijk gecreëerd moeten worden en verwerkt elke entiteit afzonderlijk,
        zodat alleen relevante entiteiten een DDL-bestand krijgen.

        Args:
            entities (list): Lijst van entiteiten waarvoor DDL-bestanden gegenereerd moeten worden.

        Returns:
            None
        """
        # Select entities that are defined within a document (not just derived from mappings (sources))
        entities_create = [entity for entity in entities if entity["IsCreated"]]
        for entity in tqdm(
            entities_create, desc="Genereren tabellen", colour="#38761d"
        ):
            self._process_entity(entity=entity)

    def _process_entity(self, entity: dict):
        """
        Verwerkt een enkele entiteit en genereert het bijbehorende DDL-bestand.

        Deze functie stelt standaardwaarden in, verwerkt business keys, vertaalt datatypes,
        rendert de DDL en slaat het resultaat op voor de opgegeven entiteit.

        Args:
            entity (dict): De entiteit die verwerkt en omgezet wordt naar een DDL-bestand.

        Returns:
            None
        """
        content = self._render_entity_ddl(entity)
        path_output_file = self._get_entity_ddl_paths(entity)
        self.save_generated_object(content=content, path_file_output=path_output_file)
        logger.info(f"Entity DDL weggeschreven naar {path_output_file.resolve()}")

    def _render_entity_ddl(self, entity: dict) -> str:
        """
        Rendert de DDL voor een entiteit met behulp van de Jinja2 template.

        Args:
            entity (dict): De entiteit waarvoor de DDL wordt gegenereerd.

        Returns:
            str: De gegenereerde DDL-string voor de entiteit.
        """
        content = self.template.render(entity=entity)
        return content  # sqlparse.format(content, reindent=True, keyword_case="upper")

    def _get_entity_ddl_paths(self, entity: dict) -> Path:
        """
        Bepaalt het outputpad en de bestandsnaam voor de DDL van een entiteit.

        Args:
            entity (dict): De entiteit waarvoor het pad wordt bepaald.
            dir_output (Path): De outputdirectory voor de DDL-bestanden.

        Returns:
            tuple: (path_output_file, file_output)
        """
        # Sanitize CodeModel to remove path separators and other unwanted characters
        code_model_safe = re.sub(r'[\\/:"*?<>|]+', '_', entity['CodeModel'])

        path_output = self.path_output / code_model_safe / "Tables"
        path_output.mkdir(parents=True, exist_ok=True)
        file_output = f"{entity['Code']}.sql"
        path_output_file = path_output / file_output
        return path_output_file
