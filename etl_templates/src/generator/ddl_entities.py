from pathlib import Path

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
        self._set_entity_defaults(entity=entity)
        entity = self._add_datatype_translations(entity=entity)
        content = self._render_entity_ddl(entity)
        path_output_file = self._get_entity_ddl_paths(entity)
        self.save_generated_object(content, path_output_file)
        logger.info(f"Entity DDL weggeschreven naar {Path(path_output_file).resolve()}")

    def _set_entity_defaults(self, entity: dict):
        """
        Stelt standaardwaarden in voor een entiteit indien deze ontbreken.

        Deze functie controleert of verplichte standaardwaarden aanwezig zijn in de entiteit
        en vult deze aan indien nodig, zodat de entiteit altijd over de benodigde eigenschappen beschikt.

        Args:
            entity (dict): De entiteit die wordt gecontroleerd en aangevuld.
        """
        #entity["Name"] = f"{entity['Name'].replace(' ', '_')}"
        if "Number" not in entity:
            logger.warning(
                f"Entiteit '{entity['Name']}' heeft geen property number, standaard distributie wordt gebruikt."
            )
            entity["Number"] = 0

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

    def _add_datatype_translations(self, entity: dict) -> dict:
        """
        Vertaalt de datatypes van attributen van een entiteit naar SQL-compatibele datatypes.

        Deze functie doorloopt alle attributen van de entiteit en wijst het juiste SQL-datatype toe aan elk attribuut,
        zodat de entiteit correct kan worden weergegeven in de gegenereerde DDL.

        Args:
            entity (dict): De entiteit waarvan de attributen vertaald worden.

        Returns:
            dict: De entiteit met toegevoegde SQL-datatypen voor elk attribuut.
        """
        for index, attribute in enumerate(entity["Attributes"]):
            data_type = attribute["DataType"]
            length = attribute.get("Length")
            precision = attribute.get("Precision", 0)

            def get_prefix(s, n):
                return s[:n] if s else ""

            prefix_1 = get_prefix(data_type, 1)
            prefix_2 = get_prefix(data_type, 2)
            prefix_3 = get_prefix(data_type, 3)
            prefix_4 = get_prefix(data_type, 4)

            is_numeric = prefix_1 == "N"
            is_decimal = prefix_2 == "DC"
            is_float = prefix_1 == "F"
            is_sf = prefix_2 == "SF"
            is_lf = prefix_2 == "LF"
            is_mn = prefix_2 == "MN"
            is_no = prefix_2 == "NO"
            is_nchar = prefix_1 == "A"
            is_nvarchar = prefix_2 == "VA"
            is_bt = prefix_2 == "BT"
            is_mbt = prefix_3 == "MBT"
            is_vmbt = prefix_4 == "VMBT"
            is_la = prefix_2 == "LA"
            is_lva = prefix_3 == "LVA"
            is_txt = prefix_3 == "TXT"
            is_bin = prefix_3 == "BIN"
            is_vbin = prefix_4 == "VBIN"
            is_lbin = prefix_4 == "LBIN"
            is_dt = prefix_2 == "DT"
            is_date = prefix_1 == "D"
            is_bit = prefix_2 == "BL"
            is_int = prefix_1 == "I"
            is_smallint = prefix_2 == "SI"
            is_li = prefix_2 == "LI"

            if is_numeric:
                dataype = f"NUMERIC({length}, {precision})"
            elif is_decimal:
                dataype = f"DECIMAL({length}, {precision})"
            elif is_float:
                dataype = f"FLOAT({getattr(attribute, 'Length', length)})"
            elif is_sf:
                dataype = "FLOAT(24)"
            elif is_lf:
                dataype = "FLOAT(53)"
            elif is_mn:
                dataype = "DECIMAL(28,4)"
            elif is_no:
                dataype = "BIGINT"
            elif is_nchar:
                dataype = f"NCHAR({length})"
            elif is_nvarchar:
                dataype = f"NVARCHAR({length})"
            elif is_bt:
                dataype = f"NCHAR({length})"
            elif is_mbt:
                dataype = f"NCHAR({length})"
            elif is_vmbt:
                dataype = f"NVARCHAR({length})"
            elif is_la:
                dataype = f"NCHAR({length})"
            elif is_lva:
                dataype = f"NVARCHAR({length})"
            elif is_txt:
                dataype = f"NVARCHAR({length})"
            elif is_bin:
                dataype = f"BINARY({length})"
            elif is_vbin:
                dataype = f"VARBINARY({length})"
            elif is_lbin:
                dataype = "VARBINARY(MAX)"
            elif is_dt:
                dataype = "DATETIME2"
            elif is_date:
                dataype = "DATE"
            elif is_bit:
                dataype = "BIT"
            elif is_int:
                dataype = "INT"
            elif is_smallint:
                dataype = "SMALLINT"
            elif is_li:
                dataype = "INT"
            else:
                dataype = data_type
            entity["Attributes"][index]["DataTypeSQL"] = dataype
        return entity

    def _get_entity_ddl_paths(self, entity: dict) -> Path:
        """
        Bepaalt het outputpad en de bestandsnaam voor de DDL van een entiteit.

        Args:
            entity (dict): De entiteit waarvoor het pad wordt bepaald.
            dir_output (Path): De outputdirectory voor de DDL-bestanden.

        Returns:
            tuple: (path_output_file, file_output)
        """
        dir_output = Path(f"{self.dir_output}/{entity['CodeModel']}/Tables/")
        dir_output.mkdir(parents=True, exist_ok=True)
        file_output = f"{entity['Code']}.sql"
        path_output_file = Path(f"{dir_output}/{file_output}")
        return path_output_file
