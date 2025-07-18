import argparse
import sys
from pathlib import Path
from typing import List

from integrator import EtlSimulator, MappingRef, FailureStrategy
from logtools import get_logger
from orchestrator import ConfigFile
from pd_extractor import PDDocument
from tqdm import tqdm

logger = get_logger(__name__)


class ReportEtlSimulation:
    def __init__(self, file_config: str):
        """
        Initialiseert een nieuwe ReportEtlSimulation instantie met het opgegeven configuratiebestand.

        Laadt de configuratie, initialiseert de ETL simulator en bereidt de rapportage voor.

        Args:
            file_config (str): Pad naar het configuratiebestand.
        """
        self.file_config = Path(file_config)
        self.config = ConfigFile(file_config=self.file_config)
        logger.info(
            f"Failure report geïnitialiseerd met configuratie uit '{file_config}'"
        )
        self.paths_RETW = []
        self.dag = EtlSimulator()

    def build_dag(self, mapping_refs: List[MappingRef]):
        """
        Genereert een rapportage van de ETL-simulatie op basis van opgegeven mappings en rapportpad.

        Voert extractie uit, bouwt de ETL-DAG en slaat het rapport op.

        Args:
            mapping_refs (List[MappingRef]): Lijst van mappings die als gefaald worden beschouwd.
        """
        self.paths_RETW = self._extract()
        self.dag.build_dag(files_RETW=self.paths_RETW)
        self.dag.set_mappings_failed(mapping_refs=mapping_refs)

    def _extract(self) -> list:
        """
        Extraheert data uit Power Designer bestanden en schrijft deze naar JSON-bestanden.

        Deze functie verwerkt de opgegeven Power Designer bestanden, extraheert het logisch datamodel en mappings,
        en slaat de resultaten op als JSON-bestanden in de opgegeven output directory.

        Returns:
            list: Lijst van paden naar de geëxtraheerde JSON-bestanden.
        """
        lst_files_RETW = []
        # Extractie van data uit Power Designer bestanden
        if not self.config.power_designer.files:
            logger.warning(
                "Geen PowerDesigner-bestanden geconfigureerd. Genesis verwerking wordt afgebroken."
            )
            return []
        files_pd_ldm = self.config.power_designer.files
        for file_pd_ldm in tqdm(
            files_pd_ldm, desc="Extracten Power Designer bestanden", colour="#d7f5cb"
        ):
            logger.info(f"Start extractie van Power Designer bestand '{file_pd_ldm}'")
            document = PDDocument(file_pd_ldm=file_pd_ldm)
            file_RETW = self.config.extractor.path_output / f"{file_pd_ldm.stem}.json"
            document.write_result(file_output=file_RETW)
            logger.info(
                f"Het logisch data model en mappings van '{file_pd_ldm}' geëxtraheerd en geschreven naar '{file_RETW}'"
            )
            lst_files_RETW.append(file_RETW)
        return lst_files_RETW

    def start_etl(self):
        self.dag.start_etl()

    def create_report(self, failure_strategy: FailureStrategy, path_report: Path):
        """
        Genereert een rapportage van de ETL-failure simulatie met de opgegeven faalstrategie en rapportpad.

        Visualiseert de impact van de faalstrategie op de ETL-DAG en slaat het resultaat op als HTML-bestand.

        Args:
            failure_strategy (FailureStrategy): De toe te passen faalstrategie.
            path_report (Path): Het pad waar het rapport opgeslagen wordt.

        Returns:
            None
        """
        logger.info("Reporting on dependencies")
        # Visualization of the ETL flow for all RETW files combined
        path_output = str(path_report)
        self.dag.plot_etl_fallout(
            failure_strategy=failure_strategy, file_png=path_output
        )


def main():
    """
    Voert de Genesis failure report simulatie uit via de command line interface.

    Parseert argumenten, initialiseert de rapportageklasse en genereert een ETL-failure rapport.

    """
    parser = argparse.ArgumentParser(description="De Genesis failure report simulatie")
    print(
        """\n
     _____                      _
    / ____|                    (_)
   | |  __  ___ _ __   ___  ___ _ ___
   | | |_ |/ _ \\ '_ \\ / _ \\/ __| / __|
   | |__| |  __/ | | |  __/\\__ \\ \\__ \\
    \\_____|\\___|_| |_|\\___||___/_|___/
                            MDDE Douane
                            Failure report simulation
    """,
        file=sys.stdout,
    )
    parser.add_argument("config_file", help="Locatie van een configuratiebestand")
    args = parser.parse_args()
    simulator = ReportEtlSimulation(file_config=Path(args.config_file))

    failed_mappings = [
        MappingRef("DA_Central", "SL_KIS_AggrMaxEndDateEad"),
        MappingRef("DA_Central", "SlDmsCustomsvalue"),
    ]
    simulator.build_dag(mapping_refs=failed_mappings)
    simulator.start_etl()
    simulator.create_report(
        failure_strategy=FailureStrategy.ONLY_SUCCESSORS,
        path_report=Path("etl_templates/intermediate/test.png"),
    )


if __name__ == "__main__":
    main()
