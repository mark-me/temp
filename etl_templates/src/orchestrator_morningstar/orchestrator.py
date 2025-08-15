from pathlib import Path

from integrator import EtlSimulator
from logtools import get_logger
from config import MorningstarConfig


logger = get_logger(__name__)

class Orchestrator:

    def __init__(self, path_config: Path):
        """
        Initialiseert een nieuwe Orchestrator instantie met het opgegeven configuratiepad.

        Slaat het pad naar het configuratiebestand op voor later gebruik.

        Args:
            path_config (Path): Pad naar het configuratiebestand.
        """
        self.path_config = path_config

    def build_dag(self,) -> EtlSimulator:
        """
        Initialiseert een nieuwe ReportEtlSimulation instantie met het opgegeven configuratiebestand.

        Laadt de configuratie, initialiseert de ETL simulator en bereidt de rapportage voor.

        Args:
            file_config (str): Pad naar het configuratiebestand.
        """
        config = MorningstarConfig(file_config=self.path_config)
        logger.info(
            f"Failure report geïnitialiseerd met configuratie uit '{self.path_config}'"
        )

        dag = EtlSimulator()
        paths_RETW = [x for x in config.path_input.iterdir() if x.is_file()]
        if not paths_RETW:
            logger.error(f"Geen Power Designer extracts gevonden in {config.path_input}")
            return None
        dag.build_dag(files_RETW=paths_RETW)
        return dag