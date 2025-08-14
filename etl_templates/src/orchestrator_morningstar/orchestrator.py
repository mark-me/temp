import os
from pathlib import Path

from integrator import EtlSimulator
from logtools import get_logger
from config import MorningstarConfig
from tqdm import tqdm

logger = get_logger(__name__)

class Orchestrator:
    
    def __init__(self):
        """
            
        """
        super().__init__()
    
    def build_dag(file_config: str) -> EtlSimulator:
        """
        Initialiseert een nieuwe ReportEtlSimulation instantie met het opgegeven configuratiebestand.

        Laadt de configuratie, initialiseert de ETL simulator en bereidt de rapportage voor.

        Args:
            file_config (str): Pad naar het configuratiebestand.
        """
        file_config = Path(file_config)
        config = MorningstarConfig(file_config=file_config)
        logger.info(
            f"Failure report geïnitialiseerd met configuratie uit '{file_config}'"
        )
        paths_RETW = []
        dag = EtlSimulator()

        paths_RETW = []
        path = config.path_input 
        files_RETW = os.listdir(path)
        print(files_RETW)
        for file_retw in tqdm(
            files_RETW, desc="Extracten Repo Json bestanden", colour = "#d7f5cb"
        ):
            logger.info("Aanvullen input files met folderlocatie ")
            file_retw = config.path_input / file_retw
            paths_RETW.append(file_retw)
        dag.build_dag(files_RETW=paths_RETW)
        return dag
