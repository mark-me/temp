import os
import sys
from pathlib import Path

from config_file import ConfigFile

# from dependencies_checker import DagReporting
from generator import CodeList, DevOpsHandler, DDLGenerator, DDLPublisher
from pd_extractor import PDDocument
from logtools import get_logger, issue_tracker

logger = get_logger(__name__)


class Orchestrator:
    """Orkestreert de Power Designer extractie en deployment workflow.

    Manages the extraction of data, dependency checking, code generation, and repository interactions.
    """
    def __init__(self, file_config: str):
        """Initialiseert de Orkestrator.

        Sets up the configuration based on the provided file path.

        Args:
            file_pd_ldm (Path): Locatie Power Designer ldm bestand
        """
        self.file_config = Path(file_config)
        self.config = ConfigFile(file_config=self.file_config)
        #logger.info(f"Genesis geïnitialiseerd met configuratie uit '{file_config}'")
        
        

# Run Current Class
if __name__ == "__main__":
    file_config = Path("./etl_templates/config.yml")
    genesis = Orchestrator(file_config=Path(file_config))
    #genesis.start_processing(skip_deployment=True)