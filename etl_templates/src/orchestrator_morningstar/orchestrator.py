import functools
import sys
from pathlib import Path

from integrator import EtlSimulator
from logtools import get_logger
from config import MorningstarConfig


logger = get_logger(__name__)

BOLD_BLUE = "\x1b[1;34m"
BOLD_RED = "\x1b[1;31m"
UNDERLINE = "\x1b[4m"
BOLD_YELLOW = "\x1b[1;33m"
BOLD_GREEN = "\x1b[1;32m"
RESET = "\x1b[0m"

class Orchestrator:
    def __init__(self, file_config: Path):
        """ """
        super().__init__()
        self.file_config = file_config
        self.config = MorningstarConfig(file_config=self.file_config)
        self.dag = EtlSimulator()
        logger.info(f"Morningstar geïnitialiseerd met configure uit '{file_config}'")
        
    def build_dag(self) -> EtlSimulator:
        """
        Initialiseert een nieuwe ReportEtlSimulation instantie met het opgegeven configuratiebestand.

        Laadt de configuratie, initialiseert de ETL simulator en bereidt de rapportage voor.

        Args:
            file_config (str): Pad naar het configuratiebestand.
        """
        print(
            f"{BOLD_BLUE}{UNDERLINE}Start Morningstar verwerking: {self.config.title} {self.config._version}.{RESET}\n",
            file=sys.stdout,    
            
        )
        logger.info(
            f"Failure report geïnitialiseerd met configuratie uit '{self.file_config}'"
        )
        files_RETW = [
            file_RETW
            for file_RETW in self.config.path_input.iterdir()
            if file_RETW.is_file and file_RETW.suffix == ".json"
        ]
        self.dag.build_dag(files_RETW=files_RETW)
        return self.dag
    
    def start_etl_simulator(self, mapping_refs, failure_strategy, file_png) -> EtlSimulator:
        print(
            f"{BOLD_BLUE}Start ETL Simulatie met strategie {failure_strategy.value}{RESET}\n",
            file=sys.stdout,    
            
        )
        print(f"{BOLD_BLUE}\tGefaalde mappings:{RESET}")
        failed_mappings = ""
        for mapping_ref in mapping_refs:
            failed_mappings += " * " + mapping_ref.CodeMapping + "\n\t"
        print(f"{BOLD_BLUE}\t{failed_mappings}{RESET} ")
        self.dag.set_mappings_failed(mapping_refs=mapping_refs)
        print(f"{BOLD_BLUE}Start ETL Simulatie{RESET}")
        self.dag.start_etl(failure_strategy=failure_strategy)
        print(f"{BOLD_BLUE}Plot de uitkomst van de ETL Simulatie:{RESET}")
        print(f"{BOLD_BLUE}\tLocatie outputbestand: {self.config.path_output}/{file_png}{RESET}")
        self.dag.plot_etl_fallout(file_png=file_png)