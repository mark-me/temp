import os
from pathlib import Path

from config_file import ConfigFile
from dependencies_checker import DagReporting
from repository_manager import RepositoryHandler
from deploy_mdde import Deployment
from generator import DDLGenerator
from logtools import get_logger, issue_tracker
from pd_extractor import PDDocument

logger = get_logger(__name__)


class ExtractionIssuesFound(Exception):
    """Exception raised when extraction issues are found and processing should stop."""


class Orchestrator:
    """Orkestreert de Power Designer extractie en deployment workflow.

    Manages the extraction of data, dependency checking, code generation, and repository interactions.
    """

    def __init__(self, file_config: str):
        """Initialiseert de Orkestrator.

        Sets up the configuration based on the provided file path.

        Args:
            file_config (Path): Locatie configuratiebestand
        """
        self.file_config = Path(file_config)
        self.config = ConfigFile(file_config=self.file_config)
        logger.info(f"Genesis geïnitialiseerd met configuratie uit '{file_config}'")

    def start_processing(self, skip_devops: bool = False) -> None:
        """Start the main processing workflow.

        Orchestrates the extraction, dependency checking, and deployment code generation.

        Args:
            skip_deployment (bool): Skip the deployment.

        Returns:
            None
        """
        logger.info("Start Genesis verwerking")
        lst_files_RETW = []
        for pd_file in self.config.power_designer.files:
            file_RETW = self.extract(file_pd_ldm=pd_file)
            lst_files_RETW.append(file_RETW)

        dag = self.inspect_etl_dag(files_RETW=lst_files_RETW)
        mapping_order = dag.get_mapping_order()

        self.generate_code(files_RETW=lst_files_RETW, mapping_order=mapping_order)
        self.generate_mdde_deployment()

        # Stop process if extraction and dependecies check result in issues
        # self._handle_issues()

        post_deployment = Deployment(
            dir_output=self.config.dir_generate, schema_post_deploy="MDDE"
        )
        post_deployment.generate_load_config(mapping_order=mapping_order)

        if not skip_devops:
            devops_handler = RepositoryHandler(
                params=self.config.devops_config,
                dir_repository=self.config.dir_repository,
            )
            devops_handler.clone()
            # TODO: Copy code and codelist to repo and update project file
            devops_handler.push()

    def extract(self, file_pd_ldm: Path) -> str:
        """Extract data from a PowerDesigner LDM file.

        Extracts the logical data model and mappings from the specified file and saves them as a JSON file.

        Args:
            file_pd_ldm (Path): Locatie Power Designer ldm bestand

        Returns:
            None
        """
        logger.info(f"Start extraction for '{file_pd_ldm}'")
        document = PDDocument(file_pd_ldm=file_pd_ldm)
        file_RETW = self.config.extractor.path_output / f"{file_pd_ldm.stem}.json"
        document.write_result(file_output=file_RETW)
        logger.info(
            f"Het logisch data model en mappings van '{file_pd_ldm}' geëxtraheerd en geschreven naar '{file_RETW}'"
        )
        return file_RETW

    def inspect_etl_dag(self, files_RETW: list):
        """
        Inspecteert de ETL-afhankelijkheden en genereert een overzicht van de mappingvolgorde.

        Deze functie analyseert de opgegeven RETW-bestanden, schrijft de mappingvolgorde naar een JSON-bestand en visualiseert de ETL-flow.

        Args:
            files_RETW (list): Een lijst met paden naar de geëxtraheerde RETW-bestanden.

        Returns:
            DagReporting: Een object met informatie over de ETL-afhankelijkheden.
        """
        logger.info("Reporting on dependencies")
        dag = DagReporting()
        dag.add_RETW_files(files_RETW=files_RETW)
        # Visualization of the ETL flow for all RETW files combined
        path_output = str(self.config.extractor.path_output / "ETL_flow.html")
        dag.plot_etl_dag(file_html=path_output)
        # dag.plot_file_dependencies(f"{dir_report}/RETW_dependencies.html"=test) FIXME: Results in error
        return dag

    def generate_mdde_deployment(self) -> Path:
        """
        Genereert een CodeList-bestand op basis van de input codelist-bestanden.

        Deze functie leest de codelist-bestanden, verwerkt ze en schrijft het resultaat naar een JSON-bestand.

        Returns:
            Path: Het pad naar het gegenereerde CodeList-bestand.
        """
        logger.info("Generating MDDE scripts")
        dir_output = self.config.dir_codelist
        # FIXME: Nooit via _data (is private)
        dir_input = self.config.dir_codelist_input
        file_output = dir_output / self.config.file_codelist_output
        generator_codelist = CodeList(dir_input=dir_input, file_output=file_output)
        # Generatate CodeList.json from input codelist files
        generator_codelist.read_CodeLists()
        generator_codelist.write_CodeLists()
        return file_output

    def generate_code(self, files_RETW: list, mapping_order: list) -> None:
        """Generate deployment code based on extracted data.

        Generates the necessary code for deployment based on the extracted data and dependencies.

        Args:
            files_RETW (list): A list of paths to the extracted RETW files.

        Returns:
            None
        """
        logger.info("Start generating deployment code")
        ddl_generator = DDLGenerator(params=self.config.generator)
        for file_RETW in files_RETW:
            ddl_generator.generate_ddls(file_RETW=file_RETW)

    def _handle_issues(self):
        """
        Controleert of er issues zijn gevonden tijdens de verwerking en handelt deze af.

        Schrijft een rapport van de gevonden issues naar een CSV-bestand en stopt de verwerking indien nodig.

        Returns:
            None
        """
        if issue_tracker.has_issues():
            file_issues = os.path.join(self.config.dir_extract, "extraction_issues.csv")
            issue_tracker.write_csv(file_csv=file_issues)
            raise ExtractionIssuesFound(
                f"Verwerking gestopt nadat er issues zijn aangetroffen. Zie rapport: {file_issues}"
            )
