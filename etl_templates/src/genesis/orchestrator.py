import os
from pathlib import Path

from config_file import ConfigFile
from dependencies_checker import DagReporting
from repository_manager import RepositoryManager
from deploy_mdde import DeploymentMDDE
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
        """Start het Genesis verwerkingsproces.

        Orkestreert extractie, afhankelijkheidsanalyse, codegeneratie en repository-operaties voor het ETL-proces.

        Args:
            skip_devops (bool): Indien True, worden DevOps repository-operaties overgeslagen.

        Returns:
            None
        """
        logger.info("Start Genesis verwerking")
        lst_files_RETW = []
        for pd_file in self.config.power_designer.files:
            file_RETW = self._extract(file_pd_ldm=pd_file)
            lst_files_RETW.append(file_RETW)

        dag = self._inspect_etl_dag(files_RETW=lst_files_RETW)
        mapping_order = dag.get_mapping_order()

        self._generate_code(files_RETW=lst_files_RETW)
        lst_paths_post_deployment = self._generate_mdde_deployment(mapping_order=mapping_order)

        # Stop process if extraction and dependencies check result in issues
        # self._handle_issues()
        if not skip_devops:
            devops_handler = RepositoryManager(
                config=self.config.devops
            )
            devops_handler.clone()
            path_source = self.config.generator.path_output
            devops_handler.add_directory_to_repo(path_source=path_source)
            # TODO: Copy code and codelist to repo and update project file
        #     devops_handler.push()

    def _extract(self, file_pd_ldm: Path) -> str:
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

    def _inspect_etl_dag(self, files_RETW: list):
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

    def _generate_mdde_deployment(self, mapping_order: list) -> list:
        """
        Genereert MDDE post-deployment scripts op basis van de opgegeven mapping order.

        Roept het DeploymentMDDE component aan om alle benodigde scripts te genereren en retourneert de paden naar de gegenereerde scripts.

        Args:
            mapping_order (list): De mapping order configuratie die in het script verwerkt moet worden.

        Returns:
            list: Een lijst met paden naar de gegenereerde post-deployment scripts.
        """
        logger.info("Generating MDDE scripts")
        deploy_mdde = DeploymentMDDE(
            path_data=self.config.deploy_mdde.path_data_input,
            schema=self.config.deploy_mdde.schema,
            path_output=self.config.deploy_mdde.path_output,
        )
        return deploy_mdde.process(mapping_order=mapping_order)

    def _generate_code(self, files_RETW: list) -> None:
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
