import json
import os
import sys
from pathlib import Path

from config_file import ConfigFile

from dependencies_checker import DagReporting
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
            file_config (Path): Locatie configuratiebestand
        """
        self.file_config = Path(file_config)
        self.config = ConfigFile(file_config=self.file_config)
        logger.info(f"Genesis geïnitialiseerd met configuratie uit '{file_config}'")

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
        dir_output = self.config.dir_extract
        file_RETW = Path(os.path.join(dir_output, f"{file_pd_ldm.stem}.json"))
        document.write_result(file_output=file_RETW)
        logger.info(
            f"Het logisch data model en mappings van '{file_pd_ldm}' geëxtraheerd en geschreven naar '{file_RETW}'"
        )
        return file_RETW

    def get_etl_dag(self, files_RETW: list) -> DagReporting:
        """Check dependencies between extracted data files.

        Analyzes the relationships and dependencies between the extracted data.

        Args:
            files_RETW (list): A list of paths to the extracted RETW files.

        Returns:
            None
        """
        logger.info("Reporting on dependencies")
        dag = DagReporting()
        dag.add_RETW_files(files_RETW=files_RETW)
        # Dump dependencies to JSON
        lst_mapping_order = dag.get_mapping_order()
        dir_output = self.config.dir_generate
        Path(os.path.join(dir_output, "mapping_order.json"))
        with open(f"{dir_output}/mapping_order.json", "w", encoding="utf-8") as file:
            for item in lst_mapping_order:
                file.write(json.dumps(item) + "\n")
        # Visualization of the ETL flow for all RETW files combined
        dag.plot_etl_dag(file_html=f"{dir_output}/ETL_flow.html")
        return dag

    def generate_codeList(self) -> Path:
        """Generate Codelist from Input files.

        Generate JSON from codelist files

        Args:

        Returns:
            None
        """
        logger.info("Generating Codelist from files")
        dir_output = self.config.dir_codelist
        codelistfolder = Path(self.config._data.codelist.input_folder)
        codelist = Path(os.path.join(dir_output, self.config._data.codelist.codeList_json))
        codelistmaker = CodeList(codelistfolder, codelist)
        # Generatate CodeList.json from input codelist files
        codelistmaker.read_CodeLists()
        codelistmaker.write_CodeLists()
        return codelist

    def generate_code(self, files_RETW: list, mapping_order: list) -> None:
        """Generate deployment code based on extracted data.

        Generates the necessary code for deployment based on the extracted data and dependencies.

        Args:
            files_RETW (list): A list of paths to the extracted RETW files.

        Returns:
            None
        """
        logger.info("Start generating deployment code")
        params = self.config
        ddl_generator = DDLGenerator(params=params)
        for file_RETW in files_RETW:
            #TODO: @Mark, generatorParams zou beter zijn als deze ook bijvoorbeeld bepaalde DIR properties bevat. Ik krijg dit niet voor elkaar..
            publisher = DDLPublisher(params)
            # 3. Write all DLL, SoureViews and MDDE ETL to the Repo
            ddl_generator.generate_code(file_RETW=file_RETW, mapping_order=mapping_order)
            # 4. Write a JSON that contains all list with al written objects with there type. Is used by the publisher.
            ddl_generator.write_json_created_ddls()
            # 5. Write all new created DDL and ETL file to the VS SQL Project file as a reference.
            publisher.publish()


    def clone_repository(self) -> None:
        """Clone the target repository.

        Clones the repository specified in the configuration to a local directory.

        Returns:
            str: The path to the cloned repository.
        """
        devopsParams = self.config.devops_config
        devopsFolder = self.config.dir_repository

        devops_handler = DevOpsHandler(devopsParams, devopsFolder)
        # Clone a clean copy of the DevOps Repo to disk, and create a new brach based on the config params.
        devops_handler.get_repo()

    def commit_repository(self) -> None:
        """Commit the repository to DevOps.

        commit the changes in the repository.

        Returns:
            str: The path to the repository.
        """
        devopsParams = self.config.devops_config
        devopsFolder = self.config.dir_repository

        devops_handler = DevOpsHandler(devopsParams, devopsFolder)
        # Publish to DevOps Repo.
        devops_handler.publish_repo()

    def start_processing(self, skip_deployment: bool = False) -> None:
        """Start the main processing workflow.

        Orchestrates the extraction, dependency checking, and deployment code generation.

        Args:
            skip_deployment (bool): Skip the deployment.

        Returns:
            None
        """
        logger.info("Start Genesis verwerking")
        lst_files_RETW = []
        for pd_file in self.config.files_power_designer:
            file_RETW = self.extract(file_pd_ldm=pd_file)
            lst_files_RETW.append(file_RETW)

        dag = self.get_etl_dag(files_RETW=lst_files_RETW)
        mapping_order = dag.get_mapping_order()

        self.generate_codeList()

        # self.clone_repository()

        self.generate_code(files_RETW=lst_files_RETW, mapping_order=mapping_order)

        # Stop process if extraction and dependecies check result in issues
        if issue_tracker.has_issues():
            file_issues = os.path.join(self.config.dir_extract, "extraction_issues.csv")
            issue_tracker.write_csv(file_csv=file_issues)
            logger.error(f"Problemen gevonden, rapport is te vinden in {file_issues}")
            sys.exit("Verwerking gestopt nadat er issues zijn aangetroffen")

        self.commit_repository()
