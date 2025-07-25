import os
import sys
from pathlib import Path

from deploy_mdde import DeploymentMDDE
from generator import DDLGenerator
from integrator import DagImplementation, DagReporting, DeadlockPrevention
from logtools import get_logger, issue_tracker
from pd_extractor import PDDocument
from repository_manager import RepositoryManager
from tqdm import tqdm

from .config_file import ConfigFile

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
        # Extraheert data uit de Power Designer ldm bestanden
        lst_files_RETW = self._extract()
        self._handle_issues(info_next_step="integreren van de RETW bestanden")  # Stop process if extraction results in issues
        # Integreer alle data uit de verschillende bestanden en voeg afgeleide data toe
        dag_etl = self._integrate_files(files_RETW=lst_files_RETW)
        self._handle_issues(info_next_step="genereren van code")
        # Genereer code voor doelschema's en mappings
        self._generate_code(dag_etl=dag_etl)
        # Genereer code voor ETL deployment
        self._generate_mdde_deployment(dag_etl=dag_etl)
        self._handle_issues(info_next_step="toevoegen aan het repository")  # Stop process when generating code result in issues
        # Voegt gegenereerde code en database objecten toe aan het repository
        if not skip_devops:
            self._add_to_repository()
        else:
            logger.info("Repository afhandeling zijn overgeslagen door de 'skip_devops' flag.")
        # Write issues to file
        file_issues = self.config.path_intermediate / "extraction_issues.csv"
        issue_tracker.write_csv(file_csv=file_issues)

    def _extract(self) -> list[Path]:
        """
        Extraheert data uit Power Designer bestanden en schrijft deze naar JSON-bestanden.

        Deze functie verwerkt de opgegeven Power Designer bestanden, extraheert het logisch datamodel en mappings,
        en slaat de resultaten op als JSON-bestanden in de opgegeven output directory.

        Returns:
            list[Path]: Lijst van paden naar de geëxtraheerde JSON-bestanden.
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

    def _handle_issues(self, info_next_step: str) -> None:
        """
        Controleert op gevonden issues en bepaalt of het verwerkingsproces moet worden gestopt.

        Deze functie bekijkt de ernst van de gevonden issues, vraagt de gebruiker om bevestiging bij waarschuwingen,
        en stopt het proces bij fouten of als de gebruiker niet wil doorgaan.

        Args:
            next_step (str): De volgende stap in het proces waarvoor toestemming wordt gevraagd bij waarschuwingen.

        Returns:
            None

        Raises:
            ExtractionIssuesFound: Indien het proces gestopt moet worden vanwege gevonden issues.
        """
        error = False
        max_severity_level = issue_tracker.max_severity_level()
        if max_severity_level == "WARNING":
            if self.config.ignore_warnings:
                return
            answer = input(f"Waarschuwingen gevonden, wil je doorgaan met {info_next_step}? (J/n):")

            if answer.upper() in ["", "J", "JA", "JAWOHL", "Y", "YES", "Jeroen"]:
                if answer.upper() == "JEROEN":
                    print("That is such a Jeroen Poll thing to do!", file=sys.stdout)
                return
            else:
                error = True
        if max_severity_level == "ERROR" or error:
            file_issues = os.path.join(
                self.config.path_intermediate, "extraction_issues.csv"
            )
            issue_tracker.write_csv(file_csv=file_issues)
            raise ExtractionIssuesFound(
                f"Verwerking gestopt nadat er issues zijn aangetroffen. Zie rapport: {file_issues}"
            )

    def _integrate_files(self, files_RETW: list) -> DagImplementation:
        """
        Integreert de opgegeven RETW-bestanden en bouwt de ETL-DAG.

        Deze functie voert eerst een rapportage uit over de afhankelijkheden en bouwt vervolgens de implementatie-DAG
        die gebruikt wordt voor verdere verwerking en codegeneratie.

        Args:
            files_RETW (list): Lijst van paden naar de RETW-bestanden.

        Returns:
            DagImplementation: De geïmplementeerde ETL-DAG voor verdere verwerking.
        """
        self._report_integration(files_RETW=files_RETW)
        logger.info("Create ETL Dag with implementation information")
        dag = DagImplementation()
        dag.build_dag(files_RETW=files_RETW)
        return dag

    def _report_integration(self, files_RETW: list) -> None:
        """
        Rapporteert en visualiseert de afhankelijkheden tussen de opgegeven RETW-bestanden.

        Deze functie bouwt een rapportage-DAG en genereert een visualisatie van de ETL-flow
        voor alle opgegeven RETW-bestanden, zodat afhankelijkheden inzichtelijk worden gemaakt.

        Args:
            files_RETW (list): Lijst van paden naar de RETW-bestanden.

        Returns:
            None
        """
        logger.info("Reporting on dependencies")
        dag = DagReporting()
        dag.build_dag(files_RETW=files_RETW)
        # Visualization of the ETL flow for all RETW files combined
        path_output = str(self.config.extractor.path_output / "ETL_flow.html")
        dag.plot_etl_dag(file_html=path_output)
        path_output = str(self.config.extractor.path_output / "RETW_dependencies.html")
        dag.plot_file_dependencies(file_html=path_output)
        path_output = str(self.config.extractor.path_output / "mappings.html")
        dag.plot_mappings(file_html=path_output)

    def _generate_mdde_deployment(self, dag_etl: DagImplementation) -> None:
        """
        Genereert MDDE deployment scripts op basis van de ETL-DAG.

        Deze functie maakt gebruik van de opgegeven ETL-DAG om de juiste volgorde van mappings te bepalen
        en genereert vervolgens de benodigde MDDE deployment scripts.

        Args:
            dag_etl (DagImplementation): De geïmplementeerde ETL-DAG.

        Returns:
            None
        """
        logger.info("Generating MDDE scripts")
        deploy_mdde = DeploymentMDDE(
            path_data=self.config.deploy_mdde.path_data_input,
            schema=self.config.deploy_mdde.schema,
            path_output=self.config.deploy_mdde.path_output,
        )
        mapping_order = dag_etl.get_run_config(
            deadlock_prevention=DeadlockPrevention.TARGET
        )
        mapping_dependencies = dag_etl.get_load_dependencies()
        mapping_clusters = dag_etl.get_mapping_clusters(
            schemas=self.config.deploy_mdde.schemas_datamart
        )
        models_info = dag_etl.get_files()
        deploy_mdde.process(
            info_models=models_info,
            mapping_order=mapping_order,
            mapping_dependencies=mapping_dependencies,
            datamart_clusters=mapping_clusters,
        )

    def _generate_code(self, dag_etl: DagImplementation) -> None:
        """
        Genereert de deployment code op basis van de opgegeven ETL-DAG.

        Deze functie initialiseert de DDL-generator en genereert de benodigde DDL-bestanden
        voor de entiteiten en mappings in de ETL-DAG.

        Args:
            dag_etl (DagImplementation): De geïmplementeerde ETL-DAG.

        Returns:
            None
        """
        logger.info("Start generating deployment code")
        ddl_generator = DDLGenerator(params=self.config.generator)
        ddl_generator.generate_ddls(dag_etl=dag_etl)

    def _add_to_repository(self) -> None:
        """Voegt de gegenereerde code toe aan de DevOps repository.

        Deze functie beheert het klonen, opschonen, toevoegen en publiceren van de gegenereerde code naar de DevOps repository.

        Returns:
            None
        """
        devops_handler = RepositoryManager(config=self.config.devops)
        devops_handler.clone()
        path_source = self.config.generator.path_output
        devops_handler.clean_directory_in_repo()
        devops_handler.add_directory_to_repo(path_source=path_source)
        devops_handler.publish()
