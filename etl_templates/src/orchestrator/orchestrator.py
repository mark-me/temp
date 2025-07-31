import functools
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

BOLD_BLUE = "\x1b[1;34m"
BOLD_RED = "\x1b[1;31m"
UNDERLINE = "\x1b[4m"
BOLD_YELLOW = "\x1b[1;33m"
RESET = "\x1b[0m"


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
        self.process_steps = iter(
            [
                "1/5) Extraheren uit Power Designer documenten",
                "2/5) Integreren van Power Designer extracten",
                "3/5) Genereren model en mapping code",
                "4/5) Genereren MDDE schema",
                "5/5) Toevoegen aan DevOps repository",
            ]
        )
        self.step_current = next(self.process_steps)

    @staticmethod
    def _decorator_proces_issues(func):
        """
        Decorator die het verwerkingsproces van een stap omhult en issues afhandelt.

        Deze decorator print de huidige processtap, voert de functie uit, en controleert op waarschuwingen of fouten.
        Indien waarschuwingen worden gevonden, wordt de gebruiker gevraagd of het proces moet doorgaan of gestopt.

        Args:
            func (callable): De te decoreren functie.

        Returns:
            callable: De omhulde functie met issue-afhandeling.
        """

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            """
            Omhult de gedecoreerde functie om issues tijdens verwerkingsstappen af te handelen.

            Deze wrapper voert de functie uit, controleert op waarschuwingen of fouten,
            en vraagt de gebruiker of het proces moet doorgaan of stoppen als er issues zijn gevonden.

            Args:
                *args: Positie-argumenten voor de gedecoreerde functie.
                **kwargs: Keyword-argumenten voor de gedecoreerde functie.

            Returns:
                Any: Het resultaat van de gedecoreerde functie.

            Raises:
                ExtractionIssuesFound: Indien fouten zijn gevonden of de gebruiker kiest om te stoppen na waarschuwingen.
            """
            print(f"{BOLD_BLUE}{self.step_current}{RESET}", file=sys.stdout)
            lst_answers_yes = ["", "J", "JA", "JAWOHL", "Y", "YES"]
            lst_answers_no = ["N", "NEE", "NEIN", "NO"]

            func_result = func(self, *args, **kwargs)

            try:
                self.step_current = next(self.process_steps)
            except StopIteration:
                return func_result

            max_severity_level = issue_tracker.max_severity_level()
            if max_severity_level in ["WARNING", "ERROR"]:
                file_issues = self.config.path_intermediate / "extraction_issues.csv"
                issue_tracker.write_csv(file_csv=file_issues)

            if max_severity_level == "WARNING" and not self.config.ignore_warnings:
                while True:
                    answer = input(
                        f"{BOLD_YELLOW}Waarschuwingen gevonden, wil je doorgaan met {self.step_current}? (J/n):{RESET}"
                    )
                    if answer.upper() in lst_answers_no:
                        raise ExtractionIssuesFound(
                            f"""Verwerking gestopt op verzoek van de gebruiker nadat er waarschuwingen zijn aangetroffen.\nZie rapport: {file_issues}"""
                        )
                    elif answer.upper() in lst_answers_yes:
                        break
                    else:
                        print(
                            f"{BOLD_RED}'{answer}' behoort niet tot de mogelijke antwoorden (j/n).{RESET}",
                            file=sys.stdout,
                        )
            elif max_severity_level == "ERROR":
                raise ExtractionIssuesFound(
                    f"Verwerking gestopt nadat er issues zijn aangetroffen.\nZie rapport: {file_issues}"
                )

            return func_result

        return wrapper

    def start_processing(self, skip_devops: bool = False) -> None:
        """Start het Genesis verwerkingsproces.

        Orkestreert extractie, afhankelijkheidsanalyse, codegeneratie en repository-operaties voor het ETL-proces.

        Args:
            skip_devops (bool): Indien True, worden DevOps repository-operaties overgeslagen.

        Returns:
            None
        """
        print(
            f"{BOLD_BLUE}{UNDERLINE}Start Genesis verwerking: {self.config.title} {self.config._version}.{RESET}\n",
            file=sys.stdout,
        )
        # Extraheert data uit de Power Designer ldm bestanden
        files_RETW = self._extract()
        # Integreer alle data uit de verschillende bestanden
        dag_etl = self._integrate_files(files_RETW=files_RETW)
        # Genereer code voor doelschema's en mappings
        self._generate_code(dag_etl=dag_etl)
        # Genereer code voor ETL deployment
        self._generate_mdde_deployment(dag_etl=dag_etl)
        # Voegt gegenereerde code en database objecten toe aan het repository
        if not skip_devops:
            self._add_to_repository()
        else:
            logger.info(
                "Repository afhandeling zijn overgeslagen door de 'skip_devops' flag."
            )

    @_decorator_proces_issues
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
            raise ValueError(
                "Configuratiefout: geen PowerDesigner-bestanden opgegeven. Genesis verwerking kan niet worden gestart."
            )
        files_pd_ldm = self.config.power_designer.files
        for file_pd_ldm in tqdm(
            files_pd_ldm,
            desc="Extracten Power Designer bestanden",
            colour="#d7f5cb",
            disable=not sys.stdout.isatty(),
        ):
            try:
                document = PDDocument(file_pd_ldm=file_pd_ldm)
                file_RETW = (
                    self.config.extractor.path_output / f"{file_pd_ldm.stem}.json"
                )
                document.write_result(file_output=file_RETW)
                logger.info(
                    f"Het logisch data model en mappings van '{file_pd_ldm}' geëxtraheerd en geschreven naar '{file_RETW}'"
                )
                lst_files_RETW.append(file_RETW)
            except (IOError, OSError, ValueError) as e:
                logger.error(
                    f"Fout bij extractie van Power Designer bestand '{file_pd_ldm}': {e}",
                    exc_info=True,
                )
        return lst_files_RETW

    @_decorator_proces_issues
    def _integrate_files(self, files_RETW: list[Path]) -> DagImplementation:
        """
        Integreert de opgegeven RETW-bestanden tot een ETL-DAG en genereert visualisaties.

        Deze functie bouwt een implementatie-DAG op basis van de RETW-bestanden en
        genereert visualisaties van de ETL-flow, afhankelijkheden en mappings.

        Args:
            files_RETW (list[Path]): Lijst van paden naar de RETW-bestanden.

        Returns:
            DagImplementation: De geïmplementeerde ETL-DAG.
        """
        logger.info("Create ETL Dag with implementation information")
        dag = DagReporting()
        dag.build_dag(files_RETW=files_RETW)
        self._visualize_etl_flow(dag)
        self._visualize_file_dependencies(dag)
        self._visualize_mappings(dag)
        return dag

    def _visualize_etl_flow(self, dag: DagReporting) -> None:
        """Genereert de ETL-flow visualisatie."""
        print(f"{BOLD_BLUE}\tReview rapporten over:{RESET}")
        path_output = self.config.extractor.path_output / "ETL_flow.html"
        dag.plot_etl_dag(file_html=path_output)
        print(f"{BOLD_BLUE}\t* ETL-flow: {UNDERLINE}{path_output}{RESET}")

    def _visualize_file_dependencies(self, dag: DagReporting) -> None:
        """Genereert de Power Designer bestandsafhankelijkheden visualisatie."""
        path_output = self.config.extractor.path_output / "RETW_dependencies.html"
        dag.plot_file_dependencies(file_html=path_output)
        print(
            f"{BOLD_BLUE}\t* Power Designer bestandsafhankelijkheden: {UNDERLINE}{path_output}{RESET}"
        )

    def _visualize_mappings(self, dag: DagReporting) -> None:
        """Genereert de mappings visualisatie."""
        path_output = self.config.extractor.path_output / "mappings.html"
        dag.plot_mappings(file_html=path_output)
        print(f"{BOLD_BLUE}\t* Mappings: {UNDERLINE}{path_output}{RESET}")

    @_decorator_proces_issues
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

    @_decorator_proces_issues
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

    @_decorator_proces_issues
    def _add_to_repository(self) -> None:
        """Voegt de gegenereerde code toe aan de DevOps repository.

        Deze functie beheert het klonen, opschonen, toevoegen en publiceren van de gegenereerde code naar de DevOps repository.

        Returns:
            None
        """
        devops_handler = RepositoryManager(config=self.config.devops)
        devops_handler.clone()
        devops_handler.clean_directory_in_repo()
        devops_handler.add_directory_to_repo(
            path_source=self.config.generator.path_output
        )
        devops_handler.publish()
