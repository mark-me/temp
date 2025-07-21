import argparse
import sys
from pathlib import Path

from integrator import EtlSimulator, MappingRef, FailureStrategy
from logtools import get_logger
from orchestrator import ConfigFile
from pd_extractor import PDDocument
from tqdm import tqdm

logger = get_logger(__name__)

def build_dag(file_config: str) -> EtlSimulator:
    """
    Initialiseert een nieuwe ReportEtlSimulation instantie met het opgegeven configuratiebestand.

    Laadt de configuratie, initialiseert de ETL simulator en bereidt de rapportage voor.

    Args:
        file_config (str): Pad naar het configuratiebestand.
    """
    file_config = Path(file_config)
    config = ConfigFile(file_config=file_config)
    logger.info(
        f"Failure report geïnitialiseerd met configuratie uit '{file_config}'"
    )
    paths_RETW = []
    dag = EtlSimulator()

    paths_RETW = []
    # Extractie van data uit Power Designer bestanden
    if not config.power_designer.files:
        logger.warning(
            "Geen PowerDesigner-bestanden geconfigureerd. Genesis verwerking wordt afgebroken."
        )
        return []
    files_pd_ldm = config.power_designer.files
    for file_pd_ldm in tqdm(
        files_pd_ldm, desc="Extracten Power Designer bestanden", colour="#d7f5cb"
    ):
        logger.info(f"Start extractie van Power Designer bestand '{file_pd_ldm}'")
        document = PDDocument(file_pd_ldm=file_pd_ldm)
        file_RETW = config.extractor.path_output / f"{file_pd_ldm.stem}.json"
        document.write_result(file_output=file_RETW)
        logger.info(
            f"Het logisch data model en mappings van '{file_pd_ldm}' geëxtraheerd en geschreven naar '{file_RETW}'"
        )
        paths_RETW.append(file_RETW)

    dag.build_dag(files_RETW=paths_RETW)
    return dag


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

    path_output = Path("etl_templates/intermediate")
    elt_simulator = build_dag(file_config=Path(args.config_file))

    failed_mappings = [
        MappingRef("DA_Central", "SL_KIS_AggrMaxEndDateEad"),
        MappingRef("DA_Central", "SlDmsCustomsvalue"),
    ]
    elt_simulator.set_mappings_failed(mapping_refs=failed_mappings)

    elt_simulator.start_etl(failure_strategy=FailureStrategy.ONLY_SUCCESSORS)
    elt_simulator.plot_etl_fallout(
        file_png=path_output / "only_successors.png"
    )


    elt_simulator.start_etl(failure_strategy=FailureStrategy.ALL_OF_SHARED_TARGET)
    elt_simulator.plot_etl_fallout(
        file_png=path_output / "all_of_shared_target.png"
    )


if __name__ == "__main__":
    main()
