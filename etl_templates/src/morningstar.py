import argparse
import sys
from pathlib import Path

from integrator import MappingRef, FailureStrategy
from orchestrator_morningstar import Orchestrator

def main():
    """
    Voert de Genesis failure report simulatie uit via de command line interface.

    Passeert argumenten, initialiseert de rapportageklasse en genereert een ETL-failure rapport.

    """
    parser = argparse.ArgumentParser(description="De Genesis failure report simulatie")
    print(
    """\n
    __  __                   _                 _
    |  \\/  | ___  _ __ _ __ (_)_ __   __ _ ___| |_ __ _ _ __
    | |\\/| |/ _ \\| '__| '_ \\| | '_ \\ / _` / __| __/ _` | '__|
    | |  | | (_) | |  | | | | | | | | (_| \\__ \\ || (_| | |
    |_|  |_|\\___/|_|  |_| |_|_|_| |_|\\__, |___/\\__\\__,_|_|
                                    |___/
                                            MDDE Douane
                                            Failure report
    """,
        file=sys.stdout,
    )
    parser.add_argument("config_file", help="Locatie van een configuratiebestand")
    args = parser.parse_args()

    path_output = Path("etl_templates/intermediate")
    path_output.mkdir(parents=True, exist_ok=True)
    etl_simulator = Orchestrator(path_config=Path(args.config_file))
    etl_simulator.build_dag()

    failed_mappings = [
        MappingRef("DA_Central", "SL_KIS_AggrMaxOfMutDatEad"),
        MappingRef("DA_Central", "SlDmsCustomsvalue"),
    ]
    etl_simulator.set_mappings_failed(mapping_refs=failed_mappings)

    # Scenario: Only successors
    etl_simulator.start_etl(failure_strategy=FailureStrategy.DIRECT_PREDECESSORS)
    etl_simulator.plot_etl_fallout(
        file_png=path_output / "only_successors.png"
    )

    # Scenario: All of shared target
    etl_simulator.start_etl(failure_strategy=FailureStrategy.ALL_OF_SHARED_TARGET)
    etl_simulator.plot_etl_fallout(
        file_png=path_output / "all_of_shared_target.png"
    )


if __name__ == "__main__":
    main()
