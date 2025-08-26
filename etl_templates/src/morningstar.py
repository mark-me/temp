import argparse
import sys
from pathlib import Path

from integrator import MappingRef, FailureStrategy
from orchestrator_morningstar import Orchestrator

BOLD_GREEN = "\x1b[1;32m"
RESET = "\x1b[0m"

def main():
    """
    Voert de Genesis failure report simulatie uit via de command line interface.

    Passeert argumenten, initialiseert de rapportageklasse en genereert een ETL-failure rapport.

    """
    parser = argparse.ArgumentParser(description="De Genesis failure report simulatie")
    print(
    f"""{BOLD_GREEN}\n
    __  __                   _                 _             
    |  \\/  | ___  _ __ _ __ (_)_ __   __ _ ___| |_ __ _ _ __ 
    | |\\/| |/ _ \\| '__| '_ \\| | '_ \\ / _` / __| __/ _` | '__|
    | |  | | (_) | |  | | | | | | | | (_| \\__ \\ || (_| | |   
    |_|  |_|\\___/|_|  |_| |_|_|_| |_|\\__, |___/\\__\\__,_|_|   
                                    |___/               
                                            MDDE Douane
                                            Failure report{RESET}
    """,
        file=sys.stdout,
    )
    parser.add_argument("config_file", help="Locatie van een configuratiebestand")
    args = parser.parse_args()

    etl_simulator = Orchestrator(file_config=Path(args.config_file))
    etl_simulator.build_dag()

    failed_mappings = [
        MappingRef("DA_Central", "SL_KIS_AggrMaxOfMutDatEad"),
        MappingRef("DA_Central", "SlDmsCustomsvalue"),
    ]
    failure_strategy = FailureStrategy.DIRECT_PREDECESSORS
    file_png =  "only_successors.png"
    etl_simulator.start_etl_simulator(mapping_refs=failed_mappings, failure_strategy=failure_strategy, file_png=file_png)


    print(f"{BOLD_GREEN}Afgerond zonder fouten.{RESET}", file=sys.stdout)

if __name__ == "__main__":
    main()
