import argparse
import sys
from pathlib import Path

from orchestrator import ExtractionIssuesFound, Orchestrator

GREEN = "\033[0;34m"
RED = "\033[1;33m"
RESET = "\033[0m"

def main():
    """
    Start het Genesis orkestratieproces via de command line interface.

    Ontleedt command line argumenten, initialiseert de Orchestrator klasse met het opgegeven configuratiebestand en start de verwerking.
    """
    parser = argparse.ArgumentParser(description="De Genesis workflow orkestrator")
    print(
        """{GREEN}\n
     _____                      _
    / ____|                    (_)
   | |  __  ___ _ __   ___  ___ _ ___
   | | |_ |/ _ \\ '_ \\ / _ \\/ __| / __|
   | |__| |  __/ | | |  __/\\__ \\ \\__ \\
    \\_____|\\___|_| |_|\\___||___/_|___/
                              MDDE Douane{RESET}
    """,
        file=sys.stdout,
    )
    parser.add_argument("config_file", help="Locatie van een configuratiebestand")
    parser.add_argument(
        "-s", "--skip", action="store_true", help="Sla DevOps deployment over"
    )
    args = parser.parse_args()
    genesis = Orchestrator(file_config=Path(args.config_file))
    try:
        genesis.start_processing(skip_devops=args.skip)
    except ExtractionIssuesFound as e:
        print(f"{RED}{e}{RESET}", file=sys.stdout)
    else:
        print(f"{GREEN}Afgerond zonder fouten.{RESET}", file=sys.stdout)


if __name__ == "__main__":
    main()
