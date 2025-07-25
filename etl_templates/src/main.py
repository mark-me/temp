import argparse
import sys
from pathlib import Path

from orchestrator import Orchestrator


def main():
    """
    Start het Genesis orkestratieproces via de command line interface.

    Ontleedt command line argumenten, initialiseert de Orchestrator klasse met het opgegeven configuratiebestand en start de verwerking.
    """
    parser = argparse.ArgumentParser(description="De Genesis workflow orkestrator")
    print(
        """\n
     _____                      _
    / ____|                    (_)
   | |  __  ___ _ __   ___  ___ _ ___
   | | |_ |/ _ \\ '_ \\ / _ \\/ __| / __|
   | |__| |  __/ | | |  __/\\__ \\ \\__ \\
    \\_____|\\___|_| |_|\\___||___/_|___/
                            MDDE Douane
    """,
        file=sys.stdout,
    )
    parser.add_argument("config_file", help="Locatie van een configuratiebestand")
    parser.add_argument(
        "-s", "--skip", action="store_true", help="Sla DevOps deployment over"
    )
    args = parser.parse_args()
    genesis = Orchestrator(file_config=Path(args.config_file))
    genesis.start_processing(skip_devops=args.skip)

if __name__ == "__main__":
    main()

