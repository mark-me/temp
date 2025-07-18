import logging
import csv


class IssueTrackingHandler(logging.Handler):
    """Een logging-handler die logberichten opslaat als issues.

    Deze handler vangt logberichten op met een ernstniveau van WARNING of hoger
    en slaat ze op als woordenboeken in een interne lijst. Hij biedt methoden
    om te controleren of er issues zijn, de lijst met issues op te halen en
    deze issues te exporteren naar een CSV-bestand.
    """

    def __init__(self):
        super().__init__()
        self.issues = []

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record.

        Args:
            record: The log record to emit.
        """
        if record.levelno >= logging.WARNING:
            self.issues.append(
                {
                    "severity": record.levelname,
                    "message": record.getMessage(),
                    "module": record.module,
                    "line": record.lineno,
                    "func": record.funcName,
                }
            )

    def max_severity_level(self) -> str:
        if self.issues:
            return min(self.issues, key=lambda x: x["severity"])["severity"]

    def has_errors(self) -> bool:
        """Controleer of er errors zijn gelogd.

        Retourneert:
            True als er errors zijn, anders False.
        """
        return self.max_severity_level() == "ERROR"

    def get_issues(self) -> list:
        """Haalt een lijst met gelogde issues op.

        Returns:
            A lijst met issue dictionaries.
        """
        return self.issues

    def write_csv(self, file_csv: str) -> None:
        """Exporteer de gelogde issues naar een CSV bestand.

        Args:
            file_csv: De locatie van het CSV bestand.
        """
        if self.issues:
            with open(file_csv, "w", encoding="utf8", newline="") as output_file:
                fc = csv.DictWriter(
                    output_file,
                    fieldnames=self.issues[0].keys(),
                    dialect="excel",
                    quoting=csv.QUOTE_STRINGS,
                )
                fc.writeheader()
                fc.writerows(self.issues)
        else:
            print("Er zijn geen issues gevonden om weg te schrijven. Genesis heeft succesvol gerund")
