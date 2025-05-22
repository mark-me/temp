# Logtools

![Logtools](images/logging.png){ align=right width="90" }

Gecentraliseerde logging en foutregistratie. Deze package biedt een herbruikbare en uitbreidbare loggingconfiguratie, inclusief:

- Gecentraliseerde loggingconfiguratie (JSON-formaat)
- Logging naar stdout én naar een roterend logbestand
- Een aangepaste logging handler die waarschuwingen en fouten bijhoudt
- Een eenvoudige interface om een logger te verkrijgen en parsingproblemen te controleren

## 🚀 Gebruik

### 1. Importeer de logger en issue tracker

In elk module waar je wilt loggen:

```python
from logtools import get_logger, issue_tracker

logger = get_logger(__name__)
logger.info("Dit is een logbericht.")
logger.warning("Deze waarschuwing wordt bijgehouden.")

if issue_tracker.has_issues():
    print("Problemen gevonden:", issue_tracker.get_issues())
    issue_tracker.write_csv(file_csv="problemen.csv")
else:
    print("Geen problemen.")
```

### 2. Wat wordt er gelogd?

De logoutput is geformatteerd als JSON en bevat tijdstempels, niveau, bericht, module, functienaam en proces-ID. Standaard wordt er:

- Gelogd naar **stdout**
- Geschreven naar een roterend bestand genaamd `log.json`

### 3. Wat wordt er bijgehouden?

Alleen logberichten op niveau `WARNING` of hoger worden bijgehouden in het geheugen door de aangepaste `IssueTrackingHandler`. Hiermee kun je:

- Problemen loggen tijdens verwerking
- Aan het einde controleren of er fouten zijn opgetreden

## Pakketstructuur

```md
logtools/
├── __init__.py           # Publieke API: get_logger, issue_tracker
├── log_config.py         # Loggingconfiguratie als dict
├── log_manager.py        # Past configuratie toe en stelt logger + tracker beschikbaar
└── issue_tracking.py     # Aangepaste handler die problemen bijhoudt
```

## Aanpassen

- Je kunt `log_config.py` aanpassen om andere formatters te gebruiken of te loggen naar extra bestemmingen (zoals syslog of externe diensten).
- De `IssueTrackingHandler` kan worden uitgebreid om extra context zoals tijdstempels of thread-informatie vast te leggen.

## Belangrijke componenten

- **`get_logger(__name__)`**
  Geeft een vooraf geconfigureerde logger terug voor de aanroepende module. Maakt logging-instellingen eenvoudig en consistent binnen de applicatie.

- **`issue_tracker`**
  Biedt methoden zoals `has_issues()` en `get_issues()` om vastgelegde waarschuwingen en fouten op te vragen.

- **`log_config.py`**
  Bevat de loggingconfiguratie als een Python dictionary. Definieert het logformaat (JSON), logniveaus en uitvoerdoelen (stdout en bestand). Ontwikkelaars kunnen dit bestand aanpassen naar hun wensen.

- **`log_manager.py`**
  Past de configuratie uit `log_config.py` toe en stelt de functies `get_logger()` en `issue_tracker` beschikbaar.

- **`issue_tracking.py`**
  Implementeert de `IssueTrackingHandler`, een aangepaste logging handler die waarschuwingen en fouten opslaat voor latere controle. Dit vormt de kern van de foutregistratie.

- **JSON Logging**
  Logregels worden weggeschreven in JSON-formaat, wat ze gemakkelijk te verwerken maakt in loganalyse- en monitoringtools.

- **Roterende Logbestanden**
  Logregels worden opgeslagen in een roterend bestand genaamd `log.json`, om te voorkomen dat logbestanden onbeperkt groeien.

## API referentie

### Log manager

#### ::: src.logtools.log_manager.get_logger

### Issue tracker

#### ::: src.logtools.issue_tracking.IssueTrackingHandler
