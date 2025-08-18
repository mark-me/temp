![Morningstar](images/logo.png){ align=right width="160" }
# Morningstar

Deze pagina is de documentatie voor Morningstar: een opdrachtregeltool voor het simuleren en rapporteren van ETL- (Extract, Transform, Load) processtoringen, specifiek in de context van datamodellen en mappings die zijn geëxtraheerd uit PowerDesigner-bestanden. Het coördineert de extractie van logische datamodellen, bouwt een ETL-simulatie-DAG (Directed Acyclic Graph), injecteert storingsscenario’s en genereert visuele rapporten van de gevolgen van deze storingen. De tool is bedoeld voor gebruik binnen het "Genesis" ETL-framework en ondersteunt storingsanalyse en rapportage voor dataintegratieprojecten.

## Belangrijkste onderdelen

* **Imports en logging** Het bestand importeert verschillende modules voor ETL-simulatie (EtlSimulator, MappingRef, FailureStrategy), configuratiebeheer (ConfigFile), PowerDesigner-extractie (PDDocument), voortgangsvisualisatie (tqdm) en logging (get_logger). Logging wordt ingesteld voor status- en foutmeldingen.
* **build_dag(file_config: str) -> EtlSimulator** Deze functie initialiseert de ETL-simulatieomgeving:
    * Laadt configuratie uit een opgegeven bestand.
    * Extraheert logische datamodellen en mappings uit PowerDesigner-bestanden en zet deze om naar JSON.
    * Bouwt de ETL-DAG op basis van de geëxtraheerde gegevens.
    * Geeft een EtlSimulator-instantie terug die klaar is voor verdere simulatie.

* **main()**
    * Het startpunt voor de opdrachtregelinterface:
    * Analyseert opdrachtregelargumenten om het pad naar het configuratiebestand te verkrijgen.
    * Stelt uitvoermappen in.

Roept build_dag aan om de ETL-simulatie voor te bereiden.

Definieert een set mappings die als mislukt worden gesimuleerd.

Voert twee storingssimulaties uit met verschillende strategieën (DIRECT_PREDECESSORS en ALL_OF_SHARED_TARGET).

Genereert visuele rapporten (PNG-afbeeldingen) van de gevolgen voor elk scenario.

Storingssimulatie en rapportage
Met de tool kunnen gebruikers specifieke mapping-storingen simuleren en hun impact op het ETL-proces analyseren met behulp van verschillende propagatie-strategieën voor storingen. De resultaten worden gevisualiseerd en als afbeeldingen opgeslagen voor verdere analyse.

Gebruik via de opdrachtregel
Het script is ontworpen om rechtstreeks te worden uitgevoerd en biedt een gebruiksvriendelijke interface voor ETL-storingssimulatie en -rapportage.
`etl_templates/src/morningstar.py` biedt een opdrachtregeltool voor het simuleren en rapporteren van fouten in ETL-processen (Extract, Transform, Load), specifiek binnen de context van het "Genesis" ETL-systeem. Het orkestreert de extractie van datamodellen uit Power Designer-bestanden, bouwt een ETL-simulatie-DAG (Directed Acyclic Graph), en genereert visuele rapporten van faalscenario’s. De tool is bedoeld voor gebruik door data engineers of ontwikkelaars om de impact van specifieke mapping-fouten in ETL-pijplijnen te analyseren.

---

## Voorbeelden

[Voorbeeld 1](ETL_flow.html){target="_blank"}

---

## API

### ::: src.morningstar.orchestrator.Orchestrator