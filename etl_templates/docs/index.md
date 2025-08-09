![Genesis](images/logo.png){ align=right width="160" }

# Welkom bij de Genesis documentatie

Genesis is een modulair Python-framework dat Power Designer-modellen omzet naar bruikbare database- en ETL-code. Het helpt data-engineers en modelleurs om complexe datamodellen automatisch te vertalen naar uitvoerbare oplossingen in Azure DevOps.

Dankzij een configureerbare, stap-voor-stap workflow kunnen model- en mappingdata worden geëxtraheerd, data uit de  Power Designer bestanden worden geïntegreerd, deployment-scripts gegenereerd en repositories bijgewerkt — volledig geautomatiseerd en reproduceerbaar.

## Hoe werkt Genesis?

Genesis bestaat uit een reeks samenwerkende componenten die elk een fase in de dataproductieketen ondersteunen: van modelinvoer tot deployment. De volledige workflow wordt georkestreerd vanuit een centrale configuratie. Onderstaande figuur toont het overzicht van de hoofdfasen:

```mermaid
graph
    PDDocument("Power Designer<br>LDM-bestand(en)")

    subgraph Genesis
    direction TB
    Configuratie
        subgraph Orkestrator
        direction LR
            RETW[["Power Designer<br>extractie"]]
            Dependency[["Bepaling<br>afhankelijkheden"]]
            Generator[["Code DB & ETL<br>objecten"]]
            DeployMDDE[["MDDE<br>deployment code"]]
            RepositoryHandler[["DevOps repository<br>management"]]
        end
    end
    DedicatedPool("Dedicated Pool<br>deployment")
    PDDocument --> Genesis
    Genesis --> DedicatedPool

    Configuratie --> Orkestrator

    RETW --> Dependency
    Dependency --> Generator
    Generator --> DeployMDDE
    DeployMDDE --> RepositoryHandler

    style PDDocument fill:#FFFFE0,stroke:#FFD700;
    style Configuratie fill:#FFFFE0,stroke:#FFD700;
    style DedicatedPool fill:#87CEFA,stroke:#808080;
    style Genesis fill:#DCDCDC,stroke:#191970;

    classDef functional fill:#90EE90,stroke:#006400;
    class RETW,Dependency,Generator,DeployMDDE,RepositoryHandler functional
```

Het Python-script, [```main.py```](#src.main.main) in de directory ```src```, dient als het startpunt waarmee de "Genesis" workflow-orkestrator wordt opgestart, waarbij een [configuratiebestand](Orchestrator/Configuration.md) kan worden gespecificeerd als parameter. De DevOps operaties kunnen overslagen worden door de parameter `-s` te gebruiken bij het opstarten van het main script via de command line.

```bash
python main.py etl_templates/config.yml -s
```

## Voor wie is Genesis?

Genesis is ontwikkeld voor:

* **Data Modellers** die werken met Power Designer-modellen;
* **Data Engineers** die ETL-processen implementeren op basis van deze modellen;
* **DevOps-teams** die verantwoordelijk zijn voor de uitrol van gegenereerde databronnen en pipelines.

## Componenten

### Orkestrator

De verschillende stappen in "Genesis"  worden door de workflow-orkestrator doorlopen, en is de plek waar alle andere belangrijke componenten samenkomen. De voornaamste functie is het beheren en uitvoeren van de stappen die in de configuratie zijn gedefinieerd, mogelijk inclusief uitrol-stappen. Meer informatie over dit proces is te vinden op de [Orkestrator-pagina](Orchestrator/Orchestrator.md).

#### Configuratie

De Orkestrator-flow wordt bepaald door een configuratiebestand. Meer informatie over configuratiebestanden en de methodes waarmee deze worden ingelezen en geverifieerd is te vinden op de [Configuratie-pagina](Orchestrator/Configuration.md)

### Power Designer extractie

De Extractor neemt een Power Designer-logisch datamodeldocument (herkenbaar aan de extensie .ldm) en extraheert model- en mapping-relevante informatie in een JSON-bestand (vaak aangeduid als een RETW-bestand). Meer informatie over dit proces is te vinden op de [Extractor-pagina](Extractor.md).

### Integreren ETL DAG

De ETL DAG Integrator biedt een modulaire en uitbreidbare set Python-componenten voor het modelleren, analyseren, implementeren en visualiseren van **ETL-workflows** op basis van **Directed Acyclic Graphs (DAG’s)**. Deze workflows worden gedefinieerd aan de hand output van de Power Designer extractie. Deze package:

- bouwt de structuur van de ETL-DAG,
- breidt deze structuur uit met uitvoeringslogica, zoals run volgorde en parallellisatie en deadlock-preventie, en
- voegt visualisatie- en rapportagemogelijkheden toe.

Meer informatie is te vinden op de pagina [Integreren ETL DAG](Integrator/Integrator.md).

### Generator code DB objecten

De Generator gebruikt de output van de Extractor om code te genereren die database objecten kan aanmaken en ETL-processen kan implementeren. Meer informatie hierover is te vinden op de [Generator-pagina](Generator.md).

### Generator MDDE deployment code

De MDDE Deployment zorgt ervoor dat de ETL processen in een pipeline kunnen worden gezet voor de ETL orchestratie. Meer informatie hierover is te vinden op de [Deployment MDDE-pagina](Deploy_MDDE/Deploy_MDDE.md).

### DevOps repository management

De Repository Handler plaatst de gegenereerde code automatisch in een DevOps-repository, zodat deze kan worden uitgerold in een Azure-omgeving. Meer informatie hierover is te vinden op de [Repository management-pagina](Repository_Manager.md).

## Hulpprogramma’s

Genesis bevat daarnaast handige tools voor Data Modellers en Data Engineers om:

* de [impact van wijzigingen](Integrator/Integrator.md) te bepalen;
* [afhankelijkheden tussen Power Designer-documenten](Integrator/Integrator.md) te analyseren;
* met behulp van de [Logger](Logtools.md) automatisch issues vast te leggen in modellen en mappings;
* met [Documentatie generatie](Documentation_Creation.md) Markdown-bestanden en docstrings om te zetten in leesbare documentatiepagina’s.

## Project folderstructuur

```bash
etl_templates
├───docs  # Source voor documentatie
├───input # Placeholder voor Power Designer documenten
├───site  # Gegenereerde HTML documentatie (niet aanwezig in repo).
└───src
    ├───deploy_mdde            # MDDE deployment code
    ├───generator              # Code DB objecten
    ├───integrator             # Integreren ETL DAG
    ├───logtools               # Logging en issue tracking
    └───orchestrator
    |      ├───config_file.py  # Configuratie lezen
    |      └───orchestrator.py # Orkestrator
    ├───pd_extractor           # Power Designer extractie
    ├───repository_manager     # DevOps repository management
    └───main.py         # Start-script Genesis
```

## API Referentie

### ::: src.main.main
