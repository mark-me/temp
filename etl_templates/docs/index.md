![Genesis](images/logo.png){ align=right width="160" }

# Welkom bij de Genesis documentatie

## Wat is Genesis?

Genesis is een programma dat het mogelijk maakt om Power Designer-logische datamodeldocumenten om te zetten in code die tabellen en ETL-programma’s genereert om die tabellen te vullen. Om dit doel te bereiken doorloopt Genesis een aantal stappen:

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
            Generator[["Code<br>DB objecten"]]
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

## Componenten

### Orkestrator

Het startpunt voor de "Genesis" is de workflow-orkestrator waar alle andere belangrijke componenten samenkomen. De voornaamste functie is het beheren en uitvoeren van de stappen die in de configuratie zijn gedefinieerd, mogelijk inclusief uitrol-stappen. Meer informatie over dit proces is te vinden op de [Orkestrator-pagina](Orkestrator.md).

#### Configuratie

De orchestrator flow wordt bepaald door een configuratiebestand. Meer informatie over configuratiebestanden en de methodes waarmee deze worden ingelezen en geverifieerd is te vinden op de [Configuratie-pagina](Configuration.md)

### Power Designer extractie

De Extractor neemt een Power Designer-logisch datamodeldocument (herkenbaar aan de extensie .ldm) en extraheert model- en mapping-relevante informatie in een JSON-bestand (vaak aangeduid als een RETW-bestand). Meer informatie over dit proces is te vinden op de [Extractor-pagina](Extractor.md).

### Bepaling afhankelijkheden

Deze component biedt inzicht in het netwerk van entiteiten en mappings, op basis van RETW-outputbestanden, om te bepalen:

* wat de juiste volgorde is van mappings in de ETL-flow en of de ETL-flow geen gesloten lussen bevat (ETL-flows moeten [acyclisch](https://nl.wikipedia.org/wiki/Gerichte_acyclische_graaf) zijn),
* wat de gevolgen zijn van een mislukte stap in het ETL-proces en
* wat de afhankelijkheden zijn tussen RETW-bestanden voor entiteiten.

Meer informatie is te vinden op de pagina [Afhankelijkheidscontrole](Dependency_checker.md).

### Generator code DB objecten

De Generator gebruikt de output van de Extractor om code te genereren die database objecten kan aanmaken en ETL-processen kan implementeren. Meer informatie hierover is te vinden op de [Generator-pagina](Generator.md).

### Generator MDDE deployment code

De MDDE Deployment zorgt ervoor dat de ETL processen in een pipeline kunnen worden gezet voor de ETL orchestratie. Meer informatie hierover is te vinden op de [Deployment MDDE-pagina](Deploy_MDDE.md).

### DevOps repository management

De Repository handler zorgt ervoor dat alle gegenereerde code naar DevOps wordt gebracht zodat deze op Azure geimplementeerd kan worden. Meer informatie hierover is te vinden op de [Repository management-pagina](Repository_Manager.md).

## Hulpprogramma’s

Naast de kernfunctionaliteit heeft Genesis ook enkele hulpmiddelen voor Data Modellers en Data Engineers om:

* de [impact van wijzigingen te bepalen](Dependency_checker.md) die op het punt staan te worden doorgevoerd,
* [afhankelijkheden tussen Power Designer-documenten op te sporen](Dependency_checker.md).
* [Logger](Logtools.md) die naast reguliere logging ook issues vastlegt in de modellen en mappings deze gebruikt kan worden voor de Genesis flow en om de modelleurs op de hoogte te stellen van deze issues.
* [Documentatie generatie](Documentation_Creation.md) waarmee Markdown bestanden en [DocStrings](https://en.wikipedia.org/wiki/Docstring) in de code omgezet kan worden tot documentatiepagina's (die je hier leest).

## Project folderstructuur

```bash
etl_templates
├───docs  # Source voor documentatie
├───input # Placeholder voor Power Designer documenten
├───site  # Gegenereerde HTML documentatie (niet aanwezig in repo).
└───src
    ├───dependencies_checker   # Bepaling afhankelijkheden
    ├───deploy_mdde            # MDDE deployment code
    ├───generator              # Code DB objecten
    ├───genesis
    |      ├───config_file.py  # Configuratie lezen
    |      ├───main.py         # Start-script Genesis
    |      └───orchestrator.py # Orkestrator
    ├───logtools               # Logging en issue tracking
    ├───pd_extractor           # Power Designer extractie
    └───repository_manager     # DevOps repository management
```
