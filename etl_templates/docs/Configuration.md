# Configuratie

![Generator](images/configuration.png){ align=right width="90" }

De Orkestrator gebruikt een [YAML](https://www.redhat.com/en/topics/automation/what-is-yaml){target="_blank"} configuratiebestand on de workflow aan te sturen, en bevat onder andere informatie over de Power Designer LDM bestanden, technische implementatie en DevOps configuratie.

De uitgelezen configuratie wordt opgeslagen in dataclasses die zijn afgeleid van de YAML-structuur om zeker te stellen dat alle verplichte configuratieopties zijn ingevuld en dat hiervoor de juiste datatypes zijn gebruikt.

## Voorbeeld configuratiebestand

Hieronder is een voorbeeld van een configuratiebestand weergegeven, voorzien van commentaar die aangeeft wat de betekenis is van de betreffende onderdelen.

```yaml
# Titel van het project of run
title: "voorbeeld-run"

# Hoofdmap waarin alle tussen-bestanden en output worden opgeslagen
folder_intermediate_root: "/pad/naar/intermediate"

# Instellingen voor PowerDesigner-modellen
power-designer:
  # Submap waar PowerDesigner-bestanden zich bevinden
  folder: "PowerDesigner"
  # Lijst met LDM-bestanden die geanalyseerd moeten worden
  files:
    - "model1.ldm"
    - "model2.ldm"

# Extractor-instellingen
extractor:
  # Submap waar geëxtraheerde gegevens (RETW-bestanden) worden opgeslagen
  folder: "RETW"

# Generator-instellingen
generator:
  # Submap waar gegenereerde output wordt opgeslagen
  folder: "generator"
  # Platformconfiguratie voor templates (bijv. "dedicated-pool" of "shared")
  templates_platform: "dedicated-pool"
  # Naam van JSON-bestand waarin gemaakte DDL-bestanden worden geregistreerd
  created_ddls_json: "list_created_ddls.json"

# Publisher-instellingen
publisher:
  # Pad naar de Visual Studio-projectmap
  vs_project_folder: "VSProject"
  # Pad naar het .sqlproj-bestand binnen het project
  vs_project_file: "./CentralLayer/project.sqlproj"
  # JSON-bestand met een lijst van codelijsten
  codeList_json: "./output/codeList.json"
  # Map waarin codelijsten als input worden verwacht
  codeList_folder: "./input/codeList/"
  # Map met MDDE scripts voor deployment
  mdde_scripts_folder: "./src/mdde_scripts/"

# DevOps-integratie-instellingen
devops:
  # Naam van de Azure DevOps organisatie
  organisation: "organisatie-naam"
  # Naam van het project in Azure DevOps
  project: "project-naam"
  # Repository waarin wijzigingen worden gepusht
  repo: "repository-naam"
  # Naam van de branch waarop gewerkt wordt
  branch: "feature-branch"
  # Werkitem-ID dat gekoppeld wordt aan deze deployment
  work_item: "12345"
  # Omschrijving van het werkitem of de deployment
  work_item_description: "Beschrijving van deze automatische deployment"
```

### Belangrijke componenten

**```ConfigData```**: Bevat globale instellingen zoals de titel van het project en het pad naar de output-map.

**```PowerDesignerConfig```**: Bevat de map en bestanden van PowerDesigner.

**```ExtractorConfig```**: Map voor geëxtraheerde RETW-bestanden.

**```GeneratorConfig```**: Bevat configuratie voor de Generator, inclusief platform-templates, een JSON-bestand met aangemaakte DDL’s en de uitvoer-map.

**```PublisherConfig```**: Bevat instellingen voor de Publisher, zoals paden naar Visual Studio-projecten, codelijsten en MDDE-scripts.

**```DevOpsConfig```**: Bevat informatie met betrekking tot DevOps-integratie, waaronder organisatie, project, repository, branch en details van het werkitem.

#### Klasse diagram

```mermaid
classDiagram
    class ConfigFile {
        +__init__(file_config: str)
        +example_config(file_output: str) : None
        +dir_intermediate : str
        +files_power_designer : list
        +dir_extract : str
        +dir_generate : str
        +devops_config : DevOpsConfig
    }
    class PowerDesignerConfig{
        +folder: str
        +files: List[str]
    }
    class ExtractorConfig{
        +folder: str
    }
    class GeneratorConfig{
        +templates_platform: str
        +created_ddls_json: str
        +folder: str
    }
    class PublisherConfig{
        +vs_project_folder: str
        +vs_project_file: str
        +codeList_json: str
        +codeList_folder: str
        +mdde_scripts_folder: str
    }
    class DevOpsConfig{
        +organisation: str
        +project: str
        +repo: str
        +branch: str
        +work_item: str
        +work_item_description: str
    }
    class ConfigData{
        +title: str
        +folder_intermediate_root: str
        +power_designer: PowerDesignerConfig
        +extractor: ExtractorConfig
        +generator: GeneratorConfig
        +publisher: PublisherConfig
        +devops: DevOpsConfig
    }

    ConfigFile -- ConfigData : has a
    ConfigData o-- PowerDesignerConfig : has
    ConfigData o-- ExtractorConfig : has
    ConfigData o-- GeneratorConfig : has
    ConfigData o-- PublisherConfig : has
    ConfigData o-- DevOpsConfig : has
```

---

## API referentie

### Configuratie lezen

::: src.genesis.config_file.ConfigFile
    options:
      heading_level: 4

---

::: src.genesis.config_file.ConfigFileError
    options:
      heading_level: 4

---

### Configuratie validatie

::: src.genesis.config_definition.ConfigData
    options:
      heading_level: 4

---

::: src.genesis.config_definition.DevOpsConfig
    options:
      heading_level: 4

---

::: src.genesis.config_definition.ExtractorConfig
    options:
      heading_level: 4

---

::: src.genesis.config_definition.GeneratorConfig
    options:
      heading_level: 4

---

::: src.genesis.config_definition.PowerDesignerConfig
    options:
      heading_level: 4

---

::: src.genesis.config_definition.PublisherConfig
    options:
      heading_level: 4
