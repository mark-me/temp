# Mapping afhankelijkheden

![Dependency checker](images/dependencies.png){ align=right width="90" }

Dit project analyseert afhankelijkheden in een ETL-proces (Extract, Transform, Load) dat is gedefinieerd door RETW-bestanden. Het helpt bij het bepalen van de optimale uitvoeringsvolgorde van mappings, identificeert potentiële fouten en visualiseert de afhankelijkheden tussen entiteiten en bestanden. De kernfunctionaliteit draait om het bouwen van een [gerichte acyclische graaf (DAG)](https://nl.wikipedia.org/wiki/Gerichte_acyclische_graaf){target="_blank"} die de ETL-flow voorstelt.

Het dekt globaal het volgende:

* wat de [volgorde van mappings](#bepalen-van-mapping-volgorde) zou moeten zijn in de ETL-flow, en of de ETL-flow geen gesloten lussen bevat (ETL-flows moeten acyclisch zijn),
* het vinden van verwijzingen naar entiteiten die niet gedefinieerd zijn in de verwerkte RETW-bestanden,
* de gevolgen van een fout in een stap van het ETL-proces en
* de afhankelijkheden tussen RETW-bestanden voor entiteiten.

## 🚀 Gebruik

De map ```dependency_checker``` bevat een bestand ```example.py``` dat laat zien hoe alle klassen gebruikt kunnen worden voor bovenstaande doeleinden.

Het voorbeeld verwijst naar een lijst van voorbeeld-RETW-bestanden die geplaatst zijn in de submap ```retw_examples```. De volgorde van de bestanden in de lijst is niet relevant voor de functionaliteit, dus je kunt eigen bestanden in willekeurige volgorde aan de lijst toevoegen.

<details><summary>Voorbeeld code</summary>

```python title="etl_templates\src\dependencies_checker\example.py"
import json

from dependencies_checker import DagReporting, EntityRef, EtlFailure

if __name__ == "__main__":
    """Examples of the class use-cases
    """
    dir_output = "etl_templates/output/etl_report/"
    dir_RETW = "etl_templates/src/dependencies_checker/retw_examples/"
    # List of RETW files to process, order of the list items is irrelevant
    lst_files_RETW = [
        "Usecase_Aangifte_Behandeling.json",
        "Usecase_Test_BOK.json",
        "DMS_LDM_AZURE_SL.json",
    ]
    lst_files_RETW = [dir_RETW + file for file in lst_files_RETW]
    dag = DagReporting()
    dag.add_RETW_files(files_RETW=lst_files_RETW)

    """File dependencies
    * Visualizes of the total network of files, entities and mappings
    * Visualizes of a given entity's network of connected files, entities and mappings
    * Visualizes dependencies between files, based on entities they have in common
    * Lists entities which are used in mappings, but are not defined in a Power Designer document
    """
    # Visualization of the total network of files, entities and mappings
    dag.plot_graph_total(file_html=f"{dir_output}all.html")
    # Visualization of a given entity's network of connected files, entities and mappings
    entity = EntityRef("Da_Central_CL", "AggrProcedureCategory")
    dag.plot_entity_journey(
        entity=entity,
        file_html=f"{dir_output}entity_journey.html",
    )
    # Visualization of dependencies between files, based on entities they have in common
    dag.plot_file_dependencies(
        file_html=f"{dir_output}file_dependencies.html", include_entities=True
    )
    # Entities which are used in mappings, but are not defined in a Power Designer document
    lst_entities = dag.get_entities_without_definition()
    with open(f"{dir_output}entities_not_defined.jsonl", "w", encoding="utf-8") as file:
        for item in lst_entities:
            file.write(json.dumps(item) + "\n")

    """ETL Flow (DAG)
    * Determine the ordering of the mappings in an ETL flow
    * Visualizes the ETL flow for all RETW files combined
    """
    # Determine the ordering of the mappings in an ETL flow: a list of mapping dictionaries with their RunLevel and RunLevelStage
    lst_mapping_order = dag.get_mapping_order()
    with open(f"{dir_output}mapping_order.jsonl", "w", encoding="utf-8") as file:
        for item in lst_mapping_order:
            file.write(json.dumps(item) + "\n")
    # Visualization of the ETL flow for all RETW files combined
    dag.plot_etl_dag(file_html=f"{dir_output}ETL_flow.html")

    """Failure simulation
    * Sets a failed object status
    * Visualization of the total network of files, entities and mappings
    """
    lst_entities_failed = [
        EntityRef("Da_Central_CL", "AggrLastStatus"),
        EntityRef("Da_Central_BOK", "AggrLastStatus"),
    ]  # Set for other examples
    etl_simulator = EtlFailure()
    # Adding RETW files to generate complete ETL DAG
    etl_simulator.add_RETW_files(files_RETW=lst_files_RETW)
    # Set failed node
    etl_simulator.set_entities_failed(lst_entities_failed)
    # Create fallout report file
    lst_mapping_order = etl_simulator.get_report_fallout()
    with open(f"{dir_output}dag_run_fallout.json", "w", encoding="utf-8") as file:
        json.dump(lst_mapping_order, file, indent=4)
    # Create fallout visualization
    etl_simulator.plot_etl_fallout(file_html=f"{dir_output}dag_run_report.html")
```
</details>

## Belangrijke componenten

Er wordt gebruik gemaakt van de klasse ```DagReporting```, gedefinieerd in het bestand ```dag_reporting.py```, om visualisaties te maken van:

* het totale netwerk van bestanden, entiteiten en mappings,
* het netwerk van verbonden bestanden, entiteiten en mappings van een specifieke entiteit,
* afhankelijkheden tussen bestanden, gebaseerd op gedeelde entiteiten, en
* een visualisatie van de ETL-flow voor alle gecombineerde RETW-bestanden.

Daarnaast worden bestanden gegenereerd met:

* de volgorde van mappings in een ETL-flow en
* de entiteiten die gebruikt worden in mappings, maar geen definitie hebben in één van de bestanden.

De klasse ```EtlFailure```, gedefinieerd in het bestand ```dag_etl_failure.py```, wordt gebruikt om:

* een visualisatie te maken van de gevolgen van een falend ETL-flow object en
* een rapportage te maken van de falende ETL-flow objecten.

## Implementatiedocumentatie

### Bepalen van mapping volgorde

De uitvoeringsvolgorde van mappings wordt bepaald door twee componenten:

* Run level: waar in de Directed Acyclic Graph ([DAG](https://nl.wikipedia.org/wiki/Gerichte_acyclische_graaf){target="_blank"}) hiërarchie, gaande van bron-entiteiten naar eind-entiteiten, de mapping zich bevindt. Mappings die enkel bron-entiteiten gebruiken krijgen run level 0, de volgende run levels worden bepaald door het aantal mappings dat in de hiërarchie vóór de huidige mapping komt.
* Run level stage: Als mappings op hetzelfde run level dezelfde entiteiten gebruiken, moeten ze een verschillende uitvoeringsvolgorde krijgen om deadlocks te voorkomen. Een [greedy coloring algoritme](https://www.youtube.com/watch?v=vGjsi8NIpSE){target="_blank"} wordt gebruikt om de uitvoeringsvolgorde binnen een run level te bepalen.

```mermaid
graph LR
  idEa[(Entity a)]
  idEb[(Entity b)]
  idEc[(Entity c)]
  idEd[(Entity d)]
  idEe[(Entity e)]
  idEf[(Entity f)]
  idEg[(Entity g)]
  idEh[(Entity h)]

  subgraph RunLevel: 0
    subgraph RunLevelStage: 0
      idMa{{Mapping a}}
    end
    subgraph RunLevelStage: 1
      idMb{{Mapping b}}
      idMc{{Mapping c}}
    end
  end
  subgraph RunLevel: 1
    subgraph RunLevelStage: 0
      idMd{{Mapping d}}
    end
  end

  idEa --> idMa
  idEb --> idMa
  idMa --> idEe
  idEb --> idMb
  idEc --> idMb
  idMb --> idEf
  idEd --> idMc
  idMc --> idEg
  idEf --> idMd
  idEg --> idMd
  idMd --> idEh
```

### Bouwen van de ETL DAG

De mapping dependency parser gebruikt [grafen](https://nl.wikipedia.org/wiki/Graaf_(wiskunde)){target="_blank"}, meer specifiek een [DAG](https://nl.wikipedia.org/wiki/Gerichte_acyclische_graaf){target="_blank"}, wat een netwerkvoorstelling is van de bestanden, entiteiten (bijv. tabellen) en mappings. Deze sectie legt uit hoe de DAG gecreëerd wordt.

Voor elk RETW-bestand worden de mappings geëxtraheerd, en de mappings, bron- en doel-entiteiten worden omgezet naar knopen (ook wel vertices genoemd). Vervolgens worden er verbindingen (ook wel edges genoemd) gelegd tussen de bron-entiteiten en de mappings en tussen de mappings en de doel-entiteiten. Als alle mappings zijn omgezet in knopen en verbindingen, kunnen deze gecombineerd worden tot een netwerk. Deze netwerkvoorstelling maakt de berekeningen mogelijk die in de introductie zijn beschreven.

```mermaid
erDiagram
ENTITY ||--|{ MAPPING: "Gevuld door: ENTITY_TARGET"
MAPPING ||--|{ ENTITY: "Gebruikt: ENTITY_SOURCE"
FILE_RETW ||--o{ MAPPING: "Bevat: FILE_MAPPING"
FILE_RETW ||--o{ ENTITY: "Bevat: FILE_ENTITY"
```

In een Power Designer-document (en het corresponderende RETW-bestand) worden alle objecten geïdentificeerd door hun 'Id'-attribuut, dat er bijvoorbeeld uitziet als 'o123'. Deze Id is intern geldig binnen een document, maar niet geschikt om objecten te identificeren wanneer we de resultaten van meerdere Power Designer-documenten combineren. Daarom moeten er nieuwe identifiers aangemaakt worden zodat er geen conflicten ontstaan tussen documenten, en tegelijkertijd de integriteit behouden blijft (bijvoorbeeld als een doel-entiteit van het ene document een bron is in een mapping van een ander document). Hoe wordt dit bereikt?

* We gaan ervan uit dat mappings uniek zijn tussen Power Designer-documenten. Om een unieke mapping-ID te maken, wordt een hash toegepast op de combinatie van de RETW-bestandsnaam en de mapping-code.

* Voor consistente identificatie van entiteiten over documenten heen, wordt een hash toegepast op de combinatie van de Code- en CodeModel-eigenschappen van een entiteit.

### Belangrijke componenten

* **```DagGenerator```**: Deze klasse vormt de basis van het project. Het ontleed RETW-bestanden, extraheert entiteiten en mappings, en bouwt de DAG. Belangrijke methoden zijn ```add_RETW_file``` (voegt een RETW-bestand toe), ```get_dag_total``` (geeft de totale DAG terug), ```get_dag_ETL``` (geeft de ETL-flow DAG terug), en andere methoden om specifieke sub-grafen op te halen.

* **```DagReporting```**: Deze klasse gebruikt de DAG van ```DagGenerator``` om inzichten en visualisaties te leveren. Methoden zijn onder andere ```get_mapping_order``` (bepaalt de uitvoeringsvolgorde), ```plot_graph_total``` (visualiseert de totale DAG), ```plot_etl_dag``` (visualiseert de ETL-flow), en andere methoden om afhankelijkheden en relaties weer te geven.

* **```EtlFailure```**: Deze klasse simuleert en analyseert de impact van falende ETL-jobs. De methode ```set_entities_failed``` specificeert de falende componenten, en ```get_report_fallout``` en ```plot_etl_fallout``` leveren rapportages en visualisaties van de gevolgen.

* **```EntityRef```** en **```MappingRef```**: Deze namedtuples representeren respectievelijk entiteiten en mappings, en geven een gestructureerde manier om ze in de DAG te refereren.

* **```VertexType```** en **```EdgeType```**: Deze enums definiëren de typen knopen en verbindingen in de DAG, wat bijdraagt aan duidelijkheid en onderhoudbaarheid van de code.

Het project gebruikt een graaf-gebaseerde aanpak om ETL-afhankelijkheden te representeren en analyseren, en biedt waardevolle inzichten voor het begrijpen en optimaliseren van het ETL-proces. ```DagGenerator``` bouwt de DAG, ```DagReporting``` verzorgt analyse en visualisatie, en ```EtlFailure``` simuleert foutscenario's.

### Klassen diagram

In deze sectie worden de klassen beschreven, waarvoor ze gebruikt worden en hoe ze samenhangen.

```mermaid
---
  config:
    class:
      hideEmptyMembersBox: true
---
classDiagram
  DagGenerator <|-- DagReporting
  DagReporting <|-- EtlFailure
  DagGenerator *-- EdgeType
  DagGenerator *-- VertexType
  EntityRef --> DagGenerator
  MappingRef --> DagGenerator

  class EntityRef{
    <<namedtuple>>
    CodeModel
    CodeEntity
  }
  class MappingRef{
    <<namedtuple>>
    FileRETW
    CodeMapping
  }
  class VertexType{
    <<enumeration>>
    ENTITY
    MAPPING
    FILE
    ERROR
  }
  class EdgeType{
    <<enumeration>>
    FILE_ENTITY
    FILE_MAPPING
    ENTITY_SOURCE
    ENTITY_TARGET
  }
  class DagGenerator{
    +add_RETW_files(list files_RETW)
    +add_RETW_file(str file_RETW)
    +get_dag_total()
    +get_dag_single_retw_file(str file_RETW)
    +get_dag_file_dependencies(bool include_entities)
    +get_dag_entity(EntityRef entity)
    +get_dag_ETL()
  }
  class DagReporting{
    +get_mapping_order() list
    +plot_graph_total(str file_html)
    +plot_graph_retw_file(str file_retw, str file_html)
    +plot_file_dependencies(str file_html, bool include_entities)
    +plot_entity_journey(EntityRef entity, str file_html)
    +plot_etl_dag(str file_html)
  }
  class EtlFailure{
    +set_pd_objects_failed(list)
    +get_report_fallout() list
    +plot_etl_fallout(str file_html)
  }
```

## Veelgestelde vragen (FAQ)

**❓ Wat als een mapping zowel bron- als doeltabellen in verschillende bestanden gebruikt?**

De parser ondersteunt dit scenario zolang de gebruikte entiteiten correct geïdentificeerd en gehasht kunnen worden. Entiteiten worden over bestanden heen herkend op basis van hun `CodeModel` + `Code`.

**❓ Kan het netwerk visualiseren in andere formaten dan HTML?**

Momenteel is HTML de standaarduitvoer. Uitbreiding naar PDF of interactieve tools zoals Plotly kan overwogen worden in een toekomstige versie.

## Mogelijke uitbreidingen

**CLI-interface**
Een eenvoudige command line interface voor het aanroepen van de dependency checker zonder Python-code.

**Live monitoring integratie**
Koppeling met live logging om visuele representatie te koppelen aan een echte ETL-run.

**Modelvergelijking**
Functionaliteit om afhankelijkheden of de DAG te vergelijken tussen twee versies van een model (bijv. veranderingen doorvoeren detecteren).

**Annotaties in visualisaties**
Mogelijkheid om extra metadata of uitleg toe te voegen aan knopen en verbindingen (zoals bronbestanden, businesslogica, etc.).

**Configuratie via YAML/JSON**
Voor projecten waarin meerdere configuratiebestanden of mapping-regels gelden, zou een YAML-config mogelijk zijn voor gebruiksgemak.

## API referentie

### ::: src.dependencies_checker.dag_generator.DagGenerator

---

### ::: src.dependencies_checker.dag_generator.EdgeType

### ::: src.dependencies_checker.dag_generator.VertexType

---

#### ::: src.dependencies_checker.dag_reporting.DagReporting

---

#### ::: src.dependencies_checker.dag_etl_failure.EtlFailure
