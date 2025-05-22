# Generator

![Generator](images/generator.png){ align=right width="90" }

Deze documentatie beschrijft de structuur, werking en samenhang van componenten in de Generator package in Genesis. De Generator is verantwoordelijk voor het genereren van de code die de tabellen van de modellen implementeert en genereert de code voor de views, stored procedures en data inserts t.b.v. het ETL proces.

```mermaid
flowchart LR
    JSON@{ shape: notch-rect, label: "Extractor<br>JSON output" }
    READ@{ shape: rect, label: "Lees extractor<br>output JSON" }
    JINJA@{ shape: rect, label: "Selecteer juiste<br>Jinja templates" }
    DDL@{ shape: rect, label: "Genereer DDLs" }
    ETL@{ shape: rect, label: "Genereer ETL<br>componenten" }
    JSON --> READ --> JINJA --> DDL --> ETL
```

## Belangrijke componenten

* **`DDLGenerator`** fungeert als een centrale component voor de vertaling van JSON-modeldata naar database-artefacten, met name voor ETL- en DDL-processen. Het maakt gebruik van templates om platform-specifieke SQL-scripts te genereren en zorgt ervoor dat gegenereerde artefacten worden gedocumenteerd voor latere verwerking in DevOps-pijplijnen.
    * Input:
        * Neemt een dictionary van parameters aan, zoals paden, template-informatie en uitvoerlocaties.
        * Initieert de interne status, waaronder een uitvoer-volgregistratie (`dict_created_ddls`).
    * Output:
        * De gegenereerde DDL- en ETL-bestanden worden georganiseerd in submappen per entiteit, view of post-deployment script.
        * Een JSON-bestand (`generated_ddls.json`) biedt een overzicht van alle gegenereerde bestanden, gecategoriseerd per type.
* **`CodeList`** is verantwoordelijk voor het lezen, transformeren en exporteren van codelijstdata. De focus ligt op het standaardiseren van data uit verschillende bronsystemen ("DMS" en "AGS") en het opslaan van de verzamelde data in een JSON-formaat voor verdere verwerking door andere componenten in het systeem.
* **`DDLPublisher`** is verantwoordelijk voor het programmatisch bijwerken van een Visual Studio SQL-projectbestand (`.sqlproj`) door nieuwe SQL-bestanden, mappen en post-deployment scripts toe te voegen die elders in het systeem zijn gegenereerd. De centrale klasse, `DDLPublisher`, leest een JSON-bestand dat de gegenereerde DDL-bestanden beschrijft en werkt het projectbestand bij om ervoor te zorgen dat het project synchroon blijft met de gegenereerde artefacten. De ouput is:
    * JSON Outputbestand (`generated_ddls.json`):
        * Dit bestand bevat een overzicht van alle gegenereerde DDL-bestanden, gestructureerd per map of type.
        * Wordt gebruikt als input voor de `publish()`-methode om nieuwe bestanden en mappen te identificeren.
    * SQL Projectbestand (`.sqlproj`):
        * De `.sqlproj`-structuur wordt bijgewerkt met nieuwe bestanden en post-deployment scripts.
        * Problematische XML-elementen, zoals dubbele `<VisualStudioVersion>`-elementen, worden verwijderd om laadfouten in Visual Studio te voorkomen.

## Afhankelijkheden

* **[`Jinja2`](https://jinja.palletsprojects.com/en/stable/):** Voor het renderen van SQL-templates.
* **([`Polars`](https://pola.rs/))** Voor het inlezen van de Excel bestanden met codelijsten (in `CodeList`).
* **[`sqlparse`/`sqlfluff`](https://sqlfluff.com/):** Voor het formatteren en linten van gegenereerde SQL-scripts.
* **[`Pathlib`](https://docs.python.org/3/library/pathlib.html):** Voor platformonafhankelijke padbeheer.

## Genereren DDL en ETL

De Generator, genereert DDL en Post-Deploy bestanden en kopieert deze ook de ETL die nodig is om de data te kunnen verwerken. De doel architectuur / platform wordt bepaald aan de hand van een parameter.
De generator gaat neemt aan dat er een deployment (Git) repository op een lokale schijf aanwezig is, zodat de gegenereerde bestanden hier kunnen worden weggeschreven. Deze locatie is opgenomen in de ```config.yml``` onder de tag: ```vs_project_folder```.
De Git interactie gebeurt in de module ```devops.py``` en zal hier niet verder worden behandeld.

Omdat de ELT en DDL per platform / doel architectuur afwijkt, is deze in een sub folder geplaatst met de naam van het platform. Tijdens onze eerste uitrol kennen wij alleen nog maar de [Synapse Dedicated Pool](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-overview-what-is) als platform, vandaar dat er nog geen andere platformen zijn uitgewerkt.

### ETL Templates

De ETL scripts maken deel uit van de Genesis generator en zijn opgeslagen in de folder: ``` ./etl_templates/src/generator/mdde_scripts/ {PLATFORM} / {** OPTIONEEL SCHEMA} ```.
Voor de Synapse Dedicated Pool zijn hier de ```Procedures``` , ```Functies``` en ```Tabellen``` opgeslagen.

### DDL MDDE PostDeploy

De Post-Deploy script worden gemaakt, om stam- en config-tabellen te vullen, na een release.
In de huidige opzet is voor de ```DataCenter``` Dedicated pool een Visual Studio Solution aangemaakt met daarin een SQL Project per laag. Per SQL project is er maximaal één Post-Deploy bestand aanwezig waarin code of referenties gezet kunnen worden die na de release worden uitgevoerd.
Voor de Centrale laag is dit het project ```3. Central Layer``` en het Post-Deploy bestand is te vinden in de folder:  ```./CentralLayer/PostDeployment/PostDeploy.sql```
Het script moet als basis de volgende code bevatten:

```
PRINT N'Running PostDeploy:'
PRINT N'Running PostDeploy: ..\DA_MDDE\PostDeployment\PostDeploy_Dates.sql'
:r ..\DA_MDDE\PostDeployment\PostDeploy_Dates.sql
PRINT N'Running PostDeploy: ..\DA_Central\PostDeployment\PostDeploy_MetaData_Calendar.sql'
:r ..\DA_MDDE\PostDeployment\PostDeploy_MetaData_Calendar.sql
PRINT N'Running PostDeploy: ..\DA_MDDE\PostDeployment\PostDeploy_MetaData_Config_CodeList.sql'
:r ..\DA_MDDE\PostDeployment\PostDeploy_MetaData_Config_CodeList.sql
```

### DDL MDDE PostDeploy Config

Per model dat door de generator heen gaat, zal er een PostDeploy bestand gemaakt worden die zorgt dat de config tabel voorzien kan worden met de mappingen die geladen moeten worden.
Deze functie ```write_dll()``` roept een private functie aan ```__write_ddl_MDDE_PostDeploy_Config()``` die van het aangeboden model de PostDeploy genereerd en klaarzet in de PostDeploy folder in de MDDE folder.
De opbouw ziet zo uit:
```. CentralLayer\DA_MDDE\PostDeployment\PostDeploy_MetaData_Config_{ MODEL CODE }.sql```
Naast dat het bestand aangemaakt is, zal er een referentie moeten worden opgenomen in het master PostDeploy script.
Deze zal voorzien worden van 2 extra regels:

```
PRINT N'Running PostDeploy: ..\DA_MDDE\PostDeployment\PostDeploy_MetaData_Config_{ MODEL CODE }.sql
:r ..\DA_MDDE\PostDeployment\PostDeploy_MetaData_Config_{ MODEL CODE }.sql.sql
```

De PRINT zorgt ervoor dat er in de Azure Replease Pipeline zichtbaar is welke scripts er uitgevoerd wordt.

### ONDERSTAANDE NOG DOORLOPEN

Toont de relaties tussen de verschillende entiteiten zoals Filters, Scalers, Mappings en Target Entities.

```mermaid
erDiagram
Mapping{
        ObjectID    string
        Id  string
        Name string
        Code string
        CodeModel string
        string  ObjectID  "*"
        string Id  "*"
        string Name  "*"
        string Code  "*"
        string CodeModel "*"
        }
Filter{
        string ObjectID   "*"
        string Id  "*"
        string Name  "*"
        string Code  "*"
        string CodeModel "*"
        string SqlExpression "*"
    }
Filter_Attribute{
        string ObjectID   "*"
        string Id  "*"
        string Name  "*"
        string Code  "*"
        string DataType "*"
        int Length
        int Precision
    }
Scaler{
        string ObjectID "*"
        string Id  "*"
        string Name  "*"
        string Code  "*"
        string CodeModel "*"
        string SqlExpression "*"
    }
Scaler_Attribute{
        string ObjectID   "*"
        string Id  "*"
        string Name  "*"
        string Code  "*"
        string DataType "*"
        int Length
        int Precision
    }

TARGET_ENTITY{
        ObjectID    string
        Id  string
        Name string
        Code string
        CodeModel string
    }
Identifier{
        ObjectID    string
        Id  string
        Name string
        Code string
        NameAttribute string
        CodeAttribute string
        IsPrimary boolean
}

    Transformations ||--o{ Filter: has
    Transformations ||--o{ Scaler: has
    Transformations ||--o{ Mapping: has

Domain{
        string ObjectID   "*"
        string Id  "*"
        string Name  "*"
        string Code  "*"
        string DataType "*"
        int Length
        int Precision
}
    Filter ||--||  Filter_Attribute :has
    Filter_Attribute ||--o|  Domain:has
    Scaler||--|{ Scaler_Attribute:has
    Scaler_Attribute ||--o|  Domain:has
    Mapping

    Mapping ||--|{ SOURCE_COMPOSITION : has
    Mapping ||--|{ TARGET_ENTITY :has
    Mapping ||--|{ ATTRIBUTE_MAPPING:has
    TARGET_ENTITY ||--|{ Identifier:has
    SOURCE_ENTITY ||--o{ JoinCondition:has
    SOURCE_ENTITY ||--o{ SoureCondition :has
    SoureCondition ||--|| CONDITION_EXPRESSION :has
    SoureCondition ||--|| ATTRIBUTE:has
    SOURCE_COMPOSITION ||--|| SOURCE_ENTITY:has
    ATTRIBUTE_MAPPING ||--|| TARGET_ATTRIBUTE :has
    ATTRIBUTE_MAPPING ||--o| SOURCE_ATTRIBUTE:has
    JoinCondition ||--|| ATTRIBUTE_CHILD:has
    JoinCondition ||--o| ATTRIBUTE_PARENT:has
    SOURCE_COMPOSITION ||--o{ GROUPING:has
    SOURCE_COMPOSITION ||--o{ SORTING:has
    SOURCE_COMPOSITION ||--o{ RESULT_CONDITION:has
```

## ERDiagram incl. notes

```mermaid
classDiagram
    class Transformations:::Extractors
    class Mapping:::Extractors
    class Filter:::Extractors
    class Scaler:::Extractors
    class SOURCE_COMPOSITION:::Transforms
    class SOURCE_ENTITY:::TransformsSub
    class JoinCondition:::TransformsSub
    class ATTRIBUTE_CHILD:::TransformsSub
    class ATTRIBUTE_PARENT:::TransformsSub
    class SourceCondition:::TransformsSub
    class CONDITION_EXPRESSION:::TransformsSub
    class ATTRIBUTE:::TransformsSub
    class GROUPING:::TransformsSub
    class SORTING:::TransformsSub
    class RESULT_CONDITION:::TransformsSub
    class TARGET_ENTITY:::Transforms
    class Identifier:::TransformsSub
    class ATTRIBUTE_MAPPING:::Transforms
    class TARGET_ATTRIBUTE:::TransformsSub
    class SOURCE_ATTRIBUTE:::TransformsSub
    classDef Extractors fill:#99fb77, color:black
    classDef Transforms fill:#28cdfd, color:black
    classDef TransformsSub fill:#a3e8fc, color:black
Transformations : current ObjectExtractor
Transformations : function mappings
Mapping : current TransformMappings
Mapping : function mappings
TARGET_ENTITY : current TransformModelInternal
TARGET_ENTITY : function entities (attributes + identifier)
TARGET_ENTITY : function __entity_attributes (attributes)
Identifier : current TransformModelInternal
Identifier : function __entity_identifiers
SOURCE_ENTITY : current TransformMappings
SOURCE_ENTITY : function __mapping_entities_source
ATTRIBUTE_MAPPING : current TransformMappings
ATTRIBUTE_MAPPING : function __mapping_attributes
TARGET_ATTRIBUTE : current TransformMappings
TARGET_ATTRIBUTE : function __mapping_attributes
SOURCE_ATTRIBUTE : current TransformMappings
SOURCE_ATTRIBUTE : function __mapping_attributes
SOURCE_COMPOSITION : current TransformMappings
SOURCE_COMPOSITION : function Mapping_compositions
SOURCE_COMPOSITION : function __compositions
JoinCondition : current TransformMappings
JoinCondition : function __composition_join_conditions
JoinCondition : function __join_condition_components
ATTRIBUTE_CHILD : current TransformMappings
ATTRIBUTE_CHILD : function __join_condition_components
ATTRIBUTE_PARENT : current TransformMappings
ATTRIBUTE_PARENT : function __join_condition_componentst
SourceCondition : current TransformMappings
SourceCondition : function __composition_apply_conditions
CONDITION_EXPRESSION : current TransformMappings
CONDITION_EXPRESSION : function __composition_apply_conditions
ATTRIBUTE : current TransformMappings
ATTRIBUTE : function __composition_apply_conditions
Filter : current PDDocument
Filter : function __all_filters

Transformations ..> Filter
Transformations ..> Scaler
Transformations ..> Mapping
Mapping ..> SOURCE_COMPOSITION
Mapping ..> TARGET_ENTITY
TARGET_ENTITY ..> Identifier
Mapping ..> ATTRIBUTE_MAPPING
SOURCE_COMPOSITION ..> SOURCE_ENTITY
SOURCE_ENTITY ..> JoinCondition
SOURCE_ENTITY ..> SourceCondition
SourceCondition ..> CONDITION_EXPRESSION
SourceCondition ..> ATTRIBUTE
SOURCE_COMPOSITION ..> GROUPING
SOURCE_COMPOSITION ..> SORTING
SOURCE_COMPOSITION ..> RESULT_CONDITION
ATTRIBUTE_MAPPING ..> TARGET_ATTRIBUTE
ATTRIBUTE_MAPPING ..> SOURCE_ATTRIBUTE
JoinCondition  ..> ATTRIBUTE_CHILD
JoinCondition ..> ATTRIBUTE_PARENT
```

## Vertaling ERDiagram naar Classes

### FlowDiagram

```mermaid
flowchart TD
PDdocument --> ModelExtractor
ModelExtractor --> FilterExtractor
FilterExtractor --> ScalerExtractor
ScalerExtractor --> MappingExtractor
MappingExtractor --> PDdocument

```

## API referentie

### ::: src.generator.generator.DDLGenerator

---

### ::: src.generator.publisher.DDLPublisher

---

### ::: src.generator.devops.DevOpsHandler

---

### ::: src.generator.codelists.CodeList
