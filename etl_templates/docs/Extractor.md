# Extractor

![Extractor](images/extractor.png){ align=right width="90" }

Deze documentatie beschrijft het gebruik van het Python-package dat gegevens extraheert uit Power Designer-documenten. Power Designer wordt vaak gebruikt voor het modelleren van databases en gegevensstromen in ETL-processen.
Het doel van deze tool is om automatisch structuurinformatie en transformatiespecificaties uit Power Designer-modellen te halen en om te zetten naar een JSON-representatie. Deze JSON kan vervolgens gebruikt worden voor documentatie, kwaliteitscontrole of gegenereerde laadprogrammatuur.

De extractie ondersteunt informatie over:

* Tabellen en attributen (LDM/Logical Data Model),
* Relaties en domeinen,
* Mappings, joins en transformatie-logica.

Deze documentatie licht de componenten van de extractor toe, hoe het ge-extracte JSON-bestand is opgebouwd, en geeft visuele modellen van de interne structuur.

## Belangrijke componenten

De volgende klassen spelen een belangrijke rol in het extractieproces:

* `PDDocument`, fungeert als de hoofdinterface voor het omzetten van Power Designer LDM-bestanden in een gestructureerd, machine-leesbaar formaat dat geschikt is voor verdere verwerking in datamodellering, DDL- en ETL-generatie workflows. Het abstraheert de complexiteit van het parsen en interpreteren van de LDM XML en biedt een overzichtelijke API voor downstream-tools en -processen.
* `StereotypeExtractor` is verantwoordelijk voor het extraheren en verwerken van specifieke typen objecten (filters, aggregaten en scalars) uit een Power Designer-document dat als een dictionary is gerepresenteerd. De extractie is gebaseerd op een opgegeven stereotype. De klasse `StereotypeTransformer` verzorgt tevens het opschonen en transformeren van deze objecten en verzamelt gerelateerde domeingegevens.
* `ModelExtractor` is verantwoordelijk voor het extraheren en transformeren van relevante objecten uit een Power Designer Logical Data Model (LDM)-document. Het hoofddoel is om de inhoud van het LDM te parsen, interne en externe modellen, entiteiten, relaties, domeinen en datasources te identificeren en deze informatie voor te bereiden voor verdere verwerking, zoals ETL of lineage-analyse. De klasse maakt gebruik van twee transformatie-helpers `ModelInternalTransformer` en `ModelsExternalTransformer` om de specifieke structuren van interne en externe modellen te verwerken.
* `MappingExtractor` is verantwoordelijk voor het extraheren van ETL (Extract, Transform, Load) mapping-specificaties uit een Power Designer Logical Data Model (LDM) dat gebruikmaakt van de CrossBreeze MDDE-extensie. De klasse verwerkt de ruwe modelgegevens, filtert irrelevante mappings eruit en transformeert de geëxtraheerde informatie naar een leesbaarder en gestructureerd formaat.
    * `MappingAttributesTransformer` is verantwoordelijk voor het transformeren en verrijken van attributen-mappings, specifiek voor ETL (Extract, Transform, Load).
    * `SourceCompositionTransformer` is verantwoordelijk voor het transformeren, opschonen en verrijken van "source composition"-datastructuren die zijn geëxtraheerd uit Power Designer Logical Data Model (LDM)-documenten. Het hoofddoel is om complexe mapping- en compositiedata te verwerken en te normaliseren. Hierbij worden voorbeelddata verwijderd, relevante entiteiten, join-condities en scalar-condities geëxtraheerd en klaargemaakt voor verdere verwerking in ETL- of DDL-generatie.
    * `TargetEntityTransformer` is verantwoordelijk voor het verwerken en verrijken van mapping-data die zijn geëxtraheerd uit Power Designer-documenten. Het hoofddoel is om mapping entries te transformeren door deze te associëren met hun doeltabellen en attributen.

### Sequentie van componenten

De sequentiediagram laat zien hoe de Orchestrator gebruik maakt van de Extractor componenten om een Power Designer document te lezen en het resultaat weg te schrijven naar een JSON bestand. Hierbij is te zien hoe de flow tussen de verschillende objecten door de functies loopt.

```mermaid
sequenceDiagram
    participant Orchestrator
    participant PDDocument
    participant DomainsExtractor
    participant StereotypeExtractor
    participant ModelExtractor
    participant MappingExtractor
    participant Power Designer LDM
    participant JSON Extract

    Orchestrator->>PDDocument: extract_to_json(file_output)
    PDDocument->>PDDocument: _read_file_model()
    PDDocument->>Power Designer LDM: Lees PowerDesigner LDM
    Power Designer LDM-->>PDDocument: pd_content
    PDDocument->>PDDocument: _get_document_info(pd_content)
    PDDocument->>DomainsExtractor: get_domains()
    DomainsExtractor-->>PDDocument: domains
    PDDocument->>StereotypeExtractor: get_objects(dict_domains=domains) (filters)
    StereotypeExtractor-->>PDDocument: filters
    PDDocument->>StereotypeExtractor: get_objects(dict_domains=domains) (scalars)
    StereotypeExtractor-->>PDDocument: scalars
    PDDocument->>StereotypeExtractor: get_objects(dict_domains=domains) (aggregates)
    StereotypeExtractor-->>PDDocument: aggregates
    PDDocument->>ModelExtractor: get_models(dict_domains=domains)
    ModelExtractor-->>PDDocument: models
    PDDocument->>MappingExtractor: get_mappings(models, filters, scalars, aggregates)
    MappingExtractor-->>PDDocument: mappings
    PDDocument->>PDDocument: _write_json(file_output, dict_document)
    PDDocument->>JSON Extract: Schrijf JSON output
```

## Veelgestelde vragen (FAQ)

❓ Ondersteunt de extractor meerdere modelversies van Power Designer?

Momenteel wordt enkel getest met Power Designer 16.7. Andere versies kunnen verschillen in XML-structuur, wat foutmeldingen kan veroorzaken.

❓ Moet ik een volledig fysiek model (PDM) hebben of volstaat een logisch model (LDM)?

De extractor is ontworpen voor logische modellen (LDM). Ondersteuning voor PDM’s is beperkt en kan later worden toegevoegd.

❓ Wordt het JSON-bestand automatisch gevalideerd?

Er is nog geen JSON-schema-validatie inbegrepen. Het is aanbevolen om het bestand visueel of met scripts te controleren.

❓ Kan ik ook alleen de transformaties extraheren?

Ja, mits de mapping-informatie aanwezig is in het LDM. Er is (nog) geen ondersteuning voor losse mapping-bestanden.

## Gegevensstructuur van de JSON-output

De JSON-output is opgebouwd rond gegevensobjecten voor modellen, entiteiten, attributen, relaties, transformaties, mappings, filters en functies. De JSON bevat twee hoofdonderdelen: een lijst met modellen en een lijst met transformaties. Op hoofdlijnen kan de output als volgt worden beschreven in de vorm van een [entiteit relatie diagram](https://nl.wikipedia.org/wiki/Entity-relationshipmodel):

```mermaid
erDiagram
    Output ||--|{ Model: heeft
    Output ||--o{ Filters: heeft
    Output ||--o{ Scalars: heeft
    Output ||--O{ Aggregates: heeft
    Output ||--|{ Mappings: heeft
    Model ||--o{ Entities: heeft
    Entities ||--o{ Attributes: heeft
    Attributes ||--o{ Domain: heeft
    Entities ||--o{ Identifiers: heeft
    Identifiers ||--o{ Attribute: heeft
    Model ||--O{ Relationships: heeft
    ObjectsFilters["Objects"]
    VariablesFilters["Variables"]
    DomainFilters["Domain"]
    Filters ||--|{ ObjectsFilters: heeft
    ObjectsFilters ||--|{ VariablesFilters: heeft
    VariablesFilters ||--|| DomainFilters: heeft
    ObjectsScalar["Objects"]
    VariablesScalars["Variables"]
    DomainScalars["Domain"]
    Scalars ||--|{ ObjectsScalar: heeft
    ObjectsScalar ||--O{ VariablesScalars: heeft
    VariablesScalars ||--|| DomainScalars: heeft
    ObjectsAggregate["Objects"]
    VariablesAggregate["Variables"]
    DomainAggregate["Domain"]
    IdentifiersAggregate["Identifiers"]
    IdentifiersVariablesAggregate["Variables"]
    Aggregates ||--|{ ObjectsAggregate: heeft
    ObjectsAggregate ||--O{ VariablesAggregate: heeft
    VariablesAggregate ||--|| DomainAggregate: heeft
    ObjectsAggregate ||--O{ IdentifiersAggregate: heeft
    IdentifiersAggregate ||--O{ IdentifiersVariablesAggregate: heeft
    Mappings ||--O{ TargetEntity: heeft
    AttributesTargetEntity["Attributes"]
    DomainTargetEntity["Domain"]
    TargetEntity ||--|{ AttributesTargetEntity: heeft
    AttributesTargetEntity ||--|| DomainTargetEntity: heeft
```

In de rest van deze sectie wordt er meer detail ingevuld voor gegevensstructuur voor elk [model](#model-output) en voor elke [transformatie](#transformatie-output).

### Model output

De JSON-structuur die door de extractor wordt gegenereerd, volgt een gelaagd datamodel. Onderstaande diagram toont de belangrijkste objecten die de structuur van het logische datamodel beschrijven, zoals tabellen (entiteiten), kolommen (attributen), en domeinen (gegevenstypen). Deze structuur vormt de basis voor documentatie of verdere verwerking van het model.

``` mermaid
erDiagram
    Model {
        string Id
        string ObjectID
        string Name
        string Code
        string Rowcount
        string CreationDate
        string Creator
        string ModificationDate
        string Modifier
        string Author
        string Version
        string RepositoryFilename
        boolean IsDocumentModel
    }
    Entity {
        string Id
        string ObjectID
        string Name
        string Code
        string CreationDate
        string Creator
        string ModificationDate
        string Modifier
    }
    Attribute {
        integer Order
        string Id
        string ObjectID
        string Name
        string Code
        string DataType
        string Length
        boolean IsMandatory
    }
    Domain {
        string Id
        string Name
        string Code
        string DataType
        string Length
        string Precision
    }
    Model ||--o{ Entity: heeft
    Entity ||--o{ Attribute: heeft
    Domain ||--o{ Attribute: is-een
```

### Transformatie output

Naast de beschrijving van het datamodel bevat de JSON ook informatie over transformaties. Deze transformaties geven aan hoe gegevens uit brontabellen gecombineerd worden, welke join-condities gelden, en hoe attributen gemapt worden naar doelstructuren. Het onderstaande klassendiagram toont de betrokken objecten zoals mappings, source objects, join-condities en de rol van attributen in deze transformaties.

``` mermaid
erDiagram
    Model ||--o{ Entity : bevat
    Entity ||--o{ Attribute : heeft
    Attribute ||--o| Domain : is-een
    Transformation ||--o{ Mapping : bevat
    Mapping ||--|| Entity_Target : doelt
    Entity_Target ||--|| Entity: rol
    Mapping ||--o{ Source_Object : bronnen
    Source_Object ||--o{ Join_Condition : Koppelt
    Join_Condition ||--|| Attribute: rol
    Source_Object ||--|| Entity: rol
    Mapping ||--o{ AttributeMapping : maps
    AttributeMapping }o--o{ Attribute: rol
```

## Class diagram voor de extractor

```mermaid
classDiagram
    PDDocument *-- DomainsExtractor
    PDDocument *-- StereotypeExtractor
    PDDocument *-- ModelExtractor
    PDDocument *-- MappingExtractor

    DomainsExtractor *-- DomainsTransformer

    StereotypeExtractor *-- StereotypeTransformer

    MappingExtractor *-- TargetEntityTransformer
    MappingExtractor *-- MappingAttributesTransformer
    MappingExtractor *-- SourceCompositionTransformer

    ModelExtractor *-- ModelInternalTransformer
    ModelExtractor *-- ModelsExternalTransformer
    ModelExtractor *-- RelationshipsTransformer
```

De bovenstaande klassediagram laat de onderliggende overerving van de basisklassen `BaseExtractor` en `BaseTransformer` niet zien.

```mermaid
classDiagram

BaseExtractor <|-- BaseTransformer
ClassExtractor <|-- BaseExtractor
ClassTransformer <|-- BaseTransformer
```

## Mogelijke uitbreidingen

**Andere modeltypes**
Ondersteuning voor andere modeltypes, zoals PDM of gecombineerde modellen.

**JSON validatie**
Genereren van een JSON-schema om de structuur formeel te valideren.

**CLI functionaliteit**
Toevoegen van CLI-functionaliteit (command line interface) voor eenvoudiger gebruik zonder code.

**Versies vergelijken**
Mogelijkheid om veranderingen tussen modelversies te vergelijken.

## API referentie

### ::: src.pd_extractor.document.PDDocument

---

### ::: src.pd_extractor.domains_extractor.DomainsExtractor

---

### ::: src.pd_extractor.domains_transformer.DomainsTransformer

---

### ::: src.pd_extractor.stereotype_extractor.StereotypeExtractor

---

### ::: src.pd_extractor.stereotype_transform.StereotypeTransformer

---

### ::: src.pd_extractor.model_extractor.ModelExtractor

---

### ::: src.pd_extractor.model_internal_transform.ModelInternalTransformer

---

### ::: src.pd_extractor.models_external_transform.ModelsExternalTransformer

---

### ::: src.pd_extractor.mapping_extractor.MappingExtractor

---

### ::: src.pd_extractor.mapping_attributes_transform.MappingAttributesTransformer

---

### ::: src.pd_extractor.mapping_composition_transform.SourceCompositionTransformer

---

### ::: src.pd_extractor.mapping_target_transform.TargetEntityTransformer

---

### ::: src.pd_extractor.base_extractor.BaseExtractor

---

### ::: src.pd_extractor.base_transformer.BaseTransformer

---
