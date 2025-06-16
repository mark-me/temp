# ETL Laadprocedures

![Deployment](images/etl.png){ align=right width="90" }

Deze documentatie beschrijft de standaard laadprocedures die worden gebruikt in onze ETL-omgeving. Het doel van deze procedures is om op een gestandaardiseerde manier data uit brontabellen te laden naar doeltabellen binnen dezelfde database en schema.

De masterprocedure bepaalt op basis van configuratie welk type laadmechanisme moet worden toegepast. Afhankelijk van het laadtype wordt een specifieke implementatie aangeroepen (full load, incrementeel, dimensioneel, enz.).

Alle procedures zijn ontworpen om:

* herbruikbaar te zijn voor verschillende entiteiten,
* controle op datatype en kolomstructuur mogelijk te maken,
* logging en foutafhandeling te ondersteunen.

Deze aanpak draagt bij aan een robuuste en onderhoudbare ETL-laag.

## Database Objecten

De ETL-laadprocedures maken gebruik van een reeks databaseobjecten die zijn opgenomen in de directory `src/deploy_mdde/db_objects`. Deze objecten zijn essentieel voor het uitvoeren, loggen en configureren van de laadprocessen.

De objecten zijn georganiseerd in drie categorieën:

* **Functions**: Kleine herbruikbare functies, zoals het bepalen van standaardwaarden of het vervangen van nulls.
* **Stored Procedures**: De kernlogica van het ETL-proces zit in de stored procedures. Deze voeren taken uit zoals data laden, logging, initialisatie van dimensies en het bijwerken van configuratiestatussen.
* **Tables**: Configuratie- en loggingstabellen die het gedrag van de laadprocedures sturen en het verloop van een run registreren.

### Overzicht van objecten

```bash
src/deploy_mdde/db_objects
├── Functions
│   ├── fn_GetDefaultValueForDatatype.sql      # Bepaalt een standaardwaarde op basis van datatype
│   └── fn_IsNull.sql                          # Alternatief voor ISNULL/COALESCE met typecontrole
├── Stored Procedures
│   ├── sp_InitializeDimension.sql             # Voegt een dummyregel toe aan een dimensionele tabel (key = -1)
│   ├── sp_InsertConfigExecution.sql           # Registreert de start van een laadproces
│   ├── sp_LoadDates.sql                       # Laadt een datumdimensie op basis van een datumbereik
│   ├── sp_LoadEntityData_DeltaLoad.sql        # Niet-actief (voorbeeld of legacy)
│   ├── sp_LoadEntityData_FullLoad.sql         # Implementeert de full load procedure
│   ├── sp_LoadEntityData_IncrementalLoad.sql  # Implementeert de incrementele laadprocedure
│   ├── sp_LoadEntityData.sql                  # Masterprocedure die bepaalt welk laadtype wordt toegepast
│   ├── sp_Logger.sql                          # Schrijft logregels naar de logtabel
│   └── sp_UpdateConfigExecution.sql           # Registreert de eindstatus van een laadproces
└── Tables
    ├── CodeList.sql                           # Optionele codetabel voor typeclassificatie of mapping
    ├── ConfigExecutions.sql                   # Houdt bij welke laadtaken zijn uitgevoerd met status/tijd
    ├── Config.sql                             # Stuurt het laadgedrag (mapping, bronnen, doel)
    ├── Dates.sql                              # Tabel voor de datumdimensie
    └── Logger.sql                             # Tabel voor logging van laadstappen, rijen en fouten
```

### Gebruik in het ETL-proces

Het ETL proces wordt gestart middels Synapse pipeline. Voor meer informatie over ETL Synapsepipeline kan je vinden op de [ETL Synapse Pipeline](MDDE_ETL_Synapse_Pipeline.md).

Tijdens een laadproces wordt de configuratie gelezen uit de `Config`-tabel. Bij de start van een run wordt een nieuwe regel toegevoegd aan `ConfigExecutions` met details over de uitvoering. De hoofdprocedure (`sp_LoadEntityData`) bepaalt op basis van deze configuratie welke specifieke laadprocedure wordt aangeroepen (full, incremental, dimensioneel). Tijdens de uitvoering wordt met `sp_Logger` gelogd wat er gebeurt en hoeveel rijen zijn verwerkt. Na afloop wordt met `sp_UpdateConfigExecution` de runstatus bijgewerkt.

De `fn_GetDefaultValueForDatatype` wordt bijvoorbeeld gebruikt bij het initialiseren van dummy-dimensieregels (key = -1), zodat alle velden zinvolle, verwachte waarden bevatten bij een onbekende of ontbrekende referentie.

## Master Load Procedure

De **master load procedure** bepaalt welk type laadmechanisme moet worden toegepast op basis van inputparameters.

### Inputparameters

* `RunId` (Azure pipeline)
* Schema-/Laagnaam
* Bronnaam
* Doelnaam
* Mappingnaam
* Laadtypenummer
* Controle kolommen en datatypes (0/1)

### Uitgangspunten

* Bron en doel bevinden zich in dezelfde database en hetzelfde schema.
* De logica zit in de brontabel/view.
* Bron en doel zijn bestaande tabellen of views.
* Verschillen in structuur en datatype worden (optioneel) gecontroleerd.

### Ondersteunde Laadtypes

| Type   | Omschrijving                     |
| ------ | -------------------------------- |
| `0`    | Entity Full Load                 |
| `1`    | Entity Incremental Load          |
| `2`    | Dimension Table Full Load        |
| `3`    | Dimension Table Incremental Load |
| `4`    | Fact Table Full Load             |
| `5`    | Fact Table Incremental Load      |
| `99`   | Disabled                         |

### Verwerking

* Controle op overeenkomende kolommen (structuur, datatypes, lengtes) indien geactiveerd.
* Bepalen van het toe te passen laadmechanisme.
* Logging van proces en resultaat.

---

## Full Load Procedure

Deze procedure laadt de volledige inhoud van een bron naar een doeltabel.

### Inputparameters

* `RunId` (Azure pipeline)
* Schema-/Laagnaam
* Bronnaam
* Doelnaam
* Mappingnaam
* Debug (`0`/`1`)

### Uitgangspunten

* Bron en doel in dezelfde database en schema.
* Identieke structuur (kolommen hoeven niet in dezelfde volgorde).
* Primary key van de doeltabel mag ontbreken in de bron.
* Structuur- en datatypecontrole gebeurt vooraf via de masterprocedure.
* Logica zit in de bron.

### Verwerking

* `TRUNCATE` van de doeltabel.
* `INSERT` van alle data vanuit de bron.
* Logging van aantal ingevoegde rijen en uitkomst van de procedure.

---

## Incrementele Procedure

De incrementele laadprocedure voert updates en inserts uit op basis van business keys.

### Inputparameters

* `RunId` (Azure pipeline)
* Schema-/Laagnaam
* Bronnaam
* Doelnaam
* Mappingnaam
* Debug (`0`/`1`)

### Uitgangspunten

* Bron en doel in dezelfde database en schema.
* Structuur van tabellen moet gelijk zijn (behalve PK).
* Controle op structuur gebeurt in de masterprocedure.
* Logica zit in de bron.

### Verwerking

* Bepalen of een business key al in de doeltabel bestaat:

  * **Niet aanwezig**: `INSERT`
  * **Wel aanwezig met andere hash**: `UPDATE` en `INSERT` nieuwe versie
* Logging van:

  * Aantal ingevoegde regels
  * Aantal bijgewerkte regels
  * Uitkomst van de procedure

---

## Dimension Table Full Load

Deze procedure is gelijk aan de [Full Load Procedure](#full-load-procedure), met als extra stap de [Initialisatie van de Dimensie](#initialisatie-van-de-dimensie).

---

## Dimension Table Incremental Load

Deze procedure is gelijk aan de [Incrementele Procedure](#incrementele-procedure), met als extra stap de [Initialisatie van de Dimensie](#initialisatie-van-de-dimensie).

> Deze stap is nodig omdat een dimensionele incremental load ook gebruikt kan worden als initiële lading.

---

## Initialisatie van de Dimensie

Bij initialisatie wordt een standaardregel toegevoegd aan de doeltabel met key `-1`.

### Waarden

* **Tekstvelden**: `'-'`
* **Numerieke velden**: `-1` (of `0` indien `-1` niet toegestaan is)
* **Datumvelden**: `'2099-12-31'`

Deze standaardwaarden worden gegenereerd via de functie `fn_GetDefaultValueForDatatype`.

