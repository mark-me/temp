Natuurlijk! Hieronder vind je de verbeterde versie van je Markdown-pagina **zonder de inhoudsopgave** en met de **ondersteunde laadtypes in een tabel**.

---

# ETL Laadprocedures

![Deployment](images/etl.png){ align=right width="90" }

Deze pagina beschrijft de verschillende procedures voor het laden van gegevens binnen het ETL-proces.

---

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
* Debug (0/1)

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
* Debug (0/1)

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

