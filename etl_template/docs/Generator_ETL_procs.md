# ETL laad procedures

## Master load procedure

Full load procedure met de [Naam] heeft de volgende input parameters.

* RunId Azure pipeline
* Schema- /Laag naam
* Bronnaam
* Doelnaam
* Mapping-naam
* Laadtypenummer
* Controle Kolommen en Datatypes (0/1)

Uitgangspunten:

* Bron en doel zitten in zelfde database.
* Bron en doel zitten in zelfde schema.
* Logica zit in de bron.
* Bron en doel zijn bestaande tabellen/view in de database.
* Bepalen welke type laadmechanisme moet worden toegepast bij het laden van een tabel
  * De verschillende laadtypes zijn als volgt:
    * 0 = Entity Full Load
      1 = Entity Incremental Load
      2 = Dimension Table Full Load
      3 = Dimension Table Incremental Load
      4 = Fact Table Full Load
      5 = Fact Table Incremental Load
      99 = Disabled
* Verschil in structuur en datatype worden gecontroleerd

Stored procedure:

* Check of de kolommen van de brontabel en doeltabel dezelfde datatypes en lengtes hebben. Indien aangezet.
* Bepalen welke laadmechanisme moet worden toegepast om tabellen te laden.
* Logging

## Full load procedure

Full load procedure met de [Naam] heeft de volgende input parameters.

* RunId Azure pipeline
* Schema- /Laag naam
* Bronnaam
* Doelnaam
* Mapping-naam
* Debug

Uitgangspunten:

* Bron en doel zitten in zelfde database.
* Bron en doel zitten in zelfde schema.
* Bron en doel moeten zelfde structuur hebben (-primary key)
  * Kolommen moeten in beide zitten, maar hoeven niet in zelfde volgorde.
  * Primary key van doeltabel mag niet opgenomen zijn in de bron.
  * Verschil in structuur en datatype wordt al gecontroleerd in de master procedure.
* Logica zit in de bron.
* Bron en doel zijn bestaande tabellen/view in de database.

Stored procedure:

* Truncate van tabel.
* Insert alle data van de bron in het doeltabel.
* Loggen van de inserted rows.
* Loggen van de uitkomst van de procedure.

## Incrementeel procedure

Incrementeel laden procedure met de [Naam] heeft de volgende input parameters.

* RunId Azure pipeline
* Schema- /Laag naam
* Bronnaam
* Doelnaam
* Mapping-naam
* Debug

Uitgangspunten:

* Bron en doel zitten in zelfde database.
* Bron en doel zitten in zelfde schema.
* Bron en doel moeten zelfde structuur hebben (-primary key)
  * Kolommen moeten in beide zitten, maar hoeven niet in zelfde volgorde.
  * Primary key van doeltabel mag niet opgenomen zijn in de bron.
  * Verschil in structuur en datatype wordt al gecontroleerd in de master procedure.
* Logica zit in de bron.
* Bron en doel zijn bestaande tabellen/view in de database.

Stored procedure:

* Tabel wordt bijgeladen.
* Check of business-key bestaat in het doeltabel.
  * Insert nieuwe regels van de bron in het doeltabel.
* Check of business-key bestaat in het doeltabel, maar de hash-key verschillend is.
  * Updaten van bestaande regels.
  * Inserten van bijgewerkte regels in het doeltabel.
* Loggen van de inserted regels.
* Loggen van bijgewerkte regels.
* Loggen van de uitkomst van de procedure.

## Dimension Table Full Load

Deze load procedure is gelijk aan die van de [Full load procedure](#full-load-procedure),
maar deze heeft een extra stap om de  dummy record toe te voegen [Initialisatie van de  Dimensie](#initialisatie-van-de-dimensie).

## Dimension Table Incremental Load

Deze load procedure is gelijk aan die van de [Incrementeel procedure](#incrementeel-procedure),
maar deze heeft een extra stap om de  dummy record toe te voegen [Initialisatie van de  Dimensie](#initialisatie-van-de-dimensie).
Deze stap zit ook in de incremental Load, omdat de incremental ook ingesteld kan worden als initieel laden van een tabel en dus moet hier ook een dummy regel gemaakt worden.

## Initialisatie van de  Dimensie

Bij deze stap maken maken wij een nieuw record aan in de tabel met Key = -1. Alle tekst velden worden voorzien van een "-" en numerieke velden worden voorzien van een -1 of een 0 indien -1 niet past en Datum velden als '2099-12-31'.
De default waarden worden bepaald door de functie fn_GetDefaultValueForDatatype.
