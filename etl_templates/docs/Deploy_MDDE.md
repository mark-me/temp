# Deployment MDDE

![Deployment](images/deployment.png){ align=right width="90" }

Deze documentatie beschrijft het gebruik van het Python-package dat de uitrol van een model-gedreven data engineering (MDDE) omgeving binnen een ETL-systeem (Extract, Transform, Load) ondersteunt. Het primaire doel is het automatiseren van de generatie en organisatie van post-deployment SQL-scripts, die worden gebruikt om databaseobjecten te configureren en te vullen na de uitrol. Het bestand maakt gebruik van Jinja2-templating voor scriptgeneratie, beheert bestandsoperaties voor uitvoer en onderhoudt een hoofdscript om de uitvoering van alle gegenereerde scripts te orkestreren.

Voor meer informatie over de laadprocedures in de MDDE deployment kan je vinden op de [ETL laadprocedures pagina](MDDE_ETL_procs.md).

---

## Belangrijke Componenten

**TemplateType (Enum):**

* Benoemt de typen beschikbare post-deployment templates (bijv. configuratie- en codelist-scripts).
* Wordt gebruikt om het juiste template te selecteren voor de scriptgeneratie.

**DeploymentMDDE (Klasse):**

* De kernklasse die verantwoordelijk is voor het coördineren van het uitrolproces.
* **Initialisatie**: Slaat paden op voor data, schema en uitvoer, en houdt gegenereerde scripts bij.
* **process()**: Hoofdmethode die alle benodigde post-deployment scripts genereert, databaseobjecten kopieert en het hoofdscript bijwerkt.
* **\_copy\_db\_objects()**: Kopieert statische databaseobjectbestanden van een bronmap naar de doelmap.
* **\_get\_template()**: Laadt het juiste Jinja2-template op basis van het scripttype.
* **\_generate\_load\_config()**: Genereert een post-deployment script voor configuratie/mappingvolgorde via een template.
* **\_generate\_load\_code\_list()**: Leest codelists in en genereert een overeenkomstig post-deployment script.
* **\_generate\_load\_dates()**: Genereert een script dat een stored procedure uitvoert voor het laden van datums.
* **\_generate\_post\_deploy\_master()**: Maakt of werkt een hoofdscript bij dat alle gegenereerde post-deployment scripts achtereenvolgens uitvoert.
* **\_get\_relative\_path()**: Berekent relatieve paden voor scriptverwijzingen in het hoofdscript, om correcte scriptinclusie te garanderen.

**CodeListReader (Klasse):**

* Hulpmiddel voor het lezen en verwerken van codelijstbestanden uit specifieke systeemdirectories (DMS en AGS). Het hoofddoel is om codelijstgegevens te verzamelen uit Excel-bestanden (.xls) die zich bevinden in aangewezen submappen, deze te standaardiseren en terug te geven als een lijst van dictionaries voor post-deployment
* **Initialisatie**: Slaat pad op van de inputdirectory en bereidt een interne lijst voor om codelijsten op te slaan.
* **read()**: Hoofdmethode die codelijsten leest voor zowel DMS als AGS systemen door een interne helper aan te roepen, waarna de resultaten worden samengevoegd en geretourneerd.
* **_read_system_list**: Helpermethode die alle .xls-bestanden in de directory van het opgegeven systeem vindt, de inhoud leest en verwerkt met behulp van Polars, kolomnamen standaardiseert en de data teruggeeft als een lijst van dictionaries.

---

## Patronen en Praktijken

* Maakt gebruik van Jinja2-templating voor flexibele scriptgeneratie.
* Organiseert de uitvoer in een gestructureerde mapstructuur, zodat scripts vindbaar en uitvoerbaar zijn in de juiste volgorde.
* Onderhoudt een hoofdscript om de post-deployment uitvoering te stroomlijnen.
* Gebruikt een logging utility (`logtools.get_logger`) voor status- en foutmeldingen.

---

## API referentie

### ::: src.deploy_mdde.deployment.DeploymentMDDE

---

### ::: src.deploy_mdde.deployment.TemplateType

---

### ::: src.deploy_mdde.data_code_lists.CodeListReader
