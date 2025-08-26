# Repository Management

![Repository](images/repository.png){ align=right width="90" }

Deze documentatie beschrijft het gebruik van het Python-package dat verantwoordelijk is voor het automatiseren en beheren van veelvoorkomende repository-operaties in een lokale ontwikkelomgeving, met name voor DevOps-gebaseerde repositories. De belangrijkste taken zijn het klonen van repositories, beheren van branches, committen en pushen van wijzigingen, en het synchroniseren van lokale mappen met de repository. De klasse verwerkt ook gebruikersauthenticatie en integreert met projectbestandsbeheer.

## Belangrijke Componenten

### `RepositoryManager` Klasse

De generieke klasse voor standaard git-acties en foutafhandeling. Wordt geïnitialiseerd met een configuratieobject dat repositorydetails bevat (zoals URL, branchnamen, paden, enz.).

### `SqlRepositoryManager` Klasse

Is een specialisatie van `RepositoryManager` voor SQL-projecten, met extra functionaliteit voor het beheren van SQL projectbestanden.

### Integratie met `SqlProjEditor`

Werkt samen met een hulpprogramma-klasse, `SqlProjEditor`, voor het programmatisch beheren van Microsoft SQL Server .sqlproj projectbestanden. Het hoofddoel is het automatiseren van het onderhoud van deze projectbestanden door:

* Het verwijderen van verwijzingen naar ontbrekende SQL-bestanden.
* Het toevoegen van nieuwe SQL-bestanden uit opgegeven mappen aan het project.
* Het opslaan van wijzigingen, met optionele back-upfunctionaliteit.

Het script is bedoeld om zelfstandig te worden uitgevoerd en werkt een .sqlproj-bestand bij zodat het overeenkomt met de actuele staat van SQL-scripts in de projectmappen. Het maakt gebruik van XML-parsing (via lxml.etree) om het projectbestand te manipuleren en gebruikt logging voor statusrapportage.

<details><summary>Voorbeeld code</summary>
```python
PATH_SQLPROJ = "pad/naar/project.sqlproj"
FOLDER_BUILD_SQL = "src/sql/build"
FOLDER_POSTDEPLOY_SQL = "src/sql/postdeploy"

editor = SqlProjEditor(PATH_SQLPROJ)
editor.remove_missing_files()
editor.add_new_files(FOLDER_BUILD_SQL, item_type="Build")
editor.add_new_files(FOLDER_POSTDEPLOY_SQL, item_type="None")
editor.save()
```
</details>

## API referentie

### ::: src.repository_manager.repository_manager.RepositoryManager

---

### ::: src.repository_manager.sql_repository_manager.SqlRepositoryManager

---

### ::: src.repository_manager.file_sql_project.SqlProjEditor
