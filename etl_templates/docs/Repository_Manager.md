# Repository Management

![Repository](images/repository.png){ align=right width="90" }

Deze documentatie beschrijft het gebruik van het Python-package datverantwoordelijk is voor het automatiseren en beheren van veelvoorkomende repository-operaties in een lokale ontwikkelomgeving, met name voor DevOps-gebaseerde repositories. De belangrijkste taken zijn het klonen van repositories, beheren van branches, committen en pushen van wijzigingen, en het synchroniseren van lokale mappen met de repository. De klasse verwerkt ook gebruikersauthenticatie en integreert met projectbestandsbeheer.

## Belangrijke Componenten

### `RepositoryManager` Klasse

De centrale klasse waarin alle logica voor repositorybeheer is ondergebracht. Wordt geïnitialiseerd met een configuratieobject dat repositorydetails bevat (zoals URL, branchnamen, paden, enz.).


### Integratie met `ProjectFile`

Werkt samen met een `ProjectFile`-klasse (geïmporteerd uit een zuster-module) om projectspecifieke metadata of configuratie te beheren.

## API referentie

### ::: src.repository_manager.repository_manager.RepositoryManager

---

### ::: src.repository_manager.project_file.ProjectFile

---

### ::: src.repository_manager.file_sql_project.SqlProjEditor
