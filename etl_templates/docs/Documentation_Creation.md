# Documentatie genereren

![Generator](images/documentation.png){ align=right width="90" }

Documentatie voor Genesis wordt gegenereerd met behulp van [MkDocs](https://www.mkdocs.org/) met [MKDocs Marerial](https://squidfunk.github.io/mkdocs-material/). De statische pagina's zijn gemaakt als [MarkDown](https://www.markdownguide.org/) bestanden in de directory `etl_templates\docs` en de configuratie van de documentatie is terug te vinden in `etl_templates\mkdocs.yml`.

## 🚀 Gebruik

Om de documentatie te genereren moet een terminal met geactiveerde virtuele omgeving worden opgestart, waarna vanuit de directory `etl_templates` in de terminal met het volgende commando de documentatie site kan worden gegenereerd:

```bash
mkdocs build
```

De site met documentatie wordt in de directory `etl_templates\site` geplaatst.

Wanneer je de documentatie wil aanpassen en direct het resultaat in de browser kan zien, kun je gebruik maken van het commando:

```bash
mkdocs serve
```

Waarna je het de browser naar[http://127.0.0.1:8000/](http://127.0.0.1:8000/) kan gaan. Elke wijziging in de documentatie wordt hier direct in bijgewerkt.
