import os
from dataclasses import field, fields, is_dataclass
from io import StringIO
from pathlib import Path
from typing import Any

import yaml

from logtools import get_logger

from config_definition import ConfigData, DevOpsConfig, CodelistConfig, PublisherConfig, GeneratorConfig

logger = get_logger(__name__)


class ConfigFileError(Exception):
    """Exception raised for configuration file errors."""

    def __init__(self, message, error_code):
        """
        Initialiseert een ConfigFileError met een foutmelding en foutcode.
        Deze exceptie wordt gebruikt om fouten in het configuratiebestand te signaleren.

        Args:
            message (str): De foutmelding.
            error_code (int): De bijbehorende foutcode.
        """
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

    def __str__(self):
        """
        Retourneert de stringrepresentatie van de ConfigFileError.
        Geeft de foutmelding samen met de foutcode terug.

        Returns:
            str: De foutmelding en foutcode als string.
        """
        return f"{self.message} (Error Code: {self.error_code})"


class ConfigFile:
    """Leest configuratie uit een YAML bestand.

    Leest configuratie data uit een YAML bestand, en biedt toegang tot opties aan.
    """

    def __init__(self, file_config: str):
        """
        Initialiseert het ConfigFile object met een opgegeven configuratiebestand.
        Leest de configuratie uit het YAML-bestand en bepaalt de versie voor de outputfolder.

        Args:
            file_config (str): Het pad naar het configuratiebestand.
        """
        self._file = Path(file_config)
        self._data: ConfigData = self._read_file()
        self._version = self._determine_version()

    def _read_file(self) -> ConfigData:
        """
        Leest het configuratiebestand en retourneert de geparste configuratie data.
        Controleert op verplichte velden en converteert de YAML-inhoud naar een ConfigData object.

        Returns:
            ConfigData: De geparste configuratie data.

        Raises:
            ConfigFileError: Als het bestand niet bestaat, leeg of ongeldig is, of verplichte sleutels ontbreken.
        """
        if not self._file.exists():
            msg = f"Couldn't find config file '{self._file.resolve()}'"
            logger.error(msg)
            raise ConfigFileError(msg, 404)

        with open(self._file) as f:
            config_raw = yaml.safe_load(f)

        if not isinstance(config_raw, dict):
            raise ConfigFileError("Configuratiebestand is leeg of ongeldig.", 400)

        # Verplichte toplevel velden
        for key in ["title", "folder_intermediate_root"]:
            if key not in config_raw:
                raise ConfigFileError(
                    f"Verplichte configuratiesleutel ontbreekt: {key}", 402
                )

        config_raw["power_designer"] = config_raw.pop("power-designer")

        return self._fill_defaults(ConfigData, config_raw)

    def _fill_defaults(self, cls, data: dict):
        """
        Vult ontbrekende velden in een dataclass aan met standaardwaarden.
        Loopt door de velden van de dataclass en vult deze aan met waarden uit de data dictionary of met standaardwaarden indien nodig.

        Args:
            cls: De dataclass waarvan de velden worden gevuld.
            data (dict): De dictionary met configuratiewaarden.

        Returns:
            Een instantie van de dataclass met alle velden ingevuld.

        Raises:
            ConfigFileError: Als een verplicht veld ontbreekt en geen standaardwaarde heeft.
        """
        init_args = {}
        for f in fields(cls):
            if f.name in data:
                val = data[f.name]
                init_args[f.name] = (
                    self._fill_defaults(f.type, val)
                    if is_dataclass(f.type) and isinstance(val, dict)
                    else val
                )
            elif f.default != field(default=None).default:
                init_args[f.name] = f.default
            elif (
                f.default_factory != field(default_factory=lambda: None).default_factory
            ):
                init_args[f.name] = f.default_factory()
            else:
                raise ConfigFileError(f"Ontbrekende configuratie voor: '{f.name}'", 400)
        return cls(**init_args)

    def _create_dir(self, dir_path: Path) -> None:
        """
        Maakt de opgegeven directory aan als deze nog niet bestaat.
        Controleert of het pad een bestand is en converteert het naar een directorypad indien nodig.

        Args:
            dir_path (Path): Het pad naar de directory die aangemaakt moet worden.
        """
        if dir_path.is_file():
            dir_path = os.path.dirname(dir_path)
        Path(dir_path).mkdir(parents=True, exist_ok=True)

    def _determine_version(self) -> str:
        """
        Bepaalt de volgende versienaam voor de outputfolder op basis van bestaande versies.
        Zoekt naar bestaande versiemappen en verhoogt het patchnummer, of start bij de standaardversie als er geen zijn.

        Returns:
            str: De volgende versienaam in het formaat 'vXX.XX.XX'.
        """
        version = "v00.01.00"
        folder = Path(
            os.path.join(
                self._data.folder_intermediate_root,
                self._data.title,
            )
        )
        if folder.exists():
            if lst_versions := [
                version for version in folder.iterdir() if version.is_dir()
            ]:
                # Extract version numbers, sort them, and increment the latest
                versions = sorted(
                    [v.name for v in lst_versions if v.name.startswith("v")],
                    key=lambda s: list(map(int, s[1:].split("."))),
                )
                latest_version = versions[-1]
                major, minor, patch = map(int, latest_version[1:].split("."))
                patch += 1
                version = f"v{major:02}.{minor:02}.{patch:02}"
        return version

    def _config_to_yaml_with_comments(
        self, config_dataclass: Any, field_comments: dict, indent=0
    ) -> str:
        """
        Genereert een YAML-string van een dataclass met commentaarregels.
        Loopt door de velden van de dataclass en voegt optioneel commentaar toe op basis van het field_comments dictionary.

        Args:
            config_dataclass (Any): De dataclass die omgezet wordt naar YAML.
            field_comments (dict): Dictionary met veldnamen als sleutel en commentaar als waarde.
            indent (int, optional): Het inspringniveau voor de YAML-output. Standaard 0.

        Returns:
            str: De YAML-string met commentaarregels.
        """
        output = StringIO()
        indent_str = "  " * indent
        for config_field in fields(config_dataclass):
            name = config_field.name
            value = getattr(config_dataclass, name)

            # Comment als die er is
            if name in field_comments:
                output.write(f"{indent_str}# {field_comments[name]}\n")

            output.write(f"{indent_str}{name}:")

            if is_dataclass(value):
                output.write("\n")
                output.write(
                    self._config_to_yaml_with_comments(
                        value, field_comments=field_comments, index=indent + 1
                    )
                )
            elif isinstance(value, list) and not value:
                output.write(" []\n")
            else:
                yaml_value = yaml.dump(value, default_flow_style=True).strip()
                output.write(f" {yaml_value}\n")

        return output.getvalue()

    def example_config(self, file_output: str) -> None:
        """
        Genereert een voorbeeldconfiguratiebestand met commentaar en schrijft dit naar een opgegeven bestand.
        Maakt gebruik van de standaardwaarden van ConfigData en voegt commentaar toe aan de YAML-output.

        Args:
            file_output (str): Het pad naar het bestand waarin de voorbeeldconfiguratie wordt opgeslagen.
        """
        field_comments = {
            "title": "De naam van de huidige uitvoering (bijv. 'dry-run')",
            "folder_intermediate_root": "Basismap waar tussenresultaten worden opgeslagen",
            "power_designer": "Instellingen voor PowerDesigner LDM-bestanden",
            "folder": "Submap binnen de root waar PowerDesigner bestanden staan",
            "files": "Lijst van PowerDesigner .ldm-bestanden",
            "extractor": "Instellingen voor extractie uit RETW",
            "generator": "Instellingen voor genereren van DDL/ETL",
            "publisher": "Instellingen voor publicatie van scripts",
            "devops": "DevOps instellingen zoals werkitems en branch",
            "work_item_description": "Omschrijving van het DevOps werkitem",
        }
        example_config = ConfigData()
        yaml_with_comments = self._config_to_yaml_with_comments(
            config_dataclass=example_config, field_comments=field_comments
        )
        with open(file_output, "w") as f:
            f.write(yaml_with_comments)

    @property
    def dir_intermediate(self) -> Path:
        """
        Geeft het pad naar de tussenliggende outputfolder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de root, titel en versie van de configuratie.

        Returns:
            Path: Het pad naar de tussenliggende outputfolder.
        """
        folder = Path(
            os.path.join(
                self._data.folder_intermediate_root,
                self._data.title,
                str(self._version),
            )
        )
        self._create_dir(dir_path=folder)
        return folder

    @property
    def files_power_designer(self) -> list:
        """
        Geeft een lijst van paden naar de PowerDesigner-bestanden die in de configuratie zijn opgegeven.
        Controleert of alle opgegeven bestanden bestaan en geeft anders een foutmelding.

        Returns:
            list: Een lijst van Path-objecten naar de PowerDesigner-bestanden.

        Raises:
            ConfigFileError: Als een of meer PowerDesigner-bestanden ontbreken.
        """
        lst_pd_files = self._data.power_designer.files
        lst_pd_files = [
            Path(
                os.path.join(
                    self._data.power_designer.folder,
                    pd_file,
                )
            )
            for pd_file in lst_pd_files
        ]
        if lst_missing := [str(file) for file in lst_pd_files if not file.exists()]:
            msg = f"Power Designer bestanden ontbreken: {', '.join(lst_missing)}"
            raise ConfigFileError(msg, 404)
        return lst_pd_files

    @property
    def dir_extract(self) -> Path:
        """
        Geeft het pad naar de extractie-outputfolder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de tussenliggende outputfolder en de extractor folder uit de configuratie.

        Returns:
            Path: Het pad naar de extractie-outputfolder.
        """
        folder = Path(os.path.join(self.dir_intermediate, self._data.extractor.folder))
        self._create_dir(folder)
        return folder

    @property
    def dir_templates(self) -> Path:
        """
        Geeft het pad naar de templates-folder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van het platform uit de generatorconfiguratie.

        Returns:
            Path: Het pad naar de templates-folder.
        """
        root = './etl_templates/src/generator/templates'
        folder = Path(os.path.join(root, self._data.generator.templates_platform))
        self._create_dir(folder)
        return folder

    @property
    def dir_generate(self) -> Path:
        """
        Geeft het pad naar de generator-outputfolder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de tussenliggende outputfolder en de generator folder uit de configuratie.

        Returns:
            Path: Het pad naar de generator-outputfolder.
        """
        folder = Path(os.path.join(self.dir_intermediate, self._data.generator.folder))
        self._create_dir(folder)
        return folder

    @property
    def dir_repository(self) -> Path:
        """
        Geeft het pad naar de DevOps repository-folder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de root en de devops folder uit de configuratie.

        Returns:
            Path: Het pad naar de repository-folder.
        """
        folder = Path(os.path.join(self._data.folder_intermediate_root, self._data.devops.folder))
        self._create_dir(folder)
        return folder

    @property
    def devops_config(self) -> DevOpsConfig:
        """
        Geeft de DevOps configuratie uit het geladen configuratiebestand.
        Retourneert het DevOpsConfig object met alle DevOps gerelateerde instellingen.

        Returns:
            DevOpsConfig: De DevOps configuratie.
        """
        return self._data.devops

    @property
    def codelist_config(self) -> CodelistConfig:
        """
        Geeft de codelijstconfiguratie uit het geladen configuratiebestand.
        Retourneert het CodelistConfig object met alle codelijst gerelateerde instellingen.

        Returns:
            CodelistConfig: De codelijstconfiguratie.
        """
        return self._data.codelist

    @property
    def publisher_config(self) -> PublisherConfig:
        """
        Geeft de publisher configuratie uit het geladen configuratiebestand.
        Retourneert het PublisherConfig object met alle publisher gerelateerde instellingen.

        Returns:
            PublisherConfig: De publisher configuratie.
        """
        return self._data.publisher

    @property
    def generator_config(self) -> GeneratorConfig:
        """
        Geeft de generator configuratie uit het geladen configuratiebestand.
        Retourneert het GeneratorConfig object met alle generator gerelateerde instellingen.

        Returns:
            GeneratorConfig: De generator configuratie.
        """
        return self._data.generator

    @property
    def dir_codelist(self) -> Path:
        """Directory for extracted data.

        Returns the path to the extraction directory within the intermediate output folder.
        """
        folder = Path(os.path.join(self.dir_intermediate, self._data.codelist.folder))
        self._create_dir(folder)
        return folder

    @property
    def dir_codelist_input(self) -> Path:
        """
        Geeft het pad naar de inputfolder voor codelijsten uit de configuratie.
        Controleert of de opgegeven inputfolder bestaat en een directory is, en geeft anders een foutmelding.

        Returns:
            Path: Het pad naar de codelist inputfolder.

        Raises:
            ConfigFileError: Als de inputfolder niet bestaat of geen directory is.
        """
        folder = Path(self._data.codelist.input_folder)
        if not folder.exists():
            ConfigFileError(message=f"Code list input directory '{folder}' doesn't exist", error_code=404)
        if not folder.is_dir():
            ConfigFileError(message=f"Code list input directory '{folder}' is not a folder", error_code=404)
        return folder

    @property
    def file_codelist_output(self) -> Path:
        """
        Geeft het pad naar het outputbestand voor codelijsten uit de configuratie.
        Retourneert het pad zoals opgegeven in de configuratie voor het codelist outputbestand.

        Returns:
            Path: Het pad naar het codelist outputbestand.
        """
        return Path(self._data.codelist.file_output)
