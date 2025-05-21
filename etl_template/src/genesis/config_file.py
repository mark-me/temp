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
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message} (Error Code: {self.error_code})"


class ConfigFile:
    """Manages configuration settings from a YAML file.

    Reads and writes configuration data to a YAML file, providing access to settings
    like default file/directory paths, level colors, and export options.
    """

    def __init__(self, file_config: str):
        """Initialize the ConfigFile object.

        Sets up the configuration file path, initializes data, defines default settings,
        and reads the configuration from the file.
        """
        self._file = Path(file_config)
        self._data: ConfigData = self._read_file()
        self._version = self._determine_version()

    def _read_file(self) -> ConfigData:
        """Read and parse the configuration file.

        Reads the configuration from the YAML file, if it exists, and populates the internal data.
        If the file doesn't exist, it logs a warning.
        """
        if not self._file.exists():
            msg = f"Couldn't find config file '{self._file}'"
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
        """Recursively fills default values for dataclass fields.

        Handles nested dataclasses and populates fields with values from the provided data,
        falling back to default values or default factories if available.
        Raises a ConfigFileError if a required key is missing.
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
        """Create a directory if it doesn't exist.

        Creates the specified directory, including any necessary parent directories.
        If the provided path is a file, it extracts the directory part.
        """
        if dir_path.is_file():
            dir_path = os.path.dirname(dir_path)
        Path(dir_path).mkdir(parents=True, exist_ok=True)

    def _determine_version(self) -> str:
        """Determine the version for the output folder.

        Determines the version string by checking existing version folders and incrementing the patch number.
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
        """Convert a configuration dataclass to YAML with comments.

        Recursively converts the given dataclass to a YAML string, adding comments from the field_comments dictionary.
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
        """Create an example configuration file.

        Generates an example YAML configuration file with default values and writes it to the specified path.
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
    def dir_intermediate(self) -> str:
        """Directory where all intermediate output is placed

        Returns the path to the default file specified in the configuration.
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
        """All Power Designer document paths

        Returns a list of Power Designer document paths
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
    def dir_extract(self) -> str:
        """Directory for extracted data.

        Returns the path to the extraction directory within the intermediate output folder.
        """
        folder = Path(os.path.join(self.dir_intermediate, self._data.extractor.folder))
        self._create_dir(folder)
        return folder
    
    @property
    def dir_templates(self) -> str:
        """Directory of JINJA templates.

        Returns the path of the JINJA template folder.
        """
        root = './etl_templates/src/generator/templates'
        folder = Path(os.path.join(root, self._data.generator.templates_platform))
        self._create_dir(folder)
        return folder    

    @property
    def dir_generate(self) -> str:
        """Directory for generated data.

        Returns the path to the generation directory within the intermediate output folder.
        """
        folder = Path(os.path.join(self.dir_intermediate, self._data.generator.folder))
        self._create_dir(folder)
        return folder
    
    @property
    def dir_repository(self) -> str:
        """Directory for GIT repository.

        Returns the path to the generation directory within the intermediate output folder.
        """
        folder = Path(os.path.join(self._data.folder_intermediate_root, self._data.devops.folder))
        self._create_dir(folder)
        return folder

    @property
    def devops_config(self) -> DevOpsConfig:
        return self._data.devops
    
    @property
    def codelist_config(self) -> CodelistConfig:
        return self._data.codelist
    
    @property
    def publisher_config(self) -> PublisherConfig:
        return self._data.publisher
    
    @property
    def generator_config(self) -> GeneratorConfig:
        return self._data.generator 
    
    @property
    def dir_codelist(self) -> str:
        """Directory for extracted data.

        Returns the path to the extraction directory within the intermediate output folder.
        """
        folder = Path(os.path.join(self.dir_intermediate, self._data.codelist.folder))
        self._create_dir(folder)
        return folder

