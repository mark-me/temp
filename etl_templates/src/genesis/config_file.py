import os
from dataclasses import field, fields, is_dataclass
from io import StringIO
from pathlib import Path
from typing import Any

import yaml
from dacite import Config, MissingValueError, WrongTypeError, from_dict
from logtools import get_logger

from config_definition import (
    ConfigData,
    ConfigFileError,
    DeploymentMDDEConfigData,
    DevOpsConfigData,
    ExtractorConfigData,
    GeneratorConfigData,
    PowerDesignerConfigData,
)

logger = get_logger(__name__)


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
        data = self._read_file()
        self.folder_intermediate_root = data.folder_intermediate_root
        self.title = data.title
        self._version = self._determine_version()
        self.power_designer = PowerDesignerConfig(data.power_designer)
        self.extractor = ExtractorConfig(
            data.extractor, path_intermediate=self.path_intermediate
        )
        self.generator = GeneratorConfig(
            data.generator, path_intermediate=self.path_intermediate
        )
        self.deploy_mdde = DeploymentMDDEConfig(
            data.deployment_mdde, path_intermediate=self.path_intermediate
        )
        self.devops = DevOpsConfig(
            data.devops, path_output_root=data.folder_intermediate_root
        )

    def _read_file(self) -> ConfigData:
        try:
            with open(self._file, "r") as file:
                config_dict = yaml.safe_load(file)

            # Transform keys with hyphens to underscores
            config_dict = self._replace_hyphens_with_underscores(config_dict)

            # Attempt to map the dictionary to the ConfigData dataclass
            config = from_dict(
                data_class=ConfigData,
                data=config_dict,
                config=Config(strict=True),  # Will raise error if extra/missing fields
            )
            return config
        except FileNotFoundError as e:
            raise ConfigFileError("Configuratiebestand niet gevonden.", 100) from e

        except yaml.YAMLError as e:
            raise ConfigFileError(f"Fout bij het parsen van YAML: {e}", 101) from e

        except MissingValueError as e:
            raise ConfigFileError(f"Verplichte waarde ontbreekt: {e}", 102) from e

        except WrongTypeError as e:
            raise ConfigFileError(
                f"Verkeerd type voor configuratieparameter: {e}", 103
            ) from e

        except Exception as e:
            raise ConfigFileError(
                f"Onverwachte fout bij het laden van de configuratie: {e}", 199
            ) from e

    def _replace_hyphens_with_underscores(self, config_raw: Any) -> dict:
        """
        Vervangt koppeltekens door underscores in alle sleutels van een geneste dictionary of lijst.
        Doorloopt recursief de structuur en past de sleutels van dictionaries aan.

        Args:
            config_raw (dict): De dictionary waarin koppeltekens in sleutels worden vervangen.

        Returns:
            dict: De aangepaste dictionary met underscores in plaats van koppeltekens in de sleutels.
        """

        if isinstance(config_raw, dict):
            return {
                k.replace("-", "_"): self._replace_hyphens_with_underscores(v)
                for k, v in config_raw.items()
            }
        elif isinstance(config_raw, list):
            return [self._replace_hyphens_with_underscores(item) for item in config_raw]
        else:
            return config_raw

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

    def _determine_version(self) -> str:
        """
        Bepaalt de volgende versienaam voor de outputfolder op basis van bestaande versies.
        Zoekt naar bestaande versiemappen en verhoogt het patch-nummer, of start bij de standaardversie als er geen zijn.

        Returns:
            str: De volgende versienaam in het formaat 'vXX.XX.XX'.
        """
        version = "v00.01.00"
        folder = Path(
            os.path.join(
                self.folder_intermediate_root,
                self.title,
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
            indent (int, optional): Het inspring-niveau voor de YAML-output. Standaard 0.

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
            "folder_intermediate_root": "Basis-map waar tussenresultaten worden opgeslagen",
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
    def path_intermediate(self) -> Path:
        """
        Geeft het pad naar de tussenliggende outputfolder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de root, titel en versie van de configuratie.

        Returns:
            Path: Het pad naar de tussenliggende outputfolder.
        """
        folder = Path(self.folder_intermediate_root) / self.title / self._version
        folder.mkdir(parents=True, exist_ok=True)
        return folder


class BaseConfigComponent:
    def __init__(self, config):
        self._data = config

    def create_dir(self, path: Path) -> None:
        """
        Maakt de opgegeven directory aan als deze nog niet bestaat.
        Controleert of het pad een bestand is en converteert het naar een director-ypad indien nodig.

        Args:
            dir_path (Path): Het pad naar de directory die aangemaakt moet worden.
        """
        if path.is_file():
            path = os.path.dirname(path)
        Path(path).mkdir(parents=True, exist_ok=True)

class PowerDesignerConfig(BaseConfigComponent):
    def __init__(self, config: PowerDesignerConfigData):
        super().__init__(config)

    @property
    def files(self) -> list:
        """
        Geeft een lijst van paden naar de PowerDesigner-bestanden die in de configuratie zijn opgegeven.
        Controleert of alle opgegeven bestanden bestaan en geeft anders een foutmelding.

        Returns:
            list: Een lijst van Path-objecten naar de PowerDesigner-bestanden.

        Raises:
            ConfigFileError: Als een of meer PowerDesigner-bestanden ontbreken.
        """
        lst_pd_files = self._data.files
        lst_pd_files = [
            Path(
                os.path.join(
                    self._data.folder,
                    pd_file,
                )
            )
            for pd_file in lst_pd_files
        ]
        if lst_missing := [str(file) for file in lst_pd_files if not file.exists()]:
            msg = f"Power Designer bestanden ontbreken: {', '.join(lst_missing)}"
            raise ConfigFileError(msg, 404)
        return lst_pd_files


class ExtractorConfig(BaseConfigComponent):
    def __init__(self, config: ExtractorConfigData, path_intermediate: Path):
        super().__init__(config)
        self.path_intermediate = path_intermediate

    @property
    def path_output(self) -> Path:
        """
        Geeft het pad naar de extractie-outputfolder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de tussenliggende outputfolder en de extractor folder uit de configuratie.

        Returns:
            Path: Het pad naar de extractie-outputfolder.
        """
        folder = self.path_intermediate / self._data.folder_output
        self.create_dir(folder)
        return folder


class GeneratorConfig(BaseConfigComponent):
    def __init__(self, config: GeneratorConfigData, path_intermediate: Path):
        super().__init__(config)
        self.path_intermediate = path_intermediate

    @property
    def path_output(self) -> Path:
        """
        Geeft het pad naar de extractie-outputfolder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de tussenliggende outputfolder en de extractor folder uit de configuratie.

        Returns:
            Path: Het pad naar de extractie-outputfolder.
        """
        folder = self.path_intermediate / self._data.folder_output
        self.create_dir(folder)
        return folder

    @property
    def template_platform(self) -> str:
        return self._data.templates_platform


class DeploymentMDDEConfig(BaseConfigComponent):
    def __init__(self, config: DeploymentMDDEConfigData, path_intermediate: Path):
        super().__init__(config)
        self.path_intermediate = path_intermediate

    @property
    def path_output(self) -> Path:
        """
        Geeft het pad naar de extractie-outputfolder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de tussenliggende outputfolder en de extractor folder uit de configuratie.

        Returns:
            Path: Het pad naar de extractie-outputfolder.
        """
        folder = self.path_intermediate / self._data.folder_output
        self.create_dir(folder)
        return folder

    @property
    def schema(self) -> str:
        return self._data.schema

    @property
    def path_data_input(self) -> Path:
        return Path(self._data.folder_data)


class DevOpsConfig(BaseConfigComponent):
    def __init__(self, config: DevOpsConfigData, path_output_root: Path):
        super().__init__(config)
        self._path_output_root = path_output_root

    @property
    def path_local(self) -> Path:
        """
        Geeft het pad naar de DevOps repository-folder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de root en de devops folder uit de configuratie.

        Returns:
            Path: Het pad naar de repository-folder.
        """
        folder = Path(self._path_output_root) / self._data.folder
        self.create_dir(folder)
        return folder

    @property
    def branch(self) -> str:
        return self._data.branch

    @property
    def feature_branch(self) -> str:
        user_login = os.getlogin().replace(' ', '_')
        descr_work_item = self._data.work_item_description.replace(' ', '_')
        return f"feature/{self._data.work_item}_{descr_work_item}_{user_login}"

    @property
    def url(self) -> str:
        return f"https://{self._data.organisation}@dev.azure.com/{self._data.organisation}/{self._data.project}/_git/{self._data.repo}"

    @property
    def url_check(self) -> str:
        return f"https://dev.azure.com/{self._data.organisation}/{self._data.project}/_git/{self._data.repo}"

    @property
    def url_branch(self) -> str:
        """De URL van de repository branch waar de wijzigingen in worden doorgevoerd"""
        user_login = os.getlogin().replace(' ', '_')
        descr_work_item = self._data.work_item_description.replace(' ', '_')
        return f"{self.url_check}?version=GBfeature%2F{self._data.work_item}_{descr_work_item}_{user_login}"

    @property
    def path_vs_project_file(self):
        return Path(self._data.vs_project_file)


"""
devops:
  folder: "GIT_repo"
  organisation: "migratie-dataketen-douane"
  project: "Douane%20Datawerkorganisatie"
  repo: "DWO%20DataCenter"
  branch: "collaboration"
  work_item: "23936"
  work_item_description: "Testen automatische uitrol DDL en ETL"
  vs_project_file: "./CentralLayer/3. Central Layer.sqlproj
  """
