import os
from dataclasses import dataclass, field, fields, is_dataclass, MISSING
from io import StringIO
from pathlib import Path
from typing import Any

import yaml
from logtools import get_logger

from .base import BaseConfigApplication
from .deploy_mdde import DeploymentMDDEConfig, DeploymentMDDEConfigData
from .devops import DevOpsConfig, DevOpsConfigData

logger = get_logger(__name__)


@dataclass
class MorningstarConfigData:
    """
    Bevat alle configuratiegegevens voor de applicatie.
    Biedt velden voor algemene instellingen, Power Designer, extractor, generator, DevOps en MDDE deployment configuraties.

    """

    title: str
    folder_intermediate_root: str
    folder: str
    folder_output: str = "CentralLayer/Failure Reports"
    ignore_warnings: bool = False

    devops: DevOpsConfigData = field(default_factory=DevOpsConfigData)
    deployment_mdde: DeploymentMDDEConfigData = field(
        default_factory=DeploymentMDDEConfigData
    )


class MorningstarConfig(BaseConfigApplication[MorningstarConfigData]):
    """
    Beheert de volledige applicatieconfiguratie voor Morningstar en laadt deze uit een YAML-bestand.

    Deze klasse biedt toegang tot alle deelconfiguraties, paden en hulpfuncties voor het werken met configuratiebestanden.
    MorningstarConfig automatiseert het inlezen van configuratiegegevens voor de Morningstar-applicatie.
    """
    CONFIG_DATACLASS = MorningstarConfigData

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
        self.ignore_warnings = data.ignore_warnings
        self.folder = data.folder
        self.folder_output = data.folder_output
        self._version = self._determine_version()
        self.deploy_mdde = DeploymentMDDEConfig(
            data.deployment_mdde, path_intermediate=self.path_intermediate
        )
        self.devops = DevOpsConfig(
            data.devops, path_output_root=data.folder_intermediate_root
        )

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
                        value, field_comments=field_comments, indent=indent + 1
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
            "ignore-warnings": "Negeert waarschuwingen voor non-interactieve runs",
            "power_designer": "Instellingen voor PowerDesigner LDM-bestanden",
            "folder": "Submap binnen de root waar PowerDesigner bestanden staan",
            "files": "Lijst van PowerDesigner .ldm-bestanden",
            "extractor": "Instellingen voor extractie uit RETW",
            "generator": "Instellingen voor genereren van DDL/ETL",
            "publisher": "Instellingen voor publicatie van scripts",
            "devops": "DevOps instellingen zoals werkitems en branch",
            "work_item_description": "Omschrijving van het DevOps werkitem",
        }
        example_config = MorningstarConfigData()
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

    @property
    def path_output(self) -> Path:
        """
        Geeft het pad naar de tussenliggende outputfolder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de root, titel en versie van de configuratie.

        Returns:
            Path: Het pad naar de tussenliggende outputfolder.
        """
        folder = Path(self.folder_intermediate_root) / self.title / self._version / self.folder_output
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    @property
    def path_input(self) -> Path:
        """
        Geeft het pad naar de tussenliggende outputfolder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de root, titel en versie van de configuratie.

        Returns:
            Path: Het pad naar de tussenliggende outputfolder.
        """
        folder = Path(self.folder_intermediate_root) / self.folder
        folder.mkdir(parents=True, exist_ok=True)
        return folder