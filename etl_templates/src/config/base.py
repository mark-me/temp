import os
from pathlib import Path
import dataclasses
#from dataclasses import field, fields, is_dataclass
import yaml
from typing import Generic, TypeVar, Type, Any
from dacite import from_dict, Config, MissingValueError, WrongTypeError

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
        Retourneert de string-representatie van de ConfigFileError.
        Geeft de foutmelding samen met de foutcode terug.

        Returns:
            str: De foutmelding en foutcode als string.
        """
        return f"{self.message} (Error Code: {self.error_code})"


class BaseConfigComponent:
    """
    Basisklasse voor configuratiecomponenten.
    Biedt functionaliteit voor het beheren van configuratiegegevens en hulpfuncties zoals het aanmaken van directories.
    """

    def __init__(self, config):
        """
        Initialiseert een BaseConfigComponent met de opgegeven configuratiegegevens.
        Slaat de configuratie op voor gebruik door afgeleide componenten.

        Args:
            config: De configuratiegegevens die door het component beheerd worden.
        """
        self._data = config

    def create_dir(self, path: Path) -> None:
        """
        Maakt de opgegeven directory aan als deze nog niet bestaat.
        Controleert of het pad een bestand is en converteert het naar een director-ypad indien nodig.

        Args:
            path (Path): Het pad naar de directory die aangemaakt moet worden.
        """
        if path.is_file():
            path = Path(os.path.dirname(path))
        path.mkdir(parents=True, exist_ok=True)

# TypeVar die verplicht een dataclass type moet zijn
T = TypeVar("T")


class BaseConfigApplication(Generic[T]):
    CONFIG_DATACLASS: Type[T]  # Wordt ingesteld door de subklasse

    def _read_file(self) -> T:
        """
        Leest de configuratie uit het YAML-bestand en converteert deze naar het type T.
        """
        if not hasattr(self, "CONFIG_DATACLASS"):
            raise NotImplementedError(
                "Subklasse moet CONFIG_DATACLASS instellen op een dataclass."
            )

        try:
            with open(self._file, "r") as file:
                config_dict = yaml.safe_load(file)

            config_dict = self._replace_hyphens_with_underscores(config_dict)

            config = from_dict(
                data_class=self.CONFIG_DATACLASS,
                data=config_dict,
                config=Config(strict=True),
            )
            return config

        except FileNotFoundError as e:
            raise ConfigFileError("Configuratiebestand niet gevonden.", 100) from e
        except yaml.YAMLError as e:
            raise ConfigFileError(f"Fout bij het parsen van YAML: {e}", 101) from e
        except MissingValueError as e:
            raise ConfigFileError(f"Verplichte waarde ontbreekt: {e}", 102) from e
        except WrongTypeError as e:
            raise ConfigFileError(f"Verkeerd type voor configuratieparameter: {e}", 103) from e
        except Exception as e:
            raise ConfigFileError(f"Onverwachte fout bij het laden van de configuratie: {e}", 199) from e

    def _replace_hyphens_with_underscores(self, config_raw: dict | list) -> dict | list:
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
        filled_data = {}
        if not dataclasses.is_dataclass(cls):
            raise TypeError(f"cls moet een dataclass type zijn, is een {type(cls)}")
        for field in dataclasses.fields(cls):
            key = field.name
            if key in data:
                filled_data[key] = data[key]
            elif field.default is not dataclasses.MISSING:
                filled_data[key] = field.default
            elif field.default_factory is not dataclasses.MISSING:  # type: ignore
                filled_data[key] = field.default_factory()  # type: ignore
            else:
                raise ConfigFileError(f"Ontbrekend verplicht configuratieveld '{key}' zonder default waarde.", error_code=404)
        return cls(**filled_data)
