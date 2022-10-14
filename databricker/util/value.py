from typing import Dict
from dataclasses import dataclass

from . import singleton

class InfraConfig(singleton.Singleton):

    def configure(self,
                  infra_config_file: str = "_infra/infra.toml",
                  pyproject: str = "pyproject.toml",
                  dist: str = "dist"):
        self.infra_config_file = infra_config_file
        self.pyproject = pyproject
        self.dist = dist
        pass

    def configured(self) -> bool:
        return hasattr(self, 'infra_config_file')


@dataclass
class DataClassAbstract:
    def replace(self, key, value):
        setattr(self, key, value)
        return self


@dataclass
class ConfigValue(DataClassAbstract):
    project: Dict
    infra: Dict
    databrickcfg: Dict
    args: Dict = None
    artefact_location: str = None

