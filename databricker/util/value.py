from typing import Dict
from dataclasses import dataclass

from . import singleton

class InfraConfig(singleton.Singleton):

    def configure(self, infra_config_file: str, dist: str = "dist"):
        self.infra_config_file = infra_config_file
        self.dist = dist
        pass


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

