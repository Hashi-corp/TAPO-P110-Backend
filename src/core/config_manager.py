import yaml
from pathlib import Path

class ConfigManager:
    @staticmethod
    def load_config(config_type: str):
        with open(Path(__file__).parent.parent.parent / f"config/{config_type}_config.yaml") as f:
            return yaml.safe_load(f)