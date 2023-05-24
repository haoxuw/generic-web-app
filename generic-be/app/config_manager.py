import os

import yaml


class ConfigManager:
    def __init__(self):
        if os.getenv("CONFIG_ENV") == "production":
            # todo
            assert False, "Not implemented"
        else:
            config_file_name = os.path.join(
                os.path.dirname(__file__), "configs", "development.yaml"
            )
        self.__configs = yaml.load(
            open(config_file_name, "r", encoding="utf-8"),
            Loader=yaml.FullLoader,
        )

    @property
    def configs(self):
        return self.__configs
