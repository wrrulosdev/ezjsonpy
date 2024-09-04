import json
import os
from typing import Union, Dict, Optional, List
from functools import reduce

import aiofiles

from .exceptions import ConfigurationAlreadyLoadedError, ConfigurationFileNotFoundError, ConfigurationNotLoadedError


class ConfigLoader:
    _instance: Optional['ConfigLoader'] = None
    configurations: Dict[str, dict]
    config_paths: Dict[str, str]

    def __new__(cls, *args, **kwargs) -> 'ConfigLoader':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.configurations = {}
            cls._instance.config_paths = {}

        return cls._instance

    def load_configuration(self, config_name: str, config_path: Union[str, os.PathLike]) -> None:
        """
        Load a configuration from a JSON file.

        :param str config_name: Configuration name
        :param Union[str, os.PathLike] config_path: Path to the configuration file
        :raises ConfigurationAlreadyLoadedError: Configuration already loaded
        :raises ConfigurationFileNotFoundError: Configuration file not found
        :raises ValueError: Configuration file is not a valid JSON file
        """
        if config_name in self.configurations:
            raise ConfigurationAlreadyLoadedError(f'Configuration {config_name} already loaded')

        if not os.path.exists(config_path):
            raise ConfigurationFileNotFoundError(f'Configuration file {config_path} not found')

        with open(config_path, 'r', encoding='utf-8') as config_file:
            try:
                self.configurations[config_name] = json.load(config_file)
                self.config_paths[config_name] = config_path
            except json.decoder.JSONDecodeError:
                raise ValueError(f'Configuration file {config_path} is not a valid JSON file')

    async def async_load_configuration(self, config_name: str, config_path: Union[str, os.PathLike]) -> None:
        """
        Load a configuration from a JSON file asynchronously.

        :param str config_name: Configuration name
        :param Union[str, os.PathLike] config_path: Path to the configuration file
        :raises ConfigurationAlreadyLoadedError: Configuration already loaded
        :raises ConfigurationFileNotFoundError: Configuration file not found
        :raises ValueError: Configuration file is not a valid JSON file
        """
        if config_name in self.configurations:
            raise ConfigurationAlreadyLoadedError(f'Configuration {config_name} already loaded')

        if not os.path.exists(config_path):
            raise ConfigurationFileNotFoundError(f'Configuration file {config_path} not found')

        async with aiofiles.open(config_path, 'r', encoding='utf-8') as config_file:
            content: str = await config_file.read()

            try:
                self.configurations[config_name] = json.loads(content)
                self.config_paths[config_name] = config_path

            except json.decoder.JSONDecodeError:
                raise ValueError(f'Configuration file {config_path} is not a valid JSON file')

    def load_configurations(self, configurations: List[Dict[str, Union[str, os.PathLike]]]) -> None:
        """
        Load multiple configurations from JSON files

        :param List[Dict[str, Union[str, os.PathLike]]] configurations: List of dictionaries with configuration names and paths
        """
        self._check_configuration_list(configurations=configurations)

        for configuration in configurations:
            self.load_configuration(configuration['name'], configuration['path'])

    async def async_load_configurations(self, configurations: List[Dict[str, Union[str, os.PathLike]]]) -> None:
        """
        Load multiple configurations from JSON files asynchronously

        :param List[Dict[str, Union[str, os.PathLike]]] configurations: _description_
        """
        self._check_configuration_list(configurations=configurations)

        for configuration in configurations:
            await self.async_load_configuration(configuration['name'], configuration['path'])

    def get_config(self, key: str, config_name: str) -> Union[str, int, float, bool, None, dict, list]:
        """
        Get a value from a configuration by key

        :param str key: Key to get the value from
        :param str config_name: Configuration name
        :raises ConfigurationNotLoadedError: Configuration not loaded
        :return Union[str, int, float, bool, None, dict, list]: Value from the configuration
        """
        if config_name not in self.configurations:
            raise ConfigurationNotLoadedError(f'Configuration {config_name} not loaded')

        keys: List[str] = key.split('.')
        return reduce(lambda d, k: d.get(k, key) if isinstance(d, dict) else key, keys, self.configurations[config_name])

    def set_config(self, key: str, value: Union[str, int, float, bool, None, dict, list], config_name: str) -> None:
        """
        Set a value in a configuration by key and save the changes to the configuration file

        :param str key: Key to set the value in
        :param Union[str, int, float, bool, None, dict, list] value: Value to set
        :param str config_name: Configuration name
        :raises ConfigurationNotLoadedError: Configuration not loaded
        """
        if config_name not in self.configurations:
            raise ConfigurationNotLoadedError(f'Configuration {config_name} not loaded')

        key_parts: List[str] = key.split('.')
        config_dict: dict = self.configurations[config_name]

        for k in key_parts[:-1]:
            config_dict: dict = config_dict.setdefault(k, {})

        config_dict[key_parts[-1]] = value
        config_path: str = self.config_paths[config_name]

        with open(config_path, 'w', encoding='utf-8') as config_file:
            json.dump(self.configurations[config_name], config_file, indent=4)

    def remove_config(self, key: str, config_name: str) -> None:
        """
        Remove a value from a configuration by key and save the changes to the configuration file

        :param str key: Key to remove the value from
        :param str config_name: Configuration name
        :raises ConfigurationNotLoadedError: Configuration not loaded
        """
        if config_name not in self.configurations:
            raise ConfigurationNotLoadedError(f'Configuration {config_name} not loaded')

        key_parts: List[str] = key.split('.')
        config_dict: dict = self.configurations[config_name]

        for k in key_parts[:-1]:
            config_dict: dict = config_dict.setdefault(k, {})

        try:
            del config_dict[key_parts[-1]]

        except KeyError:
            raise KeyError(f'Key {key} not found in configuration {config_name}')
        
        config_path: str = self.config_paths[config_name]

        with open(config_path, 'w', encoding='utf-8') as config_file:
            json.dump(self.configurations[config_name], config_file, indent=4)

    async def async_set_config(self, key: str, value: Union[str, int, float, bool, None, dict, list], config_name: str) -> None:
        """
        Set a value in a configuration by key and save the changes to the configuration file asynchronously

        :param str key: Key to set the value in
        :param Union[str, int, float, bool, None, dict, list] value: Value to set
        :param str config_name: Configuration name
        :raises ConfigurationNotLoadedError: Configuration not loaded
        """
        if config_name not in self.configurations:
            raise ConfigurationNotLoadedError(f'Configuration {config_name} not loaded')

        key_parts: List[str] = key.split('.')
        config_dict: dict = self.configurations[config_name]

        for part in key_parts[:-1]:
            config_dict = config_dict.setdefault(part, {})

        config_dict[key_parts[-1]] = value
        config_path = self.config_paths[config_name]

        async with aiofiles.open(config_path, 'w', encoding='utf-8') as config_file:
            await config_file.write(json.dumps(self.configurations[config_name], indent=4))

    async def async_remove_config(self, key: str, config_name: str) -> None:
        """
        Remove a value from a configuration by key and save the changes to the configuration file asynchronously

        :param str key: Key to remove the value from
        :param str config_name: Configuration name
        :raises ConfigurationNotLoadedError: Configuration not loaded
        """
        if config_name not in self.configurations:
            raise ConfigurationNotLoadedError(f'Configuration {config_name} not loaded')

        key_parts: List[str] = key.split('.')
        config_dict: dict = self.configurations[config_name]

        for part in key_parts[:-1]:
            config_dict = config_dict.setdefault(part, {})

        try:
            del config_dict[key_parts[-1]]

        except KeyError:
            raise KeyError(f'Key {key} not found in configuration {config_name}')
        
        config_path = self.config_paths[config_name]

        async with aiofiles.open(config_path, 'w', encoding='utf-8') as config_file:
            await config_file.write(json.dumps(self.configurations[config_name], indent=4))

    def get_configurations(self) -> Dict[str, dict]:
        """
        Get all loaded configurations

        :return Dict[str, dict]: Loaded configurations
        """
        return self.configurations

    def _check_configuration_list(self, configurations: List[Dict[str, Union[str, os.PathLike]]]) -> None:
        """
        Check if the configurations list is valid

        :param List[Dict[str, Union[str, os.PathLike]]] configurations: List of dictionaries with configuration names and paths
        :raises ValueError: Invalid configurations list | Each configuration entry must be a dictionary | Each configuration dictionary must have "name" and "path" keys | "name" must be a string and "path" must be a string or os.PathLike
        """
        if not isinstance(configurations, list):
            raise ValueError('Configurations must be a list of dictionaries')

        for configuration in configurations:
            if not isinstance(configuration, dict):
                raise ValueError('Each configuration entry must be a dictionary')

            if 'name' not in configuration or 'path' not in configuration:
                raise ValueError('Each configuration dictionary must have "name" and "path" keys')

            if not isinstance(configuration['name'], str) or not isinstance(configuration['path'], (str, os.PathLike)):
                raise ValueError('"name" must be a string and "path" must be a string or os.PathLike')


config_loader: ConfigLoader = ConfigLoader()


def load_configuration(config_name: str, config_path: Union[str, os.PathLike]) -> None:
    """
    Load a configuration from a JSON file

    :param str config_name: Configuration name
    :param Union[str, os.PathLike] config_path: Configuration path
    """
    config_loader.load_configuration(config_name, config_path)


def load_configurations(configurations: List[Dict[str, Union[str, os.PathLike]]]) -> None:
    """
    Load multiple configurations from JSON files

    :param List[Dict[str, Union[str, os.PathLike]]] configurations: List of dictionaries with configuration names and paths
    """
    config_loader.load_configurations(configurations)


async def async_load_configuration(config_name: str, config_path: Union[str, os.PathLike]) -> None:
    """
    Load a configuration from a JSON file asynchronously

    :param str config_name: Configuration name
    :param Union[str, os.PathLike] config_path: Configuration path
    """
    await config_loader.async_load_configuration(config_name, config_path)


async def async_load_configurations(configurations: List[Dict[str, Union[str, os.PathLike]]]) -> None:
    """
    Load multiple configurations from JSON files asynchronously

    :param List[Dict[str, Union[str, os.PathLike]]] configurations: List of dictionaries with configuration names and paths
    """
    await config_loader.async_load_configurations(configurations)


def get_configuration(config_name: str) -> dict:
    """
    Get a loaded configuration

    :param str config_name: Configuration name
    :raises ConfigurationNotLoadedError: Configuration not loaded
    :return dict: Loaded configuration
    """
    if config_name not in config_loader.get_configurations():
        raise ConfigurationNotLoadedError(f'Configuration {config_name} not loaded')

    return config_loader.get_configurations()[config_name]


def get_configurations() -> Dict[str, dict]:
    """
    Get all loaded configurations

    :return Dict[str, dict]: Loaded configurations
    """
    return config_loader.get_configurations()


def get_config_value(key: str, config_name: str = 'default') -> Union[str, int, float, bool, None, dict, list]:
    """
    Get a value from a configuration by key

    :param str key: Key to get the value from
    :param str config_name: Configuration name
    :return Union[str, int, float, bool, None, dict, list]: Value from the configuration
    """
    return config_loader.get_config(key, config_name)


def set_config_value(key: str, value: Union[str, int, float, bool, None, dict, list], config_name: str = 'default') -> None:
    """
    Set a value in a configuration by key and save the changes to the configuration file

    :param str key: Key to set the value in
    :param Union[str, int, float, bool, None, dict, list] value: Value to set
    :param str config_name: Configuration name
    """
    config_loader.set_config(key, value, config_name)


def remove_config_value(key: str, config_name: str = 'default') -> None:
    """
    Remove a value from a configuration by key and save the changes to the configuration file

    :param str key: Key to remove the value from
    :param str config_name: Configuration name
    """
    config_loader.remove_config(key, config_name)
    
    
async def async_set_config_value(key: str, value: Union[str, int, float, bool, None, dict, list], config_name: str = 'default') -> None:
    """
    Set a value in a configuration by key and save the changes to the configuration file asynchronously

    :param str key: Key to set the value in
    :param Union[str, int, float, bool, None, dict, list] value: Value to set
    :param str config_name: Configuration name
    """
    await config_loader.async_set_config(key, value, config_name)


async def async_remove_config_value(key: str, config_name: str = 'default') -> None:
    """
    Remove a value from a configuration by key and save the changes to the configuration file asynchronously

    :param str key: Key to remove the value from
    :param str config_name: Configuration name
    """
    await config_loader.async_remove_config(key, config_name)


def remove_configuration(config_name: str) -> None:
    """
    Remove a loaded configuration

    :param str config_name: Configuration name
    :raises ConfigurationNotLoadedError: Configuration not loaded
    """
    if config_name not in config_loader.get_configurations():
        raise ConfigurationNotLoadedError(f'Configuration {config_name} not loaded')

    del config_loader.get_configurations()[config_name]


def remove_all_configurations() -> None:
    """ Remove all loaded configurations """
    config_loader.get_configurations().clear()
    config_loader.config_paths.clear()
