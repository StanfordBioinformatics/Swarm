import json
import os

from providers.provider import Provider


class MissingProviderError(Exception):
    pass


class InvalidProviderError(Exception):
    pass


def get_source_provider(source: str, conf) -> Provider:
    return __get_provider(source, conf)


def get_target_provider(target, conf) -> Provider:
    return __get_provider(target, conf)


def __get_provider(name, initial_conf) -> Provider:
    if not name:
        raise MissingProviderError

    config_path = os.path.join('conf', '{}.json'.format(name))
    with open(config_path, 'r') as config_file:
        conf = json.load(config_file)
        conf.update(initial_conf)

        if name.lower() == 'gcp':
            from .gcp import GCPProvider
            return GCPProvider(conf)
            pass
        elif name.lower() == 'azure':
            from .azure import AzureProvider
            return AzureProvider(conf)
            pass
        elif name.lower() == 'aws':
            from.aws import AWSProvider
            return AWSProvider(conf)
            pass

    raise InvalidProviderError
