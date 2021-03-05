#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os

from providers.provider import Provider


class MissingProviderError(Exception):
    pass


class InvalidProviderError(Exception):
    pass


def get_source_provider(args) -> Provider:
    return __get_provider(args.platform1, {
        'image': args.image1,
        'input': args.input1,
        'output': args.output1,
        'logging': args.logging1
    })


def get_target_provider(args) -> Provider:
    return __get_provider(args.platform2, {
        'image': args.image2,
        'input': args.input2,
        'output': args.output2,
        'logging': args.logging2,
    })


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
