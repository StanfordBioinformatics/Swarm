import argparse
import logging
from os import path
from tempfile import TemporaryDirectory

from providers import factory as provider_factory

logger = logging.getLogger('federated')


def main():
    args = parse_cli_args()

    configure_logging(args)

    logger.info('Platform1: "{}" -- Platform2: "{}"'.format(args.platform1, args.platform2))

    platform1 = provider_factory.get_source_provider(args.platform1, {
        'image': args.image1,
        'input': args.input1,
        'output': args.output1,
        'logging': args.logging1
    })

    platform2 = provider_factory.get_target_provider(args.platform2, {
        'image': args.image2,
        'input': args.input2,
        'output': args.output2,
        'logging': args.logging2,
    })

    logger.info('Starting workload on Platform1')
    platform1.run_source()
    logger.info('Completed workload on Platform1')

    with TemporaryDirectory() as temp_dir:
        logger.info('Starting transfer of output files from Platform1 to Platform2')
        platform1.download_blobs(path.dirname(args.output1), temp_dir)
        platform1.download_blobs(args.input1, temp_dir)

        result = platform2.upload_blobs(temp_dir, args.input2)
        logger.info('Completed transfer of output files from Platform1 to Platform2. %s', result)

        logger.info('Starting workload on Platform2')
        platform2.run_target()
        logger.info('Completed workload on Platform2')


def parse_cli_args():
    parser = argparse.ArgumentParser(description='Swarm: Run Commands Across Cloud Providers')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action='store_true')
    group.add_argument('-q', '--quiet', action='store_true')
    parser.add_argument('--platform1', type=str, help='GCP/AWS/Azure', required=True)
    parser.add_argument('--image1', type=str, help='Container Image (e.g., sample:code:v0.1)', required=True)
    parser.add_argument('--input1', type=str, help='Input Dataset (e.g., gs://swarm-fed/swarm/data/in/)', required=True)
    parser.add_argument('--output1', type=str, help='Output Dataset (e.g., gs://swarm-fed/swarm/data/out/)',
                        required=True)
    parser.add_argument('--logging1', type=str, help='Logging (e.g., gs://swarm-fed/swarm/logging/)', required=False)
    parser.add_argument('--platform2', type=str, help='GCP/AWS/Azure', required=True)
    parser.add_argument('--image2', type=str, help='Container Image (e.g., sample:code:v0.1)', required=True)
    parser.add_argument('--input2', type=str, help='Input Dataset (e.g., s3://swarm-fed/swarm/data/in/)', required=True)
    parser.add_argument('--output2', type=str, help='Output Dataset (e.g., s3://swarm-fed/swarm/data/out/)',
                        required=True)
    parser.add_argument('--logging2', type=str, help='Logging (e.g., gs://swarm-fed/swarm/logging/)',
                        required=False)

    args = parser.parse_args()
    return args


def configure_logging(args):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    if args.quiet:
        logger.setLevel(logging.WARNING)
    elif args.verbose:
        logger.setLevel(logging.DEBUG)


if __name__ == '__main__':
    main()
