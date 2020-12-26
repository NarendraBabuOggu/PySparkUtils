import argparse
from argparse import Namespace
from pysparkutils.process_transform_rules import process_all_transform_rules
from pysparkutils.dependencies.utils import get_spark


def main(args: Namespace) -> bool:
    """
    The Function to process the data at the given path using the given transformation rules 
    and store them at given destination path. if any file fails at transformation level,
    it will be stored in error path

    Args:
        args (Namespace): Argument Namespace containing the required arguments

    Returns:
        Boolean: Returns True if Success 
    """

    try:
        spark, _, logger = get_spark(args.config_path, env=args.env)

        process_all_transform_rules(
            args.data_path, 
            args.transform_rule_path, 
            args.error_path, 
            args.output_path, 
            spark, logger, 
            header = True, 
            inferSchema = True, 
            mode = 'FAILFAST', 
            save_mode = 'overwrite'
        )

        return True
    except Exception as e:
        raise(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog = "main.py",
        usage = """
        python main.py -d="path/to/data/file.csv" -o="path/to/ouput/file" -e="path/to/ouput/file" -c="path/to/config/file" -t="path/to/transform/file" --env='env'
        """,
        description = "Argument Parser for processing the data",
        add_help = True
    )

    parser.add_argument('-d', '--data-path', type = str, required = True, help = "Path Containing the Input Files", dest = 'data_path')
    parser.add_argument('-o', '--output-path', type = str, required = True, help = "Path to place the Output Files", dest = 'output_path')
    parser.add_argument('-e', '--error-path', type = str, required = True, help = "Path to place the Errored Files", dest = 'error_path')
    parser.add_argument('-c', '--config-path', type = str, default = 'hocon.conf', help = "Path Containing Configuration File", dest = 'config_path')
    parser.add_argument('-t', '--transform-rules-path', type = str, default = 'transform_rules.txt', help = "Path Containing Transformations File", dest = 'transform_rule_path')
    parser.add_argument('--env', type = str, default = 'dev', help = "The Environment Configuration to pick from config file", dest = 'env')

    args = parser.parse_args()

    main(args)

