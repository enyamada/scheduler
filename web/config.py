
"""
  Module that contains functions related to reading and handling
  configuration files. The yaml module is leveraged here.

"""

import os
import yaml


def read_aws_env_config(config):
    """
    Check if any AWS configuration parameter was specified using a
    environment variable. If so, it must override the value provided
    by the dictionary passed (that must be updated for later use).

    Args:
        config: dictionary containing the aws default parameters
    """

    config["ami-id"] = os.environ.get("AWS_AMI_ID", config["ami-id"])
    config[
        "spot-price"] = os.environ.get("AWS_SPOT_PRICE", config["spot-price"])
    config[
        "instance-type"] = os.environ.get("AWS_INSTANCE_TYPE",
                                          config["instance-type"])
    config["key-name"] = os.environ.get("AWS_KEY_NAME", config["key-name"])
    config["sg-name"] = os.environ.get("AWS_SG_NAME", config["sg-name"])


def read_app_env_config(config):
    """
    Check if any app configuration parameter was specified using a
    environment variable. If so, it must override the value provided
    by the dictionary passed (that must be updated for later use).

    Args:
        config: dictionary containing the app default parameters
    """


    config["polling-interval"] = os.environ.get(
        "APP_POLLING_INTERVAL", config["polling-interval"])


def read_db_env_config(config):
    """
    Check if any db configuration parameter was specified using a
    environment variable. If so, it must override the value provided
    by the dictionary passed (that must be updated for later use).

    Args:
        config: dictionary containing the db default parameters
    """
    config["host"] = os.environ.get("MYSQL_PORT_3306_TCP_ADDR", config["host"])


def read_log_env_config(config):
    """
    Check if any log configuration parameter was specified using a
    environment variable. If so, it must override the value provided
    by the dictionary passed (that must be updated for later use).

    Args:
        config: dictionary containing the log default parameters
    """
    config["level"] = os.environ.get("LOG_LEVEL", config["level"])


def read_config(config_file):
    """
    Reads the parameters described in the passed config file (that must be
    a YAML file). Then checks if any of them were overriden by a environment
    variable and  finally returns a dictionary containing the
    setting in effect.

    Args:
        config_file: YAML file to be used as a configuration file.

    Returns:
        A dictionary as retuned by the yaml module.
    """

    with open(config_file, "r") as f:
        config = yaml.load(f)

    read_aws_env_config(config["aws"])
    read_app_env_config(config["app"])
    read_db_env_config(config["db"])
    read_log_env_config(config["log"])

    return config
