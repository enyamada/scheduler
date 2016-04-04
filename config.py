import os
import yaml




def read_aws_env_config(config):

    config["ami-id"]        = os.environ.get("AWS_AMI_ID", config["ami-id"])
    config["spot-price"]    = os.environ.get("AWS_SPOT_PRICE", config["spot-price"])
    config["instance-type"] = os.environ.get("AWS_INSTANCE_TYPE", config["instance-type"])
    config["key-name"]      = os.environ.get("AWS_KEY_NAME", config["key-name"])
    config["sg-name"]       = os.environ.get("AWS_SG_NAME", config["sg-name"])


def read_app_env_config(config):

    config["polling-interval"] = os.environ.get("APP_POLLING_INTERVAL", config["polling-interval"])



def read_db_env_config (config):

   config["host"] = os.environ.get("MYSQL_PORT_3306_TCP_ADDR", config["host"])


def read_log_env_config (config):

   config["level"] = os.environ.get("LOG_LEVEL", config["level"])


def read_config(config_file):

    with open (config_file, "r") as f:
        config = yaml.load (f)

    read_aws_env_config(config["aws"])
    read_app_env_config(config["app"])
    read_db_env_config(config["db"])
    read_log_env_config(config["log"])

    return config


