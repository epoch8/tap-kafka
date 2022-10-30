import importlib
import singer
import platform
import os
import ssl
from singer import utils
from kafka import KafkaConsumer
import json
import sys
import tap_kafka.sync as sync
import tap_kafka.discovery as discovery


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "bootstrap_servers",
    "topic",
    "schema"
]


def get_ssl_context():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.check_hostname = True
    ssl_context.load_default_certs()

    if platform.system().lower() == 'darwin':
        import certifi
        ssl_context.load_verify_locations(
            cafile=os.path.relpath(certifi.where()),
            capath=None,
            cadata=None)

    return ssl_context


def get_value_deserializer(config):
    if 'deserializer' in config:
        path = config['deserializer'].split('.')
        function_name = path.pop()
        module_name = ".".join(path)
        module = importlib.import_module(module_name)
        function = getattr(module, function_name)
        return lambda m: function(m.decode('utf-8'))

    return lambda m: json.loads(m.decode("utf-8"))


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    consumer = None
    try:
        consumer_config = args.config.get("consumer_config", {})
        
        if any(
            i.startswith('ssl') or i.startswith('sasl') or i == 'security_protocol' 
            for i in consumer_config.keys()
        ):
            # Do not force SSL context if user knows better
            ssl_context = None
        else:
            ssl_context = get_ssl_context()

        consumer = KafkaConsumer(
            bootstrap_servers=args.config['bootstrap_servers'],
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            consumer_timeout_ms=args.config.get("consumer_timeout_ms", 10000),
            value_deserializer=get_value_deserializer(args.config),
            ssl_context=ssl_context,
            ** consumer_config
        )
    except Exception as ex:
        LOGGER.critical(
            "Unable to connect to kafka. bootstrap_servers: %s, topic: %s",
            args.config["bootstrap_servers"].split(","),
            args.config["topic"],
        )
        raise ex

    if args.discover:
        discovery.do_discovery(consumer, args.config)
    elif args.catalog:
        state = args.state or {}
        sync.do_sync(args.config, consumer, state, args.catalog)
    else:
        LOGGER.info("No catalog selections were made")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
