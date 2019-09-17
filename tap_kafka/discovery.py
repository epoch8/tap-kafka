import json
import sys
from singer import metadata, get_logger

LOGGER = get_logger()


def dump_catalog(all_streams):
    json.dump({"streams": all_streams}, sys.stdout, indent=2)


def default_streams(config):
    schema = config["schema"]
    primary_keys = config.get("primary_keys", [])

    mdata = {}
    metadata.write(mdata, (), "table-key-properties", primary_keys)

    return [{"tap_stream_id": config["topic"], "metadata": metadata.to_list(mdata), "schema": schema}]


def do_discovery(consumer, config):
    if config["topic"] not in consumer.topics():
        LOGGER.warn(
            "Unable to view topic %s. bootstrap_servers: %s, topic: %s",
            config["topic"],
            config["bootstrap_servers"].split(","),
            config["topic"],
        )
        raise Exception("Unable to view topic {}".format(config["topic"]))

    dump_catalog(default_streams(config))
