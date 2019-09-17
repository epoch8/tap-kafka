import singer
from singer import utils, metadata, write_schema
from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition
import pdb
import sys
import json
import time
import copy
from jsonschema import ValidationError, Draft4Validator, FormatChecker

LOGGER = singer.get_logger()
UPDATE_BOOKMARK_PERIOD = 1000


def write_schema_message(schema_message):
    sys.stdout.write(json.dumps(schema_message) + "\n")
    sys.stdout.flush()


def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams


def do_sync(config, consumer, state, catalog):
    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        if stream_id in selected_stream_ids:
            LOGGER.info('Syncing stream:' + stream_id)
            sync_stream(config, consumer, stream, state)

    return


def send_reject_message(config, message, reject_reason):
    producer = KafkaProducer(bootstrap_servers=config["bootstrap_servers"], value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    message.record["stitch_error"] = reject_reason
    future = producer.send(config["reject_topic"], message.record)
    try:
        future.get(timeout=10)
    except Exception as ex:
        LOGGER.critical(ex)
        raise ex


def sync_stream(config, consumer, stream, state):
    write_schema(stream.tap_stream_id, stream.schema.to_dict(), stream.key_properties or [])
    stream_version = singer.get_bookmark(state, stream.tap_stream_id, "version")
    if stream_version is None:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state, stream.tap_stream_id, "version", stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    activate_version_message = singer.ActivateVersionMessage(stream=stream.tap_stream_id, version=stream_version)
    singer.write_message(activate_version_message)

    validator = Draft4Validator(stream.schema.to_dict(), format_checker=FormatChecker())
    time_extracted = utils.now()
    rows_saved = 0

    # Assign all the partitions for the topic to this consumer
    topic_partitions = [TopicPartition(config['topic'], partition_id) for partition_id in consumer.partitions_for_topic(config['topic'])]
    consumer.assign(topic_partitions)

    # Seek each partition to it's value from the STATE, or to the beginning otherwise
    offsets = singer.get_offset(state, stream.tap_stream_id, {})
    for topic_partition in topic_partitions:
        if str(topic_partition.partition) in offsets:
            consumer.seek(topic_partition, offsets[str(topic_partition.partition)])
        else:
            consumer.seek_to_beginning(topic_partition)

    for message in consumer:
        record = singer.RecordMessage(stream=stream.tap_stream_id, record=message.value, time_extracted=time_extracted)
        validator.validate(record.record)
        singer.write_message(record)

        state = singer.set_offset(state, stream.tap_stream_id, message.partition, message.offset)
        rows_saved = rows_saved + 1
        if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
