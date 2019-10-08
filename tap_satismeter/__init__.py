#!/usr/bin/env python3
import json
import os
import sys
from typing import List

import arrow
import requests
from requests.auth import HTTPBasicAuth
from tenacity import retry, stop_after_attempt, wait_fixed
from singer import (get_logger, metadata, utils, write_record, write_schema,
                    write_state)
from singer.catalog import Catalog
from singer.metrics import record_counter, http_request_timer

REQUIRED_CONFIG_KEYS = ["project_id", "api_key"]
LOGGER = get_logger()
SESSION = requests.Session()

SATISMETER_BASE_URL = "https://app.satismeter.com/api"


@retry(stop=stop_after_attempt(2), wait=wait_fixed(1), reraise=True)
@utils.ratelimit(1, 5)
def request(path: str, params: dict = None, auth=None,
            user_agent: str = None, extra_headers: dict = None):
    url = SATISMETER_BASE_URL + path

    params = params or {}
    headers = {}

    if extra_headers:
        headers.update(extra_headers)

    if user_agent is not None:
        headers["User-Agent"] = user_agent

    req = requests.Request("GET", url, params=params, headers=headers, auth=auth).prepare()
    LOGGER.info("GET %s", req.url)

    with http_request_timer(url):
        resp = SESSION.send(req, timeout=(3, 30))

    if resp.status_code >= 400:
        LOGGER.error("GET %s [%s - %s]", req.url, resp.status_code, resp.content)
        sys.exit(1)

    return resp


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    """ read all json files in the schemas folder. """
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        if path.endswith('.json'):
            schema_name = filename.replace('.json', '')
            with open(path) as file:
                yield schema_name, json.load(file)


def get_key_properties(schema_name: str) -> List[str]:
    """ return for each schema which fields are mandatory included. """
    if schema_name == 'responses':
        return ['id', 'created', 'rating', 'category', 'score', 'feedback']
    return []


def discover():
    streams = []

    for schema_name, schema in load_schemas():

        meta_data = metadata.get_standard_metadata(schema=schema, schema_name=schema_name,
                                                   key_properties=get_key_properties(schema_name))

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': meta_data,
            'key_properties': get_key_properties(schema_name),
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def output_responses(stream_id, config: dict, state: dict) -> dict:
    """ Query and output the api for individual responses """

    while True:
        previous_state_end_datetime = state.get(
            'bookmarks', {}).get(stream_id, {}).get('last_record', None)

        # Start where the previous run left off or it's a first run.
        start_datetime = arrow.get(
            previous_state_end_datetime or '2015-01-01')

        # request data from the api in blocks of a month
        end_datetime = start_datetime.shift(months=1)

        # Fetch data from api
        params = {
            "format": "json",
            "project": config["project_id"],
            "startDate": start_datetime.isoformat(),
            "endDate": end_datetime.isoformat(),
        }

        res_json = request(
            '/responses/', params=params,
            auth=HTTPBasicAuth(config["api_key"], None),
            user_agent=config.get('user_agent', None)
        ).json()

        # Output items
        bookmark = start_datetime
        with record_counter(endpoint=stream_id) as counter:
            for record in res_json['responses']:
                write_record(stream_id, record)
                counter.increment()
                bookmark = max([arrow.get(record['created']), bookmark])

        # If we're not past the current timestamp, set the bookmark
        # to the end_datetime requested as there won't be any new ones
        # coming in for past times.
        if end_datetime < arrow.utcnow():
            bookmark = end_datetime

        # Update and export state
        if 'bookmarks' not in state:
            state['bookmarks'] = {}
        state['bookmarks'][stream_id] = {'last_record': bookmark.isoformat()}

        write_state(state)

        # Stop when we had requested past the current timestamp,
        # there won't be anything more.
        if end_datetime > arrow.utcnow():
            break

    return state


def sync(config: dict, state: dict, catalog: Catalog) -> None:
    """ sync performs querying of the api and outputting results. """
    selected_stream_ids = [s.tap_stream_id for s in catalog.streams]

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        if stream_id in selected_stream_ids:
            LOGGER.info('Syncing stream: %s', stream_id)
            write_schema(stream_id, stream.schema.to_dict(), 'id')

            if stream_id == 'responses':
                state = output_responses(stream_id, config, state)
            else:
                LOGGER.error("No handler for stream_id: %s", stream_id)

        LOGGER.info('Finished syncing stream: %s', stream_id)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = Catalog.from_dict(discover())

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
