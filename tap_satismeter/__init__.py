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

SATISMETER_URL = "https://app.satismeter.com/api/responses"


@retry(stop=stop_after_attempt(2), wait=wait_fixed(1), reraise=True)
@utils.ratelimit(1, 1)
def request(url: str, params: dict = None, auth=None, user_agent: str = None):

    params = params or {}
    headers = {}

    if user_agent is not None:
        headers["User-Agent"] = user_agent

    req = requests.Request("GET", url, params=params, headers=headers, auth=auth).prepare()
    LOGGER.info("GET %s", req.url)

    with http_request_timer(url):
        resp = SESSION.send(req)

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
    if schema_name == 'response':
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
            'key_properties': []
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def sync(config: dict, state: dict, catalog: Catalog) -> None:
    """ sync performs querying of the api and outputting results. """
    selected_stream_ids = [s.tap_stream_id for s in catalog.streams]

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        if stream_id in selected_stream_ids:
            LOGGER.info('Syncing stream: %s', stream_id)
            write_schema(stream_id, stream.schema.to_dict(), 'id')

            while True:
                previous_state_end_datetime = state.get('bookmarks', {}).get(stream_id, {}).get('last_record', None)

                if previous_state_end_datetime is not None: # Start where the previous run left off.
                    start_datetime = arrow.get(previous_state_end_datetime)
                else:  # This case indicates a first-ever run
                    start_datetime = arrow.get('2015-01-01')

                # request data from the api in blocks of 30 days
                end_datetime = start_datetime.shift(days=30)

                # Fetch data from api
                params = {
                    "format": "json",
                    "project": config["project_id"],
                    "startDate": start_datetime.isoformat(),
                    "endDate": end_datetime.isoformat(),
                }

                res_json = request(
                    SATISMETER_URL, params=params,
                    auth=HTTPBasicAuth(config["api_key"], None),
                    user_agent=config.get('user_agent', None)
                ).json()

                # Output items
                bookmark = start_datetime
                with record_counter(endpoint=stream_id) as counter:
                    for response in res_json['responses']:
                        write_record(stream_id, response)
                        counter.increment()
                        bookmark = max([arrow.get(response['created']), bookmark])
                
                    # If no responses were returned and the end_datetime requested is not past the current timestamp,
                    # it means we can move the bookmark to the end_datetime to request the following period.
                    # If the end_datetime is past the current timestamp, it means there are still responses that can
                    # come in; don't update the bookmark, the loop will exit at the end due to the if-break condition
                    if counter.value == 0 and end_datetime < arrow.utcnow():
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
            catalog = discover()

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
