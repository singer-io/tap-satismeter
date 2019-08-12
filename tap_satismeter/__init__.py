#!/usr/bin/env python3
import json
import os
import sys
from typing import List

import arrow
import requests
import singer
from requests.auth import HTTPBasicAuth
from singer import metadata, utils
from singer.catalog import Catalog
from tenacity import retry, stop_after_attempt, wait_fixed

REQUIRED_CONFIG_KEYS = ["project_id", "api_key", "start_date"]
LOGGER = singer.get_logger()
SESSION = requests.Session()

SATISMETER_URL = "https://app.satismeter.com/api/responses"


@retry(stop=stop_after_attempt(2), wait=wait_fixed(1), reraise=True)
@utils.ratelimit(10, 1)
def request(url: str, params: dict = None, auth=None, user_agent: str = None):

    params = params or {}
    headers = {}

    if user_agent is not None:
        headers["User-Agent"] = user_agent

    req = requests.Request("GET", url, params=params, headers=headers, auth=auth).prepare()
    LOGGER.info("GET %s", req.url)

    # with singer.stats.Timer(source=parse_source_from_url(url)) as stats:
    #     resp = SESSION.send(req)
    #     stats.http_status_code = resp.status_code
    resp = SESSION.send(req)

    if resp.status_code >= 400:
        LOGGER.error("GET %s [%s - %s]", req.url, resp.status_code, resp.content)
        sys.exit(1)

    return resp


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        if path.endswith('.json'):
            schema_name = filename.replace('.json', '')
            with open(path) as file:
                yield schema_name, json.load(file)


def get_key_properties(schema_name: str) -> List[str]:
    if schema_name == 'response':
        return ['id', 'created', 'rating', 'category', 'score', 'feedback']
    return []


def discover():
    streams = []

    for schema_name, schema in load_schemas():

        # TODO: populate any metadata and stream's key properties here..
        md = metadata.get_standard_metadata(schema=schema, schema_name=schema_name,
            key_properties=get_key_properties(schema_name)
        )
        # md = []
        # stream_metadata = [{'breadcrumb': 'selected', 'metadata': True}]
        stream_key_properties = []

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': md,
            'key_properties': []
        }
        streams.append(catalog_entry)

    return {'streams': streams}


# def get_selected_streams(catalog):
#     '''
#     Gets selected streams.  Checks schema's 'selected' first (legacy)
#     and then checks metadata (current), looking for an empty breadcrumb
#     and mdata with a 'selected' entry
#     '''
#     selected_streams = []
#     import ipdb; ipdb.set_trace()
#     for stream in catalog.streams:
#         stream_metadata = metadata.to_map(stream.metadata)
#         # stream metadata will have an empty breadcrumb
#         if metadata.get(stream_metadata, (), "selected"):
#             selected_streams.append(stream.tap_stream_id)

#     return selected_streams

def sync(config: dict, state: dict, catalog: Catalog) -> None:
    # selected_stream_ids = get_selected_streams(catalog)
    selected_stream_ids = [s.tap_stream_id for s in catalog.streams]

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        if stream_id in selected_stream_ids:
            LOGGER.info(f'Syncing stream: {stream_id}')
            singer.write_schema(stream_id, stream.schema.to_dict(), 'id')

            while True:
                # TODO: use end_date in state or use bookmark? How do they relate
                # if 'bookmarks' in state:  # Use bookmark as starting point
                    # startDateTime = arrow.get(state['bookmarks'][stream_id]['last_record'])
                if 'end_date' in state:  # Start where the previous run left off. TODO: is this necessary to keep in state? Isn't it either bookmark or state?
                    startDateTime = arrow.get(state['end_date'])
                else:  # This case indicates a first-ever run
                    startDateTime = arrow.get(config['start_date'])

                # request a month of data from the api
                endDateTime = startDateTime.shift(days=30)

                # Fetch data from api
                params = {
                    "format": "json",
                    "project": config["project_id"],
                    "startDate": startDateTime.isoformat(),
                    "endDate": endDateTime.isoformat(),
                }

                res = request(SATISMETER_URL, params=params, auth=HTTPBasicAuth(config["api_key"], None), user_agent=config.get('user_agent'))

                # Output items
                # bookmark = startDateTime
                for response in res.json()['responses']:
                    singer.write_record(stream_id, response)
                    # bookmark = max([arrow.get(response['created']), bookmark])

                # Update and export state
                state.update({
                    'start_date': startDateTime.isoformat(),
                    'end_date': endDateTime.isoformat()
                })

                # utils.update_state(state, stream_id, bookmark.datetime)
                # if 'bookmarks' not in state:
                #     state['bookmarks'] = {}
                # state['bookmarks'][stream_id] = {'last_record': bookmark.isoformat()}
                singer.write_state(state)

                if endDateTime > arrow.utcnow():
                    break
        LOGGER.info(f'Finished syncing stream: {stream_id}')
    return


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
            catalog =  discover()

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
