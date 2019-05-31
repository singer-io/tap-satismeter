#!/usr/bin/env python3
import json
import os
import sys

import arrow
import requests
from requests.auth import HTTPBasicAuth
from tenacity import retry, stop_after_attempt, wait_fixed

import singer
from singer import metadata, utils

REQUIRED_CONFIG_KEYS = ["project_id", "api_key", "start_datetime"]
LOGGER = singer.get_logger()
SESSION = requests.Session()

SATISMETER_URL = "https://app.satismeter.com/api/responses"


CONFIG = {
  "project_id": "",
  "api_key": "",
  "start_datetime": "2019-03-01T00:00:00Z",
  "user_agent": ""
}

STATE = {
    'end_date': None
}



@retry(stop=stop_after_attempt(2), wait=wait_fixed(1), reraise=True)
@utils.ratelimit(10, 1)
def request(url, params=None, auth=None):

    params = params or {}
    headers = {}

    if "user_agent" in CONFIG:
        headers["User-Agent"] = CONFIG["user_agent"]

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


def discover():
    streams = []

    for schema_name, schema in load_schemas():

        # TODO: populate any metadata and stream's key properties here..
        # md = metadata.get_standard_metadata(schema=schema, schema_name=schema_name)
        md = []
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

def sync(config, state, catalog):
    # selected_stream_ids = get_selected_streams(catalog)
    selected_stream_ids = [s.tap_stream_id for s in catalog.streams]
    selected_stream_schemas = {d['tap_stream_id']: d['schema'] for d in discover()['streams']}

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        # stream_schema = stream.schema
        stream_schema = selected_stream_schemas[stream_id]
        if stream_id in selected_stream_ids:
            LOGGER.info('Syncing stream:' + stream_id)
            singer.write_schema(stream_id, stream_schema, 'id')

            while True:
                if 'end_datetime' not in state:
                    startDateTime = arrow.get(CONFIG['start_datetime'])
                else:
                    startDateTime = arrow.get(state['end_datetime'])
                endDateTime = startDateTime.shift(days=30)

                # LOGGER.info("Processing " + startDateTime.isoformat() + ' to ' + endDateTime.isoformat())

                params = {
                    "format": "json",
                    "project": CONFIG["project_id"],
                    "startDate": startDateTime.isoformat(),
                    "endDate": endDateTime.isoformat(),
                }

                bookmark = startDateTime
                res = request(SATISMETER_URL, params=params, auth=HTTPBasicAuth(CONFIG["api_key"], None))
                res_json = res.json()
                for response in res_json['responses']:
                    singer.write_record(stream_id, response)
                    bookmark = max([arrow.get(response['created']), bookmark])

                STATE.update({
                    'start_datetime': startDateTime.isoformat(),
                    'end_datetime': endDateTime.isoformat()
                })
                state.update(STATE)

                # Write out state
                # TODO: find the right way to do this
                utils.update_state(STATE, stream_id, bookmark.datetime)
                singer.write_state(STATE)

                if endDateTime > arrow.utcnow():
                    break
        LOGGER.info('Finished syncing stream:' + stream_id)
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    if args.state:
        STATE.update(args.state)

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
