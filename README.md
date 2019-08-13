# tap-satismeter

This is a singer tap to query the Satismeter REST api. Exports the responses and response statistics.

## Connecting tap-satismeter

### Setup

You need your Satismeter `Project ID` and need to create an `API key` (also called Read key). This is described on the Satismeter docs for the [Export responses api](https://help.satismeter.com/en/articles/87961-export-responses-api)


---

## tap-satismeter Replication

The tap will query the Satismeter REST api to obtain all of the user responses, and the per-month statistics for your project.

For _responses_, the number of rows replicated equals the number of responses. The datasource is append-only style, the tap will keep track of the last replicated response to only replicate new responses (and it will only request responses later than that moment from the Satismeter api).

For _response statistics_, the number of rows replicated equals the number of months. The tap will keep track of which month it has requested, and will continue from then on (it will always requery for the current month).

There are no rate limits enforced on the side of Satismeter api, but the tap has a rate limit of requests to Satismeter of 1 per 5 seconds, and each request has a 30 seconds timeout.

---

## tap-satismeter Table Schemas

1. Responses
    - Table name: responses
    - Description: all of the user responses
    - Primary key column(s): id
    - Replicated fully or incrementally _(uses a bookmark to maintain state)_: incrementally, based on the `created` field
    - Bookmark column(s): _(if replicated incrementally)_ : created
    - Link to API endpoint documentation: [Satismeter Export Responses API](https://help.satismeter.com/en/articles/87961-export-responses-api)

2. Response statistics
    - Table name: response-statistics
    - Description: aggregated response statistics per month
    - Primary key column(s): month
    - Replicated fully or incrementally _(uses a bookmark to maintain state)_: incrementally, based on the `month` field
    - Bookmark column(s): _(if replicated incrementally)_ : month
    - Link to API endpoint documentation: [Satismeter Response statistics API](https://help.satismeter.com/en/articles/93675-response-statistics-api)

---


## Instructions to run from source

Add the project id and api key for Satismeter in `tap_satismeter/config.json`.

Install the tap and its dependancies:

```bash
pip install -e .
```

```bash
cd tap_satismeter
```

Generate a catalog file which describes the included object stream schemas:

```bash
python __init__.py -c ../config.json --discover > catalog.json
```

Run the tap:

```bash
python __init__.py -c ../config.json --catalog catalog.json > output.json
```
