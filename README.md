# tap-satismeter

This is a singer tap to query the Satismeter export responses REST api.

## Connecting tap-satismeter

### Setup

You need your Satismeter `Project ID` and need to create an `API key` (also called Read key). This is described on the Satismeter docs for the [Export responses api](https://help.satismeter.com/en/articles/87961-export-responses-api)


---

## tap-satismeter Replication

The tap will query the Satismeter REST api to obtain all of the user responses for your project.

The number of rows replicated equals the number of responses. The datasource is append-only style, the tap will keep track of the last replicated response to only replicate new responses (and it will only request responses later than that moment from the Satismeter api).

There are no rate limits enforced on the side of Satismeter api, but the tap has a rate limit of requests to Satismeter of 1 per 5 seconds, and each request has a 30 seconds timeout.

---

## tap-satismeter Table Schemas

- Table name: responses
- Description: all of the user responses
- Primary key column(s): id
- Replicated fully or incrementally _(uses a bookmark to maintain state)_: incrementally, based on the `created` field
- Bookmark column(s): _(if replicated incrementally)_ : created
- Link to API endpoint documentation: [Satismeter Export Responses api](https://help.satismeter.com/en/articles/87961-export-responses-api)

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
