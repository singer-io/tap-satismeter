# tap-satismeter

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Created using https://github.com/singer-io/singer-tap-template and https://github.com/singer-io/tap-appsflyer/blob/master/tap_appsflyer/

## Instructions

Add the project id and api key for Satismeter in `tap_satismeter/config.json`

```bash
pip install -e .
```

```bash
python __init__.py -c ../config.json --discover > catalog.json
```

```bash
python __init__.py -c ../config.json --catalog catalog.json
```