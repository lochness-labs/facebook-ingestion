# Singer extraction

## Prerequisites

- Singer.io's `tap-facebook`:
  - Make a Python virtual environment and activate it.
  - Run `pip install tap-facebook` inside your virtual environment.

## Instructions

### Make a config file

1. Make a copy of `singer/config_example.json`.
2. Put in your Facebook Ad account Id and your access token.

### Run in discovery mode

With Discovery mode you get a json file containing all the supported extraction streams of the Facebook Tap.

To start the Tap in discovery mode you need to run the following command

```shell
tap-facebook -c {config_file_name.json} -d > catalog.json`
```

### Make a properties.json file

Then, you choose the streams you want to extract and put them into a `properties.json` file.

In the metadata section of the streams, add a `"selected": true` inside each property you want to extract.

You can find an example in `properties_example.json`.


### Extraction mode

If you want to extract the data into a CSV file, you first need to install the `target-csv` executable into a different virtual environment (because it depends on different version of the Singer base Python package).

```shell
pip install target-csv
```

Then run:
    
```shell
tap-facebook -c {config_file_name.json} -p properties.json -s state.json | target-{your-target}`
```

## Useful links

- More info about the Facebook Ads tap made by Singer.io: https://github.com/singer-io/tap-facebook
- More info about the CSV target made by Singer.io: https://github.com/singer-io/target-csv
