# Facebook Ingestion for Data Lakes

This glue jobs uses the Facebook Marketing API to retrieve `ad`, `ad_set`, `campaign`, `ad_image`, and `ad_insights` (also known as facebook's `extractions`) data for all advertising account under your business account on Facebook. 
For each API object, the job retrieves the last execution time, and gets all updated/new data since then. It then proceeds to store data, metadata, and it runs the crawler to add new data to the catalog.

## Checklist

- [ ] TODO: Make example IAM policy that includes all the required permissions, then test it.
- [ ] TODO: Open source libaries attribution.
- [ ] TODO: Virtual environment instructions.

## Requirements

- Node.js and NPM: https://nodejs.org/en/
- Serverless Framework: https://www.serverless.com/framework/docs/getting-started/

## Environments

The `facebook-ingest/env/` contains the environment configuration files, one for each of your AWS environments.

For example: substitute `example_enviroment.yaml` with `dev.yaml` for a development environment.

---

This is a project created by [Linkalab](https://linkalab.it) and Talent Garden [Talent Garden](https://talentgarden.org).
