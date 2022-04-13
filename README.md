# Facebook Ingestion for Data Lakes

This glue jobs uses the Facebook Marketing API to retrieve `ad`, `ad_set`, `campaign`, `ad_image`, and `ad_insights` (also known as facebook's `extractions`) data for all advertising account under your business account on Facebook. 
For each API object, the job retrieves the last execution time, and gets all updated/new data since then. It then proceeds to store data, metadata, and it runs the crawler to add new data to the catalog.

The glue script is of type `pythonshell`.

The infrastructure is described (IaC) and deployed with Serverless Framework (https://www.serverless.com/framework/).

The infrastructure was developed on the AWS cloud platform.

## Checklist

- [ ] TODO: Make example IAM policy that includes all the required permissions, then test it.
- [ ] TODO: Add remaining open source libaries attributions.
- [ ] TODO: Virtual environment instructions.
- [ ] TODO: Add remaining deployment instructions.

## Deployment instructions

1. Open `env/dev.yaml` file and substitute:
   1. `000000000000` with your AWS account id.
   2. `example-data-s3-bucket-name` for your data lake AWS S3 bucket.
   3. `example-code-s3-bucket-name` for your code AWS S3 bucket.
2. Make a secret on AWS Secrets Manager for your Facebook access token and save its name on the`secret_name` field in `facebook-ingest/env/dev.yaml`.
   1. For example, we named it `accessToken-appId-appSecret-businessId/facebookApi/ingestion`.
3. Make a IAM role for the Glue job, that includes all the required permissions (as per the example policy **that is coming soon, TODO**)
   1. Substitute its ARN in the `facebook-ingest/env/dev.yaml` file (`role_arn: arn:aws:iam::000000000000:role/example_glue_role`).
4. Install npm dependencies: `npm install`.
5. Deploy on AWS with: `sls deploy --stage {stage}`.
   1. Substitute `{stage}` with one of the available stages defined as the YAML files in the `facebook-ingest/env/` directory.


## Requirements

- Node.js and NPM: https://nodejs.org/en/
- Serverless Framework: https://www.serverless.com/framework/docs/getting-started/

## Environments

The `facebook-ingest/env/` contains the environment configuration files, one for each of your AWS environments.

For example: substitute `example_enviroment.yaml` with `dev.yaml` for a development environment.

## Trigger Schedule:

By default, the glue job is triggered by the following rules:

- Every 55 minutes, between 6 and 20, from Mondays to Fridays
- At 10, on Saturdays and Sundays

## Open source libraries used in this project

- Serverless Framework: https://github.com/serverless/serverless
- serverless-glue: https://github.com/toryas/serverless-glue


---

This is a project created by [Linkalab](https://linkalab.it) and Talent Garden [Talent Garden](https://talentgarden.org).
