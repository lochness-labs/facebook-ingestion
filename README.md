# Facebook Ads Ingestion for Data Lakes

This AWS Glue job uses the Facebook Marketing API to retrieve `ad`, `ad_set`, `campaign`, `ad_image`, and `ad_insights` (also known as facebook's `extractions`) data for all advertising account under your business account on Facebook.
For each API object, the job retrieves the last execution time, and gets all updated/new data since then. It then proceeds to store data with AWS Data Wrangler which sinks the data in S3 and generates a validation metadata file.

The AWS Glue script is of type `pythonshell`.

The infrastructure is described (IaC) and deployed with Serverless Framework (https://www.serverless.com/framework/).

The infrastructure has been developed on the AWS Cloud Platform.

## Checklist

- [ ] TODO: Test job run.

## Requirements

- Node.js and NPM: https://nodejs.org/en/
- Serverless Framework: https://www.serverless.com/framework/docs/getting-started/
- virtualenv:

## Environments setup

The `facebook-ingest/env/` contains the environment configuration files, one for each of your AWS environments.

The name of the files corresponds to the environment names. For example: substitute `example_enviroment.yml` with `dev.yml` for a development environment.

## Development environment setup

1. Create virtualenv: `virtualenv -p python3 venv`
2. Activate virtualenv: `source venv/bin/activate`
3. Install requirements: `pip install -r requirements.txt`

## Deployment instructions

1. You need 2 AWS S3 buckets, one for the glue code and one as the Data Lake, if you have them, just keep in mind the names for the nexts steps, otherwise create the buckets on S3.
1. Make a copy of `facebook-ingest/env/example-environment.yml`, name it as your desired environment's name and substitute:
   1. `000000000000` with your AWS account id.
   2. `example-data-s3-bucket-name` for your data lake AWS S3 bucket.
   3. `example-code-s3-bucket-name` for your code AWS S3 bucket.
   4. `eu-west-1` with your AWS region.
2. Substitute `000000000000` with your AWS Account ID in `facebook-ingest/partials/resources.yml`.
3. Make a secret on AWS Secrets Manager for your Facebook access token and save its name on the `secret_name` field in your environment files located in `facebook-ingest/env/`.
   1. For example, we named it `accessToken-appId-appSecret-businessId/facebookApi/ingestion`.
4. Check and substitute s3 bucket and key as needed on the `wr`, `facebook_sdk` and `pandas` fields in your environment files located in `facebook-ingest/env/`.
5. Go to the `facebook-ingest` folder: `cd facebook-ingest`.
6. Install npm dependencies: `npm install`.
7.  Deploy on AWS with: `sls deploy --stage {stage}`.
   1. Substitute `{stage}` with one of the available stages defined as the YAML files in the `facebook-ingest/env/` directory.

**Note:** You can set `execute_libraries_upload` as `False` in `facebook-ingest/partials/custom.yml` to speed up the deployment if there are no updates to the libraries.

## Usage

You can start the Glue job manually from the AWS console or using any of the AWS allowed methods such as AWS CLI, AWS SDKs, etc...

There is also a triggering schedule enabled by default, described below:

## Trigger Schedule

By default, the glue job is triggered by the following rules:

- Every 55 minutes, between 6 and 20, from Mondays to Fridays
- At 10, on Saturdays and Sundays

You can change the rules on the `Glue.triggers` YAML property in the `facebook-ingest/serverless.yml` file.

## Contributing

Feel free to contribute! Create an issue and submit PRs (pull requests) in the repository. Contributing to this project assumes a certain level of familiarity with AWS, the Python language and concepts such as virtualenvs, pip, modules, etc.

Try to keep commits inside the rules of https://www.conventionalcommits.org/. The `sailr.json` file is used for configuration of the commit hook, as per: https://github.com/craicoverflow/sailr.

## License

This project is licensed under the **Apache License 2.0**.

See [LICENSE](LICENSE) for more information.

## Acknowledgements

Many thanks to the mantainers of the open source libraries used in this project:

- Serverless Framework: https://github.com/serverless/serverless
- Serverless Glue: https://github.com/toryas/serverless-glue
- Pandas: https://github.com/pandas-dev/pandas
- AWS Data Wrangler: https://github.com/awslabs/aws-data-wrangler
- Boto3 (AWS SDK for Python): https://github.com/boto/boto3
- Facebook Business SDK for Python: https://github.com/facebook/facebook-python-business-sdk
- Sailr (conventional commits git hooke): https://github.com/craicoverflow/sailr/

Contact us if we missed an acknowledgement to your library.

---

This is a project created by [Linkalab](https://linkalab.it) and [Talent Garden](https://talentgarden.org).
