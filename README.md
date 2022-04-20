# Facebook Ads Ingestion for Data Lakes

A Data Lake ingestion connector for the Facebook Ads APIs

The main component of the repository is an AWS Glue job, of type `pythonshell`, which uses the Facebook Marketing API to retrieve `ad`, `ad_set`, `campaign`, `ad_image`, and `ad_insights` (also known as facebook's `extractions`) data for all advertising account under your business account on Facebook. For each API object, the job retrieves the last execution time, and gets all updated/new data since then. It then proceeds to store data with AWS Data Wrangler which sinks the data in S3 and generates a validation metadata file. The glue job is deployed by the Serverless Framework Stack and the script is located here: `facebook-ingest/src/facebook_ingest.py`.

The infrastructure is described (IaC) and deployed with Serverless Framework (https://www.serverless.com/framework/). The entry point is `facebook-ingest/serverless.yml`.

The infrastructure has been developed on the AWS Cloud Platform.

## Getting Started

### Requirements

- Node.js and NPM: https://nodejs.org/en/download/
- Serverless Framework: https://www.serverless.com/framework/docs/getting-started/

#### For local development only

- Python: https://www.python.org/downloads/
- virtualenv: https://virtualenv.pypa.io/en/latest/installation.html

### Environments setup

The `facebook-ingest/env/` contains the environment configuration files, one for each of your AWS environments.

The name of the files corresponds to the environment names. For example: substitute `example_enviroment.yml` with `dev.yml` for a development environment.

### Development environment setup

1. Create virtualenv: `virtualenv -p python3 venv`
2. Activate virtualenv: `source venv/bin/activate`
3. Install requirements: `pip install -r requirements.txt`

### Deployment instructions

1. You need two AWS S3 buckets, one for the glue code and one as the Data Lake, if you have them, just keep in mind the names for the nexts steps, otherwise create the buckets on S3.

2. Make a copy of `facebook-ingest/env/example-environment.yml`, name it as your desired environment's name (for example `dev.yml` or `prod.yml`) and substitute:

   - `000000000000` with your aws account Id.
   - `example-data-s3-bucket-name` for your data lake AWS S3 bucket.
   - `example-code-s3-bucket-name` for your code AWS S3 bucket.
   - `eu-west-1` with your AWS region.

3. Make a secret on AWS Secrets Manager for your Facebook access token and save its name on the `secret_name` field in your environment files located in `facebook-ingest/env/`.

   - For example, we named it `accessToken-appId-appSecret-businessId/facebookApi/ingestion`.

4. Alternatively to 3, it is possible to use an external API to retrieve a list of secrets

5. Check and substitute s3 bucket and key as needed on the `wr`, `facebook_sdk` and `pandas` fields in your environment files located in `facebook-ingest/env/`.

6. Go to the `facebook-ingest` folder: `cd facebook-ingest`.

7. Install npm dependencies: `npm install`.

8.  Deploy on AWS with: `sls deploy --stage {stage}`.
   1. Substitute `{stage}` with one of the available stages defined as the YAML files in the `facebook-ingest/env/` directory.

**Note:** You can set `execute_libraries_upload` as `False` in `facebook-ingest/serverless-parts/custom.yml` to speed up the deployment if there are no updates to the libraries.

## Usage

You can start the Glue job manually from the AWS console or using any of the AWS allowed methods such as AWS CLI, AWS SDKs, etc...

There is also a triggering schedule enabled by default, described below:

## Trigger Schedule

By default, the glue job is triggered by the rule(s) defined inside `serverless-parts/stepFunctions.yml` on the `Glue.triggers` YAML property.

## Contributing

Feel free to contribute! Create an issue and submit PRs (pull requests) in the repository. Contributing to this project assumes a certain level of familiarity with AWS, the Python language and concepts such as virtualenvs, pip, modules, etc.

Try to keep commits inside the rules of https://www.conventionalcommits.org/. The `sailr.json` file is used for configuration of the commit hook, as per: https://github.com/craicoverflow/sailr.

## License

This project is licensed under the **Apache License 2.0**.

See [LICENSE](LICENSE) for more information.

## Acknowledgements

Many thanks to the mantainers of the open source libraries used in this project:

- Serverless Framework: https://github.com/serverless/serverless
- Pandas: https://github.com/pandas-dev/pandas
- AWS Data Wrangler: https://github.com/awslabs/aws-data-wrangler
- Boto3 (AWS SDK for Python): https://github.com/boto/boto3
- Facebook Business SDK for Python: https://github.com/facebook/facebook-python-business-sdk
- Sailr (conventional commits git hooke): https://github.com/craicoverflow/sailr/

### Serverless plugins

These are the Serverless plugin used on this project:

- serverless-step-functions: https://github.com/serverless-operations/serverless-step-functions
- serverless-plugin-log-retention: https://github.com/ArtificerEntertainment/serverless-plugin-log-retention
- serverless-glue: https://github.com/toryas/serverless-glue
- serverless-s3-sync: https://github.com/k1LoW/serverless-s3-sync

Contact us if we missed an acknowledgement to your library.

---

This is a project created by [Linkalab](https://linkalab.it) and [Talent Garden](https://talentgarden.org).
