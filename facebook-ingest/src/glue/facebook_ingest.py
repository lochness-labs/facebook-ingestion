# Import all the facebook mumbo jumbo
from asyncio.log import logger
from distutils.log import error
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adset import AdSet  # !
from facebook_business.adobjects.campaign import Campaign  # !
from facebook_business.adobjects.adcreative import AdCreative  # !
from facebook_business.adobjects.adimage import AdImage
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adpreview import AdPreview

# Import all the python mumbo jumbo
import datetime
import pandas as pd
import json
import time
import boto3
import base64
import awswrangler as wr
import logging
import pytz
import sys
import yaml
from typing import List, Dict, Any, Union

# Import getResolvedOptions if on Glue environment
try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    print("Local run...")


# Set AWS constants and clients
S3_CLIENT = boto3.client('s3')
GLUE_CLIENT = boto3.client('glue')
SECRET_MANAGER_CLIENT = boto3.client('secretsmanager')

# Set execution details constants # TODO/FEAT/Configuration.
SOURCE = 'facebook'
ZONE = 'intake'
TIER = 'raw'
PROCESS = 'ingestion'

TZ = pytz.timezone('Europe/Rome')  # TODO/FEAT/Configuration.TZ


def get_logger():
    """
    Get the right logger configuration (thanks to https://stackoverflow.com/a/63361324)

    TODO/FEAT/GenericWHL: This is a generic function and should be moved into a custom WHL
    """

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


logger = get_logger()


def get_credentials(secret_manager_client, secret_name):
    """
    Decrypts secret stored in AWS Secrets Manager by using the secret-name's associated KMS key.
    Depending on whether the secret is a string or binary, a Dict is returned.

    TODO/FEAT/GenericWHL: This is a generic function and should be moved into a custom WHL

    :param secret_manager_client: botocore.client.Client - SecretsManager client instance
    :param secret_name: - Name of the secret as saved in AWS
    :return: Dict[str, object] - Dict containing object stores in SecretsManager
    """

    secret_response = secret_manager_client.get_secret_value(SecretId=secret_name)

    if 'SecretString' in secret_response:
        secret = secret_response['SecretString']
        return json.loads(secret)
    else:
        decoded_binary_secret = base64.b64decode(secret_response['SecretBinary'])
        return json.loads(decoded_binary_secret)


def get_latest_epoch(s3_client, bucket_name, zone, tier, source, extraction):
    """
    Given a specific data process (ingestion, pseud-ingestion, refinement, ecc.), based on
    the combination of bucket name, zone, tier, source and extraction, list all metadata for
    that specific prefix. Then, grab the last modified one (that is, the last stored), open it
    and get the latest execution time. Then return this value.

    :param s3_client: botocore.client.Client - Boto3 S3 client instance
    :param bucket_name: str - Name of the bucket containing the metadata
    :param zone: str - Name of the zone of the data process
    :param tier: str - Name of the tier of the data process
    :param source: str - Name of the source involved in the data process
    :param extraction: str - Name of the extraction involved in the data process
    :return: str - Last execution epoch timestamp up to seconds
    """

    def get_last_modified(obj): return int(obj['LastModified'].strftime('%s'))
    prefix = f"metadata/{zone}/{tier}/{source}/{extraction}/"

    objs = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']

    last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][-1]
    logger.info(f"Last added key: {last_added}")

    response = s3_client.get_object(Bucket=bucket_name, Key=last_added)

    content = json.loads(response['Body'].read().decode('utf-8'))
    latest_epoch = content['Execution Overview']['execution_time']

    logger.info(f"Latest epoch for {source}/{extraction} is: {latest_epoch}")
    return latest_epoch


def get_params(object_type, latest_epoch, history):
    """
    This function is used to create a params body dict to pass to a Facebook API call. Params are
    used to filter the API calls.

    :param object_type: str - Data object being queried
    :param latest_epoch: str - Epoch timestamp of last executed ingestion
    :param history: bool - Enable history ingestion for ad_insights
    :return: Dict - Dictionary containing parameters to be passed to a Facebook API call
    """

    logger.info(f"Object type is {object_type}, creating parameters accordingly...")

    if object_type in ['ad', 'ad_set', 'campaign']:
        params = {
            'filtering': [{
                'field': "updated_time",
                'operator': "GREATER_THAN",
                'value': latest_epoch
            }],
            'limit': 1000
        }
        return params

    elif object_type in ['ad_insights']:
        today = datetime.datetime.now().strftime("%Y-%m-%d")  # today
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")  # yesterday

        if history:
            since = datetime.datetime.fromtimestamp(int(latest_epoch)).isoformat().split('T')[0]  # default value
        else:
            since = yesterday

        logger.info(f"Since {since} -> until {today}")

        params_since = {
            'time_range': {'since': since, 'until': yesterday},
            'level': 'ad',
            'limit': 1000
        }

        params_until = {
            'time_range': {'since': today, 'until': today},
            'level': 'ad',
            'limit': 1000
        }

        return params_since, params_until

    else:
        params = {}
        return params


def create_validation_metadata(
        s3_client, execution_time, bucket_name, zone, tier, source, extraction, **kwargs) -> None:
    """
    Pass all required arguments, plus all keywords arguments to a dictionary, convert it to JSON
    and dump it to S3 based on the combination of 'metadata' and bucket_name, zone, tier, source,
    extraction, and execution_time.

    TODO/FEAT/GenericWHL: This is a generic function and should be moved into a custom WHL

    :param s3_client: S3 - Boto3 S3 client instance
    :param execution_time: int - Execution time of present process
    :param bucket_name: str - Bucket name for the metadata
    :param zone: str - Zone of the present process
    :param tier: str - Tier of the present process
    :param source: str - Source of the present process
    :param extraction: str -  Extraction of the present process
    """

    kwargs['execution_time'] = execution_time
    kwargs['bucket_name'] = bucket_name
    kwargs['zone'] = zone
    kwargs['tier'] = tier
    kwargs['extraction'] = extraction
    json_data = {'Execution Overview': kwargs}
    key = f"metadata/{zone}/{tier}/{source}/{extraction}/dumpdate={execution_time}/metadata.json"

    logger.info(f"Creating validation metadata in: {key}")

    s3_client.put_object(Bucket=bucket_name,
                         Body=(bytes(json.dumps(json_data, default=str).encode('UTF-8'))),
                         Key=key)

    logger.info(f"Succesfully uploaded metadata to s3://{bucket_name}/{key}")


def adjust_ad_image_data(df, latest_epoch):
    """
    This function flattens ad_image data on 'creatives' column. It then proceeds to filter the data
    so to keep only the entries with an 'updated_timestamp' greater then last ingestion execution
    epoch - due to the fact that ad_image object cannot be filter at the moment of the API call.
    Return the adjusted DataFrame.

    :param df: pd.DataFrame - Dataframe containing ad_image data
    :param latest_epoch: str - Last ingestion execution epoch timestamp up to seconds
    :return: pd.DataFrame - Adjusted pandas DataFrame
    """

    def _sanitize_timestamp(timestamp: str) -> int:
        return int(datetime.datetime.strptime(timestamp[:-5], "%Y-%m-%dT%H:%M:%S").timestamp())

    df = df.explode('creatives')
    df['updated_time_timestamp'] = df['updated_time'].apply(_sanitize_timestamp)
    df = df.loc[df['updated_time_timestamp'] >= int(latest_epoch)]
    df = df.drop('updated_time_timestamp', 1)

    return df


def get_preview_url(df):
    """
    Get the AD preview URL

    :param df: pd.DataFrame - Dataframe
    :return: pd.DataFrame - Adjusted DataFrame
    """

    id_list = df['id'].unique().tolist()

    for ad_id in id_list:

        time.sleep(1.5)

        ad_id = str(ad_id)
        publisher = df.loc[df['id'] == ad_id]['publisher_platforms'].max(
        )[0] if df.loc[df['id'] == ad_id]['publisher_platforms'].max() != '' else ''

        if publisher != '':
            if publisher == 'instagram' and df.loc[df['id'] == ad_id]['instagram_positions'].max() != '':
                position = df.loc[df['id'] == ad_id]['instagram_positions'].max()[0]
                if position == 'story':
                    ad_format = AdPreview.AdFormat.instagram_story
                else:
                    ad_format = AdPreview.AdFormat.instagram_standard
            elif publisher == 'facebook' and df.loc[df['id'] == ad_id]['facebook_positions'].max() != '':
                position = df.loc[df['id'] == ad_id]['facebook_positions'].max()[0]
                if position == 'story':
                    ad_format = AdPreview.AdFormat.facebook_story_mobile
                else:
                    ad_format = AdPreview.AdFormat.desktop_feed_standard
            else:
                ad_format = AdPreview.AdFormat.desktop_feed_standard

            ad = Ad(ad_id)
            ad_preview = ad.get_previews(params={
                'ad_format': ad_format
            })

            for p in ad_preview:
                preview = p["body"]
                preview = str(preview)

            preview = preview.replace("amp;", "")
            preview = (preview.split('src="')[1].split('" width')[0])
            preview = preview.replace(';t=', '&t=')
            df.loc[df['id'] == ad_id, 'preview_url'] = preview

        else:
            df.loc[df['id'] == ad_id, 'preview_url'] = "not_available"

    return df


def get_objects(object_type, account_id, fields, params, latest_epoch=None):
    """
    This function makes API calls to Facebook in order to retrieve data. Every Facebook's
    data object has it's own method. Due to the amount of data, the call to ad_insights
    is asynchronous. If the API call returns something, parse the response and add it to a
    pandas DataFrame. In the case of ad_image data object, adjust the data as well. Return
    the pandas DataFrame.

    :param object_type: str - Name of one of the data object
    :param account_id: AdAccount - Object of Facebook AdAccount class
    :param fields: Dict - Constant dict of the type {object_type: [List_of_fields]}
    :param params: Dict - Parameters to be passed to the API call
    :param latest_epoch: int (default None) - last execution time
    :return: pd.DataFrame - Pandas dataframe containing the data of the passed object_type
    """

    if object_type == 'ad':
        objects = account_id.get_ads(params=params, fields=fields[object_type])
    elif object_type == 'ad_set':
        objects = account_id.get_ad_sets(params=params, fields=fields[object_type])
    elif object_type == 'campaign':
        objects = account_id.get_campaigns(params=params, fields=fields[object_type])
    elif object_type == 'ad_image':
        objects = account_id.get_ad_images(params=params, fields=fields[object_type])
    elif object_type == 'ad_insights':
        async_job = account_id.get_insights(params=params, fields=fields[object_type], is_async=True)
        async_job.api_get()
        while async_job[AdReportRun.Field.async_status] != 'Job Completed':
            time.sleep(3)
            async_job.api_get()
        objects = async_job.get_result(params={"limit": 1000})
    else:
        raise ValueError(f'#: Object_type not mapped!')

    storing_dataframe = pd.DataFrame(columns=fields[object_type])

    for object in objects:
        time.sleep(0.5)
        values_holder = {}
        for field in fields[object_type]:
            key = field
            if field == 'creative':
                value = object[field]['id'] if field in object else ''
            elif field == 'pacing_type':
                value = object[field][0] if field in object else ''
            elif field in ['cost_per_outbound_click', 'outbound_clicks']:
                value = object[field][0]['value'] if field in object else ''
            elif field == 'targeting':
                value = object[field]
                for internal_key in object[field].keys():
                    if 'publisher_platforms' in object[field].keys():
                        internal_value = object[field][internal_key]
                        values_holder[internal_key] = internal_value
                    else:
                        values_holder['publisher_platforms'] = ''

                    if 'instagram_positions' in object[field].keys():
                        internal_value = object[field][internal_key]
                        values_holder[internal_key] = internal_value
                    else:
                        values_holder['instagram_positions'] = ''

                    if 'facebook_positions' in object[field].keys():
                        internal_value = object[field][internal_key]
                        values_holder[internal_key] = internal_value
                    else:
                        values_holder['facebook_positions'] = ''

                    if 'device_platforms' in object[field].keys():
                        internal_value = object[field][internal_key]
                        values_holder[internal_key] = internal_value
                    else:
                        values_holder['device_platforms'] = ''
            else:
                value = object[field] if field in object else ''

            values_holder[key] = value

        df_ = pd.DataFrame([values_holder], index=[0])

        if object_type == 'ad_image' and len(df_) > 0:
            logger.info("Adjusting ad images data")
            df_ = adjust_ad_image_data(df=df_, latest_epoch=latest_epoch)

        elif object_type == 'ad' and len(df_) > 0:
            df_ = get_preview_url(df=df_)

        storing_dataframe = storing_dataframe.append(df_)

    return storing_dataframe


def sink(dataframe, execution_time, bucket_name, zone, tier, source, extraction,
         partition_columns, process, fields, table_version, account_id) -> None:
    """
    Check if the passed DataFrame has > 0 rows. If so, add partition columns, sink the data in S3
    and generate a validation metadata.json.

    :param dataframe: pd.DataFrame - Pandas dataframe containing GoogleAnalytics data
    :param execution_time: int - Execution time of present process
    :param bucket_name: str - Bucket name for the data
    :param zone: str - Zone of the present process
    :param tier: str - Tier of the present process
    :param source: str - Source of the present process
    :param extraction: str -  Extraction of the present process
    :param partition_columns: List - List of the columns sinking data needs to be partitioned on
    :param process: str - Name of the data workflow process at hand
    :param table_version: str - Version of the Athena table
    """

    def sink_(bucket_name: str, zone: str, tier: str, source: str, extraction: str,
              partition_columns: List, dataframe: pd.DataFrame, table_version: str, account_id: str):

        s3_out_path = f"s3://{bucket_name}/{zone}/{tier}/{source}/{extraction}/"
        logger.info(f"Sinking parquet to: {s3_out_path}")

        wr.s3.to_parquet(
            df=dataframe,
            path=s3_out_path,
            dataset=True,
            partition_cols=partition_columns,
            mode='append',  # TODO/FEAT/Configuration.
            schema_evolution=True,
            database=tier,
            table=f"t_{source}_{extraction}_{table_version}"
        )

    n_rows = len(dataframe)
    n_fields = len(dataframe.columns)

    if n_rows > 0:
        dataframe[partition_columns[0]] = execution_time  # dumpdate partition
        if len(partition_columns) > 1:
            dataframe[partition_columns[1]] = account_id

        dataframe = dataframe.astype(str)
        logger.info(f"Sinking {source}/{extraction}, partition by: {partition_columns}")
        sink_(bucket_name, zone, tier, source, extraction, partition_columns, dataframe, table_version, account_id)
        create_validation_metadata(s3_client=S3_CLIENT, execution_time=execution_time,
                                   bucket_name=bucket_name, zone=zone, tier=tier,
                                   source=source, extraction=extraction, n_rows=n_rows,
                                   fields=fields, process=process, n_fields=n_fields)

        logger.info(f"Sinking for {source}/{extraction} completed")
    else:
        logger.info(f"Got nothing to ingest for {source}/{extraction}")


def get_config_from_s3(s3_bucket, conf_file_s3_key):
    """
    Retrieves the configuration file from S3

    :param s3_bucket: str - Bucket name
    :param conf_file_s3_key: str - S3 path
    """

    logger.info(conf_file_s3_key)
    logger.info(s3_bucket)

    response = S3_CLIENT.get_object(Bucket=s3_bucket, Key=conf_file_s3_key)
    try:
        configfile = yaml.safe_load(response["Body"])
        return configfile
    except yaml.YAMLError as exc:
        raise exc


def get_fb_objects(field_keys):
    """
    Retrieves FB objects from a dictionary of values.

    :param field_keys: Dict - The given configuration of fields per object.
    :return: List, Dict - List of object names and Dictionary of the mappaing of fields-object
    """

    ObjectToApply = {
        'ad_insights': AdsInsights.Field,
        'ad': Ad.Field,
        'ad_set': AdSet.Field,
        'campaign': Campaign.Field,
        'ad_image': AdImage.Field,
        'ad_creatives': AdCreative.Field
    }

    fields_list = []  # List with object names
    fields = {}  # object -> [fields]
    for k in field_keys:
        fields_list.append(k)
        fields.update({
            k: [getattr(ObjectToApply[k], item) for item in field_keys[k]]
        })

    return fields_list, fields


def main(event):

    default_latest_epoch_date = event['latest_epoch']
    default_latest_epoch = str(int(datetime.datetime.strptime(default_latest_epoch_date, '%Y-%m-%d %H:%M:%S').timestamp()))
    data_bucket = event['s3_data_bucket']
    code_bucket = event['s3_code_bucket']
    s3_key_conf_file = event['s3_key_conf_file']

    if 'fb_secret_name' in event and event['fb_secret_name'] != 'none':
        sm_name = event['fb_secret_name']

        logger.info(f"#: Using secret_name {sm_name}...")

        credentials = get_credentials(secret_manager_client=SECRET_MANAGER_CLIENT, secret_name=sm_name)

        # Start the connection to the facebook API
        if 'FB_APP_ID' in credentials and 'FB_APP_SECRET' in credentials and 'FB_ACCESS_TOKEN' in credentials:
            FacebookAdsApi.init(
                credentials['FB_APP_ID'],
                credentials['FB_APP_SECRET'],
                credentials['FB_ACCESS_TOKEN']
            )
        elif 'long_live_user_token' in credentials:
            FacebookAdsApi.init(access_token=credentials['long_live_user_token'])
        else:
            raise ValueError("Fatal Error: uncorrect credentials from Secret Manager")

    elif 'long_live_user_token' in event and event['long_live_user_token'] != 'none':
        account_id = event['account_id']
        long_token = event['long_live_user_token']

        logger.info(f"#: Using long_live_user_token for account id {account_id}...")

        # Start the connection to the facebook API using only the long token
        FacebookAdsApi.init(access_token=long_token)

    else:
        raise ValueError("Fatal Error: FacebookAdsApi.init failed.")

    # Retrieve configuration file
    conf_data = get_config_from_s3(s3_bucket=code_bucket, conf_file_s3_key=s3_key_conf_file)

    # Set partitions
    partition_columns = ['dumpdate']
    try:
        partition_by_account_id = conf_data['partition_by_account_id']
        if partition_by_account_id:
            partition_columns.append('account_id')
    except:
        pass

    table_version = conf_data['table_version']

    fields_list, fields = get_fb_objects(conf_data['field_keys'])

    try:
        # All object type list # TODO/FEAT/MoreGeneric
        object_type_list = ['ad', 'ad_set', 'campaign', 'ad_insights']

        # If sunday, add ad_image to object_type list
        if datetime.date.today().weekday() == 6:  # TODO/FEAT/MoreGeneric
            object_type_list.append('ad_image')

        # Get all ad_accounts system user has access to
        system_user = User(fbid='me')
        accounts = list(system_user.get_ad_accounts(fields=[AdAccount.Field.id, AdAccount.Field.name]))

        # Query one object_type at a time for all ad accounts. If the query returns results,
        # sink the data and sleep for 30 seconds. Else, move to the next object_type
        df = pd.DataFrame()
        for i, object_type in enumerate(object_type_list):
            if object_type not in fields_list:
                logger.info(f'#: Object_type "{object_type}" not in fields_list {fields_list}')
                continue

            logger.info(f'#: Working with object: {object_type}')

            # Go through this if statement only after first loop
            if i > 0 and len(df) > 0:
                # TODO/DoubleCheck: does it really have to be hardcoded to 60?
                logger.info("#: Sleeping for 60 seconds...")
                time.sleep(60)

            # Set df to be sinked
            df = pd.DataFrame(columns=fields[object_type])

            # Set execution time of the process
            EXECUTION_TIME = int(datetime.datetime.now(tz=TZ).timestamp())
            logger.info(f"#: Querying {object_type}, execution_time: {EXECUTION_TIME}")

            history = False

            # Get last execution time for object type
            try:
                latest_epoch = get_latest_epoch(s3_client=S3_CLIENT, bucket_name=data_bucket, zone=ZONE,
                                                tier=TIER, source=SOURCE, extraction=object_type)
            except:
                # This is the first execution, so we'll start from a default value
                # and we'll grub the whole history of data
                logger.info(f"#: Get latest_epoc: {default_latest_epoch_date}")
                latest_epoch = default_latest_epoch
                history = True

            # Get parameters to be passed to the API
            if object_type == 'ad_insights':
                logger.info(f'#: History ad_insights value: {history}')
                params_yesterday, params_today = get_params(
                    object_type=object_type, latest_epoch=latest_epoch, history=history)
                logger.info(f"Yesterday params: {params_yesterday}\nToday params {params_today}")
            else:
                # TODO/DoubleCheck: what about the history of other types?
                params = get_params(object_type=object_type, latest_epoch=latest_epoch, history=False)
                logger.info(f"These are the passed params: {params}")

            # Loop through ad accounts
            for account in accounts:
                time.sleep(5)  # TODO/DoubleCheck: does it really have to be hardcoded to 5?

                ad_account_id = AdAccount(account[AdAccount.Field.id])
                logger.info(f"Querying {object_type} objects of {account['name']}, id: {account['id']}")

                # Get data for object type of the ad account and append it to the df to be sinked
                if object_type == 'ad_insights':
                    logger.info("Querying yesterday's data for ad_insights")
                    df = df.append(get_objects(object_type=object_type,
                                               account_id=ad_account_id,
                                               fields=fields,
                                               params=params_yesterday)).reset_index(drop=True)
                    logger.info("Querying today's data for adinsights")
                    df = df.append(get_objects(object_type=object_type,
                                               account_id=ad_account_id,
                                               fields=fields,
                                               params=params_today)).reset_index(drop=True)
                else:
                    df = df.append(get_objects(object_type=object_type,
                                               account_id=ad_account_id,
                                               fields=fields,
                                               params=params,
                                               latest_epoch=latest_epoch)).reset_index(drop=True)

            # Sink df containing all data of all ad account of the one object type at hand
            df.pipe(sink,
                    execution_time=EXECUTION_TIME,
                    bucket_name=data_bucket,
                    zone=ZONE,
                    tier=TIER,
                    source=SOURCE,
                    extraction=object_type,
                    partition_columns=partition_columns,
                    process=PROCESS,
                    fields=fields[object_type],
                    table_version=table_version,
                    account_id=account['id']
                    )

        logger.info("I'm done")

    except Exception as e:
        # TODO/FEAT/FlowControl: Maybe handle errors on S3 or elsewhere
        logger.error(e)
        raise e


if __name__ == "__main__":
    try:
        event = getResolvedOptions(sys.argv, [
            'account_id',
            'long_live_user_token',
            'latest_epoch',
            's3_data_bucket',
            's3_code_bucket',
            's3_key_conf_file',
            'fb_secret_name'
        ])
    except NameError:
        # Fallback event for local development (outside Glue environment)
        event = {
            # * -> HERE YOU CAN CHANGE THE DATE FOR TESTING
            'latest_epoch': '2022-01-01 00:00:00',
            's3_data_bucket': 'your-data-bucket-name',
            's3_code_bucket': 'your-code-bucket-name',
            'account_id': 'act_1234567891011',
            'long_live_user_token': 'ABC123def....',
            'fb_secret_name': 'null'
        }

    logger.info(f'#: Event: {event}')
    main(event)
