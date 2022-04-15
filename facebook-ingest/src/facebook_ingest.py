# facebook_business imports
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adimage import AdImage
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adpreview import AdPreview

# Other imports
import datetime
import pandas as pd
import json
import time
import boto3
import base64
import botocore
import awswrangler as wr
import logging
import pytz
import sys
from awsglue.utils import getResolvedOptions
from typing import List, Dict, Any, Union

# Set logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get Job arguments
args = getResolvedOptions(sys.argv, ['secret_name',
                                     'data_bucket'])

# Set AWS constants and clients
S3_CLIENT = boto3.client('s3')
SECRET_MANAGER_CLIENT = boto3.client('secretsmanager')

SECRET_NAME = args['secret_name']
DATA_BUCKET = args['data_bucket']

# Set execution details constants
SOURCE = 'facebook'
ZONE = 'intake'
TIER = 'raw'
PROCESS = 'ingestion'
DUMPDATE = 'dumpdate'
PARTITION = [DUMPDATE]
TZ = pytz.timezone('Europe/Rome')

# Set Facebook fields
fields = {
    'ad_insights': [
        AdsInsights.Field.date_start,
        AdsInsights.Field.date_stop,
        AdsInsights.Field.account_id,
        AdsInsights.Field.account_name,
        AdsInsights.Field.ad_id,
        AdsInsights.Field.ad_name,
        AdsInsights.Field.adset_id,
        AdsInsights.Field.adset_name,
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.campaign_name,
        AdsInsights.Field.reach,
        AdsInsights.Field.impressions,
        AdsInsights.Field.clicks,
        AdsInsights.Field.frequency,
        AdsInsights.Field.cost_per_outbound_click,
        AdsInsights.Field.outbound_clicks,
        AdsInsights.Field.spend
    ],

    'ad': [
        Ad.Field.id,
        Ad.Field.adset_id,
        Ad.Field.campaign_id,
        Ad.Field.account_id,
        Ad.Field.configured_status,
        Ad.Field.creative,
        Ad.Field.effective_status,
        Ad.Field.priority,
        Ad.Field.date_format,
        Ad.Field.execution_options,
        Ad.Field.name,
        Ad.Field.updated_time,
        Ad.Field.created_time,
        Ad.Field.status,
        Ad.Field.targeting
    ],

    'ad_set': [
        AdSet.Field.id,
        AdSet.Field.account_id,
        AdSet.Field.adset_schedule,
        AdSet.Field.billing_event,
        AdSet.Field.budget_remaining,
        AdSet.Field.campaign_id,
        AdSet.Field.configured_status,
        AdSet.Field.created_time,
        AdSet.Field.effective_status,
        AdSet.Field.end_time,
        AdSet.Field.start_time,
        AdSet.Field.status,
        AdSet.Field.name,
        AdSet.Field.lifetime_budget,
        AdSet.Field.updated_time
    ],

    'campaign': [
        Campaign.Field.id,
        Campaign.Field.account_id,
        Campaign.Field.budget_remaining,
        Campaign.Field.buying_type,
        Campaign.Field.can_use_spend_cap,
        Campaign.Field.configured_status,
        Campaign.Field.created_time,
        Campaign.Field.daily_budget,
        Campaign.Field.name,
        Campaign.Field.effective_status,
        Campaign.Field.objective,
        Campaign.Field.pacing_type,
        Campaign.Field.start_time,
        Campaign.Field.status,
        Campaign.Field.stop_time,
        Campaign.Field.updated_time
    ],

    'ad_image': [
        AdImage.Field.id,
        AdImage.Field.account_id,
        AdImage.Field.creatives,
        AdImage.Field.hash,
        AdImage.Field.name,
        AdImage.Field.permalink_url,
        AdImage.Field.status,
        AdImage.Field.updated_time,
    ]
}


def get_credentials(secret_manager_client: 'botocore.client.SecretsManager',
                    secret_name: str) -> Dict[str, Any]:
    """
    Decrypts secret stored in AWS Secrets Manager by using the secret-name's associated KMS key.
    Depending on whether the secret is a string or binary, a Dict is returned.

    :param secret_manager_client: botocore.client.SecretsManager - SecretsManager client instance
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


def get_latest_epoch(s3_client: 'botocore.client.S3', bucket_name: str, zone: str,
                     tier: str, source: str, extraction: str) -> str:
    """
    Given a specific data process (ingestion, pseud-ingestion, refinement, ecc.), based on
    the combination of bucket name, zone, tier, source and extraction, list all metadata for
    that specific prefix. Then, grab the last modified one (that is, the last stored), open it
    and get the latest execution time. Then return this value.

    :param s3_client: botocore.client.S3 - Boto3 S3 client instance
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


def get_params(object_type: str, latest_epoch: str) -> Dict[str, Union[str, List, int]]:
    """
    This function is used to create a params body dict to pass to a Facebook API call. Params are
    used to filter the API calls.

    :param object_type: str - Data object being queried
    :param latest_epoch: str - Epoch timestamp of last executed ingestion
    :return: Dict - Dictionary containing parameters to be passed to a Facebook API call
    """

    if object_type in ['ad', 'ad_set', 'campaign']:
        params = {
            'filtering': [{'field': "updated_time",
                           'operator': "GREATER_THAN",
                           'value': latest_epoch}],
            'limit': 1000
        }
        return params

    elif object_type in ['ad_insights']:
        logger.info(f"Object type is {object_type}, creating parameters accordingly")
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        today = datetime.datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Yesterday is {yesterday}, today is {today}")

        params_yesterday = {
            'time_range': {'since': yesterday, 'until': yesterday},
            'level': 'ad',
            'limit': 1000
        }

        params_today = {
            'time_range': {'since': today, 'until': today},
            'level': 'ad',
            'limit': 1000
        }
        return params_yesterday, params_today

    else:
        params = {}
        return params


def create_validation_metadata(s3_client: 'botocore.client.S3', execution_time: int,
                               bucket_name: str, zone: str, tier: str, source: str, extraction: str,
                               **kwargs) -> None:
    """
    Pass all required arguments, plus all keywords arguments to a dictionary, convert it to JSON
    and dump it to S3 based on the combination of 'metadata' and bucket_name, zone, tier, source,
    extraction, and execution_time.

    :param s3_client: botocore.client.S3 - Boto3 S3 client instance
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


def adjust_ad_image_data(df: pd.DataFrame, latest_epoch: str) -> pd.DataFrame:
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


def get_preview_url(df: pd.DataFrame) -> pd.DataFrame:
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
            ad_preview = ad.get_previews(
                params={
                    'ad_format': ad_format
                }
            )

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


def get_objects(object_type: str, account_id: AdAccount, fields: Dict[str, List[str]],
                params: Dict[str, Union[str, List, int]]) -> pd.DataFrame:
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
        async_job = account_id.get_insights(params=params, fields=fields[object_type],
                                            is_async=True)
        async_job.api_get()
        while async_job[AdReportRun.Field.async_status] != 'Job Completed':
            time.sleep(3)
            async_job.api_get()
        objects = async_job.get_result(params={"limit": 1000})

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


def sink(dataframe: pd.DataFrame, execution_time: int, bucket_name: str, zone: str, tier: str,
         source: str, extraction: str, partition_columns: List[str], process: str,
         fields: List[str]) -> None:
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
    """
    def sink_(bucket_name: str, zone: str, tier: str, source: str, extraction: str,
              partition_columns: List, dataframe: pd.DataFrame):

        logger.info(f"Sinking parquet to: s3://{bucket_name}/{zone}/{tier}/{source}/{extraction}/")

        wr.s3.to_parquet(
            df=dataframe,
            path=f"s3://{bucket_name}/{zone}/{tier}/{source}/{extraction}/",
            dataset=True,
            partition_cols=partition_columns,
            mode='append',
            schema_evolution=True,
            database=tier,
            table=f"t_{source}_{extraction}"
        )

    n_rows = len(dataframe)
    n_fields = len(dataframe.columns)

    if n_rows > 0:
        dataframe[DUMPDATE] = execution_time
        dataframe = dataframe.astype(str)
        logger.info(f"Sinking {source}/{extraction}, partition by: {partition_columns}")
        sink_(bucket_name, zone, tier, source, extraction, partition_columns, dataframe)
        create_validation_metadata(s3_client=S3_CLIENT, execution_time=execution_time,
                                   bucket_name=bucket_name, zone=zone, tier=tier,
                                   source=source, extraction=extraction, n_rows=n_rows,
                                   fields=fields, process=process, n_fields=n_fields)

        logger.info(f"Sinking for {source}/{extraction} completed")
    else:
        logger.info(f"Got nothing to ingest for {source}/{extraction}")


# Retrieve credentials
logger.info(f"Retrieving credentials: {SECRET_NAME}")
credentials = get_credentials(secret_manager_client=SECRET_MANAGER_CLIENT, secret_name=SECRET_NAME)

APP_ID = credentials['FB_APP_ID']
APP_SECRET = credentials['FB_APP_SECRET']
ACCESS_TOKEN = credentials['FB_ACCESS_TOKEN']
BUSINESS_ID = credentials['FB_BUSINESS_ID']

# Start the connection to the facebook API
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# All object type list
object_type_list = ['ad', 'ad_set', 'campaign', 'ad_insights']

# If sunday, add ad_image to object_type list
if datetime.date.today().weekday() == 6:
    object_type_list.append('ad_image')

# Get all ad_accounts system user has access to
system_user = User(fbid='me')
accounts = list(system_user.get_ad_accounts(fields=[AdAccount.Field.id, AdAccount.Field.name]))

# Query one object_type at a time for all ad accounts.
# If the query returns results, sink the data and sleep for 30 seconds
# Else, move to the next object_type
for i, object_type in enumerate(object_type_list):

    # Go through this if statement only after first loop
    if i > 0:
        if len(df) > 0:
            logger.info("Sleeping for 60 seconds")
            time.sleep(60)

    # Set df to be sinked
    df = pd.DataFrame(columns=fields[object_type])

    # Set execution time of the process
    EXECUTION_TIME = int(datetime.datetime.now(tz=TZ).timestamp())
    logger.info(f"Querying {object_type}, execution_time: {EXECUTION_TIME}")

    # Get last execution time for object type
    try:
        latest_epoch = get_latest_epoch(s3_client=S3_CLIENT, bucket_name=DATA_BUCKET, zone=ZONE,
                                        tier=TIER, source=SOURCE, extraction=object_type)
    except KeyError as e:
        latest_epoc = "2022-04-14 00:00:00"
        logger.info(f"#: Get latest_epoch: {latest_epoc}")
        latest_epoch = str(int(datetime.datetime.strptime(latest_epoc, '%Y-%m-%d %H:%M:%S').timestamp()))

    # Get parameters to be passed to the API
    if object_type == 'ad_insights':
        params_yesterday, params_today = get_params(object_type=object_type, latest_epoch=latest_epoch)
        logger.info(f"These are the (yesterday) passed params: {params_yesterday}")
        logger.info(f"These are the (today) passed params: {params_today}")
    else:
        params = get_params(object_type=object_type, latest_epoch=latest_epoch)
        logger.info(f"These are the passed params: {params}")

    # Loop through ad accounts
    for account in accounts:
        time.sleep(5)
        tempaccount = AdAccount(account[AdAccount.Field.id])
        logger.info(f"Querying {object_type} objects of {account['name']}, id: {account['id']}")

        # Get data for object type of the ad account and append it to the df to be sinked
        if object_type == 'ad_insights':
            logger.info("Querying yesterday's data for ad_insights")
            df = df.append(get_objects(object_type=object_type,
                                       account_id=tempaccount,
                                       fields=fields,
                                       params=params_yesterday)).reset_index(drop=True)
            logger.info("Querying today's data for ad_insights")
            df = df.append(get_objects(object_type=object_type,
                                       account_id=tempaccount,
                                       fields=fields,
                                       params=params_today)).reset_index(drop=True)
        else:
            df = df.append(get_objects(object_type=object_type,
                                       account_id=tempaccount,
                                       fields=fields,
                                       params=params)).reset_index(drop=True)

    # Sink df containing all data of all ad account of the one object type at hand
    df.pipe(sink, execution_time=EXECUTION_TIME, bucket_name=DATA_BUCKET, zone=ZONE,
            tier=TIER, source=SOURCE, extraction=object_type, partition_columns=PARTITION,
            process=PROCESS, fields=fields[object_type])

logger.info("I'm done")
