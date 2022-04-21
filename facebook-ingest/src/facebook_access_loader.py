#!/usr/bin/python3
# -*- coding: utf-8 -*-
import os
import requests
import logging

# Environment variables
FB_SECRETS_URL = os.getenv('fb_secrets_url')
FB_SECRET_NAME = os.getenv('fb_secret_name')
FB_USE_SECRET = os.getenv('fb_use_secret')
JOB_NAME = os.getenv('job_name')
S3_DATA_BUCKET = os.getenv('s3_data_bucket')
S3_CODE_BUCKET = os.getenv('s3_code_bucket')
LATEST_EPOCH = os.getenv('latest_epoch')
S3_KEY_CONF_FILE = os.getenv('s3_key_conf_file')


def get_logger():
    """
    Get logger suppressing some boto info.

    TODO/FEAT/GenericLayer: This is a generic function and should be moved into a custom layer
    """
    logger = logging.getLogger()
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)
    logging.basicConfig(format='%(message)s', level=logging.INFO)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)

    return logger


logger = get_logger()


def handler(event=None, context=None):

    try:
        out_event = []

        logger.info(f'FB_SECRET_NAME: ' + str(FB_SECRET_NAME))
        logger.info(f'FB_USE_SECRET: ' + str(FB_USE_SECRET))

        if FB_USE_SECRET:
            logger.info(f'#: Run glue job with credential from SM')
            out_event.append({
                'latest_epoch': LATEST_EPOCH,
                's3_data_bucket': S3_DATA_BUCKET,
                's3_code_bucket': S3_CODE_BUCKET,
                'resource_name': JOB_NAME,
                's3_key_conf_file': S3_KEY_CONF_FILE,
                'fb_secret_name': FB_SECRET_NAME,
                'account_id': 'null',
                'long_live_user_token': 'null',
            })

        elif FB_SECRETS_URL is not None:
            logger.info(f'#: Run glue job with multiple credentials')

            data = requests.get(FB_SECRETS_URL)
            if data.status_code == 200:
                data_json = data.json()

                # logger.info(data_json)
                logger.info(f"#: Get {len(data_json['result'])} users.")

                for item in data_json['result']:
                    out_event.append({
                        'latest_epoch': LATEST_EPOCH,
                        's3_data_bucket': S3_DATA_BUCKET,
                        's3_code_bucket': S3_CODE_BUCKET,
                        'resource_name': JOB_NAME,
                        's3_key_conf_file': S3_KEY_CONF_FILE,
                        'account_id': item['ad_accont_id'],
                        'long_live_user_token': item['long_live_user_token'],
                        'fb_secret_name': 'null'
                    })

            else:
                raise ValueError(f'# Request Error: {data.status_code}')

        else:
            raise ValueError(f'# Request Error: required FB_SECRET_NAME or FB_SECRETS_URL')

        return out_event

    except Exception as e:
        logger.error('#: %s' % e, exc_info=True)
        raise e


if __name__ == "__main__":
    handler(event=None, context=None)
