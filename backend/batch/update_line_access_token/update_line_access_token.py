import os
import logging
import json
import requests
from datetime import (datetime, timedelta)
from dateutil.tz import gettz

from common import common_const as const
from common.channel_access_token import ChannelAccessToken

# Environmental variables
LOGGER_LEVEL = os.environ.get("LOGGER_LEVEL")
# Configuration for log output
logger = logging.getLogger()
if LOGGER_LEVEL == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

# Initialization of the table operation class
channel_access_token_table_controller = ChannelAccessToken()


def update_limited_channel_access_token(channel_id, channel_access_token):  # noqa 501
    """
    Update the short-term channel access token for the specified channel ID
    Parameters
    table : dynamoDB.Table
        The table object of dynamoDB
    key :string
    itemDict : dict
        New item

    Returns
    -------
    None
    """
    now = datetime.now(gettz('Asia/Tokyo'))
    # Set the expiration to 20 days from acquisition
    limit_date = (now + timedelta(days=20)).strftime('%Y-%m-%d %H:%M:%S%z')

    channel_access_token_table_controller.update_item(channel_id,
                                                      channel_access_token,
                                                      limit_date)


def get_channel_access_token(channel_id, channel_secret):
    """
    Obtain a new short-term channel access token for the MINI app

    Returns
    -------
    str
        access_token: short-term channel access token
    """

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    body = {
        'grant_type': 'client_credentials',
        'client_id': channel_id,
        'client_secret': channel_secret
    }

    response = requests.post(
        const.API_ACCESSTOKEN_URL,
        headers=headers,
        data=body
    )
    logger.debug('new_channel_access_token %s', response.text)
    res_body = json.loads(response.text)

    return res_body['access_token']


def lambda_handler(event, contexts):
    """
    Check and update the expiration of the short-term channel access token in the db

    Returns
    -------
    event
        channel_access_token: short-term channel access token
    """
    channel_access_token_info = channel_access_token_table_controller.scan()
    for item in channel_access_token_info:
        # Ensure subsequent processes run even if an error occurs midway
        try:
            if item.get('channelAccessToken'):
                limit_date = datetime.strptime(
                    item['limitDate'], '%Y-%m-%d %H:%M:%S%z')
                now = datetime.now(gettz('Asia/Tokyo'))
                # If the token has expired today or before, reacquire the token
                if limit_date < now:
                    channel_access_token = get_channel_access_token(
                        item['channelId'], item['channelSecret'])
                    # Update the channel access token in the DB
                    update_limited_channel_access_token(
                        item['channelId'], channel_access_token)
                    logger.info('channelId: %s updated', item['channelId'])
                else:
                    channel_access_token = item['channelAccessToken']
            # If the access token has never been obtained before, acquire it new
            else:
                channel_access_token = get_channel_access_token(
                    item['channelId'], item['channelSecret'])
                update_limited_channel_access_token(
                    item['channelId'], channel_access_token)
                logger.info('channelId: %s created', item['channelId'])
        except Exception as e:
          logger.error('An Exception occurred: %s', e)
          continue
