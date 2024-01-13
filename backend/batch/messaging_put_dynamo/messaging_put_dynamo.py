import logging
import datetime
from dateutil.tz import gettz
import os
import boto3
import json

from common import (line, utils)
# Import DynamoDB operation class
from common.remind_message import RemindMessage
from common.channel_access_token import ChannelAccessToken


# Configuration for log output
logger = logging.getLogger()
LOGGER_LEVEL = os.getenv('LOGGER_LEVEL', None)
if LOGGER_LEVEL == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

# Declaration of the table
remind_message_table_controller = RemindMessage()
channel_access_token_table_controller = ChannelAccessToken()


def send_message_from_dynamodb():
    """
    Retrieve data registered in the table and send a push message.
    """

    # Retrieve today's messages to be sent from the DynamoDB table
    today = datetime.datetime.strftime(
        (datetime.datetime.now(gettz('Asia/Tokyo')).date()), '%Y-%m-%d')

    today_messages = remind_message_table_controller.query_index_remind_date(today)

    # NOTE: [Search with query] If no data→Items exist [Search with get_item] If no data→The key 'Items' itself does not exist
    if not today_messages:
        return

    # MEMO: If Lambda execution time becomes long, consider saving to SQS once and then polling with another Lambda.
    # MEMO: In the above case (EventBridge→Lambda→SQS→Lambda)
    for message_item in today_messages:
        # Convert Decimal type to int
        message_info = json.loads(json.dumps(
            message_item['messageInfo'],
            default=utils.decimal_to_int))
        try:
            channel_info = channel_access_token_table_controller.get_item(
                message_info['channelId'])
            line.send_push_message(channel_info['channelAccessToken'],
                                   message_info['messageBody'],
                                   message_info['userId'])
        except Exception as e:
            logger.exception(
                'An error occurred while sending the push message. Please check the corresponding message. Message ID: %s',
                message_item['id'])
            logger.exception('Error details: %s', e)
            continue


def lambda_handler(event, context):
    """
    Return the content of the LINE talk sent to the Webhook

    Parameters
    ----------
    event : dict
        Request content to the Webhook.
    context : dict
        Context content.

    Returns
    -------
    Response : dict
        Response content to the Webhook.
    """
    logger.info(event)

    try:
        send_message_from_dynamodb()
    except Exception as e:
        logger.exception('Occur Exception: %s', e)
        return utils.create_error_response('ERROR')

    return utils.create_success_response('OK')
