"""
Base module for DynamoDB operations

"""
import boto3
from boto3.dynamodb.conditions import Key
import logging
from datetime import (datetime, timedelta)

# ログ出力の設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DynamoDB:
    """Base class for DynamoDB operations"""
    __slots__ = ['_db', '_table_name']

    def __init__(self, table_name):
        """Initialization method"""
        self._table_name = table_name
        self._db = boto3.resource('dynamodb')

    def _put_item(self, item):
        """
        Register an item

        Parameters
        ----------
        item : dict
            Item to be registered

        Returns
        -------
        response : dict
            Response information

        """
        try:
            response = self._table.put_item(
                Item=self._replace_data_for_dynamodb(item))
        except Exception as e:
            raise e

        return response

    def _update_item(self, key, expression, expression_value, return_value):
        """
        Update an item

        Parameters
        ----------
        key : dict
            Key of the item to be updated
        expression : str
            Update expression
        expression_value : dict
            Values to be updated
        return_value : str
            Value to be obtained in response

        Returns
        -------
        response : dict
            Response information

        """
        try:
            response = self._table.update_item(Key=key,
                                               UpdateExpression=expression,
                                               ExpressionAttributeValues=self._replace_data_for_dynamodb(  # noqa: E501
                                                   expression_value),
                                               ReturnValues=return_value)
        except Exception as e:
            raise e

        return response

    def _update_item_optional(self, key, update_expression,
                              condition_expression, expression_attribute_names,
                              expression_value, return_value):
        """
        Update an item
        * Supports when there are update conditions other than the key

        Parameters
        ----------
        key : dict
            Key of the item to be updated
        update_expression : str
            Update expression
        condition_expression : str
            Update condition
        expression_attribute_names: dict
            Placeholders
            (for reserved words)
        expression_value : dict
            Variable declarations
        return_value : str
            Value to be obtained in response

        Returns
        -------
        response : dict
            Response information

        """
        try:
            response = self._table.update_item(
                Key=key,
                UpdateExpression=update_expression,
                ConditionExpression=condition_expression,
                ExpressionAttributeNames=expression_attribute_names,  # noqa 501
                ExpressionAttributeValues=self._replace_data_for_dynamodb(
                    expression_value),
                ReturnValues=return_value,
            )
        except Exception as e:
            raise e

        return response

    def _delete_item(self, key):
        """
        Delete an item

        Parameters
        ----------
        key : dict
            Key of the item to be deleted

        Returns
        -------
        response : dict
            Response information

        """
        try:
            response = self._table.delete_item(Key=key)
        except Exception as e:
            raise e

        return response

    def _get_item(self, key):
        """
        Retrieve an item

        Parameters
        ----------
        key : dict
            Key of the item to be retrieved

        Returns
        -------
        response : dict
            Response information

        """
        try:
            response = self._table.get_item(Key=key)
        except Exception as e:
            raise e

        return response.get('Item', {})

    def _query(self, key, value):
        """
        Use the query method to retrieve items

        Parameters
        ----------
        key : dict
            Key of the item to be retrieved

        Returns
        -------
        items : list
            List of target items

        """
        try:
            response = self._table.query(
                KeyConditionExpression=Key(key).eq(value)
            )
        except Exception as e:
            raise e

        return response['Items']

    def _query_index(self, index, expression, expression_value):
        """
        Retrieve items from an index

        Parameters
        ----------
        index : str
            Index name
        expression : str
            Expression of the target search
        expression_value : dict
            Variable names and values used in the expression

        Returns
        -------
        items : list
            Search results

        """
        try:
            response = self._table.query(
                IndexName=index,
                KeyConditionExpression=expression,
                ExpressionAttributeValues=self._replace_data_for_dynamodb(
                    expression_value),
            )
        except Exception as e:
            raise e

        return response['Items']

    def _scan(self, key, value=None):
        """
        Use the scan method to retrieve data

        Parameters
        ----------
        key : str
            Key name
        value : object, optional
            Value to search for, by default None

        Returns
        -------
        items : list
            List of target items

        """
        scan_kwargs = {}
        if value:
            scan_kwargs['FilterExpression'] = Key(key).eq(value)

        try:
            response = self._table.scan(**scan_kwargs)
        except Exception as e:
            raise e

        return response['Items']

    def _get_table_size(self):
        """
        Retrieve the number of items

        Returns
        -------
        count : int
            Number of items in the table

        """
        try:
            response = self._table.scan(Select='COUNT')
        except Exception as e:
            raise e

        return response.get('Count', 0)

    def _replace_data_for_dynamodb(self, value: dict):
        return value
