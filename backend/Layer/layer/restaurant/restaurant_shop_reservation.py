"""
RestaurantShopReservation操作用モジュール

"""
import os
from datetime import datetime
from dateutil.tz import gettz

from aws.dynamodb.base import DynamoDB
from common import utils


class RestaurantShopReservation(DynamoDB):
    """Class for RestaurantShopReservation operations"""
    __slots__ = ['_table']

    def __init__(self):
        """Initialization method"""
        table_name = os.environ.get("SHOP_RESERVATION_TABLE")
        super().__init__(table_name)
        self._table = self._db.Table(table_name)

    def put_item(self, shop_id, reserved_day, reserved_year_month,
                 reserved_info, total_reserved_number, vacancy_flg):
        """
        Register data

        Parameters
        ----------
        shop_id : int
            Shop ID
        reserved_day : str
            Reservation day
        reserved_year_month : str
            Reservation year and month
        reserved_info : list
            Reservation information every 30 minutes, including the number of people, time, etc.
        total_reserved_number : int
            Total number of reservations for the specified day
        vacancy_flg : int
            Vacancy flag -> 0: No vacancy, 1: Vacancy, 2: Limited vacancy

        Returns
        -------
        response : dict
            Response information
        """
        item = {
            'shopId': shop_id,
            'reservedDay': reserved_day,
            'reservedYearMonth': reserved_year_month,
            'reservedInfo': reserved_info,
            'totalReservedNumber': total_reserved_number,
            'vacancyFlg': vacancy_flg,
            "expirationDate": utils.get_ttl_time(datetime.strptime(reserved_day, '%Y-%m-%d')),
            'createdTime': datetime.now(
                gettz('Asia/Tokyo')).strftime("%Y/%m/%d %H:%M:%S"),
            'updatedTime': datetime.now(
                gettz('Asia/Tokyo')).strftime("%Y/%m/%d %H:%M:%S"),
        }

        try:
            response = self._put_item(item)
        except Exception as e:
            raise e
        return response

    def update_item(self, shop_id, reserved_day, reserved_info,
                    total_reserved_number, vacancy_flg):
        """
        Update data

        Parameters
        ----------
        shop_id : int
            Shop ID
        reserved_day : str
            Reservation day
        reserved_info : list
            Reservation information every 30 minutes, including the number of people, time, etc.
        total_reserved_number : int
            Total number of reservations for the specified day
        vacancy_flg : int
            Vacancy flag -> 0: No vacancy, 1: Vacancy, 2: Limited vacancy

        Returns
        -------
        response : dict
            Response information
        """
        key = {'shopId': shop_id, 'reservedDay': reserved_day}
        expression = ('set reservedInfo=:reserved_info, '
                      'totalReservedNumber=:total_reserved_number, '
                      'vacancyFlg=:vacancy_flg, '
                      'updatedTime=:updated_time')
        expression_value = {
            ':reserved_info': reserved_info,
            ':total_reserved_number': total_reserved_number,
            ':vacancy_flg': vacancy_flg,
            ':updated_time': datetime.now(
                gettz('Asia/Tokyo')).strftime("%Y/%m/%d %H:%M:%S")
        }
        return_value = "UPDATED_NEW"

        try:
            response = self._update_item(key, expression,
                                         expression_value, return_value)
        except Exception as e:
            raise e
        return response

    def get_item(self, shop_id, reserved_day):
        """
        Retrieve data

        Parameters
        ----------
        shop_id : int
            Shop ID
        reserved_day : str
            Reservation day

        Returns
        -------
        item : dict
            Reservation information for a specific day
        """
        key = {'shopId': shop_id, 'reservedDay': reserved_day}

        try:
            item = self._get_item(key)
        except Exception as e:
            raise e
        return item

    def query_index_shop_id_reserved_year_month(self, shop_id, reserved_year_month):  # noqa: E501
        """
        Retrieve data from the shopId-reservedYearMonth-index using the query method

        Parameters
        ----------
        shop_id : int
            Shop ID
        reserved_year_month : str
            Reservation year and month

        Returns
        -------
        items : list
            List of reservation information for a specific year and month
        """
        index = 'shopId-reservedYearMonth-index'
        expression = 'shopId = :shop_id AND reservedYearMonth = :reserved_year_month'  # noqa: E501
        expression_value = {
            ':shop_id': shop_id,
            ':reserved_year_month': reserved_year_month
        }

        try:
            items = self._query_index(index, expression, expression_value)
        except Exception as e:
            raise e
        return items
