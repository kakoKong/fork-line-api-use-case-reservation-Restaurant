
import datetime


class ParamCheck():
    """
    Class to perform parameter checks.
    """

    def __init__(self):
        """
        Perform initialization.
        """
        pass

    def check_required(self, columns, column_name):
        """
        Perform a required check.

        Parameters
        ----------
        columns : obj
            The item to be checked for requirement
        column_name: str
            Item name

        Returns
        -------
        str
            Error content
        """
        columns_replaced = str(columns).replace(' ', '')

        if columns is None or not columns_replaced:
            return '必須入力エラー:' + column_name

    def check_length(self, columns, column_name, min, max):
        """
        Perform a character count check.

        Parameters
        ----------
        columns : obj
            The item to be checked for character count
        column_name: str
            Item name
        min : int
            Minimum number of characters
        max : int
            Maximum number of characters

        Returns
        -------
        str
            Error content
        """
        # Code for check_length method
        if type(columns) is int:
            columns = str(columns)

        if min and int(min) > len(columns):
            return f'文字数エラー（最小文字数[{min}]未満）:{column_name}'

        if max and int(max) < len(columns):
            return f'文字数エラー（最大文字数[{max}]超過）:{column_name}'

    def check_int(self, columns, column_name):
        """
        Perform an integer type check.

        Parameters
        ----------
        columns : obj
            The item to be checked if it's of integer type
        column_name: str
            Item name

        Returns
        -------
        str
            Error content
        """
        # Code for check_int method
        if isinstance(columns, int):
            columns_replaced = True
        else:
            columns_replaced = columns.isnumeric()

        if columns is None or not columns_replaced:
            return 'int型チェックエラー:' + column_name

    def check_year_month(self, columns, column_name):
        """
        Check the format of year and month.

        Parameters
        ----------
        columns : obj
            The item to check the format
        column_name: str
            Item name

        Returns
        -------
        str
            Error content
        """
        # Code for check_year_month method
        columns_replaced = columns.replace('-', '').replace('/', '')
        try:
            datetime.datetime.strptime(columns_replaced, "%Y%m")
        except ValueError:
            return f'年月形式エラー : {column_name}({columns})'

    def check_year_month_day(self, columns, column_name):
        """
        Check the format of year, month, and day.

        Parameters
        ----------
        columns : obj
            The item to check the format
        column_name: str
            Item name

        Returns
        -------
        str
            Error content
        """
        # Code for check_year_month_day method
        columns_replaced = columns.replace('-', '').replace('/', '')
        try:
            datetime.datetime.strptime(columns_replaced, "%Y%m%d")
        except ValueError:
            return f'年月日形式エラー : {column_name}({columns})'

    def check_time_format(self, columns, column_name, time_format):
        """
        Check the format of time.

        Parameters
        ----------
        columns : obj
            The item to check the format
        column_name: str
            Item name
        time_format: str
            The format to be checked

        Returns
        -------
        str
            Error content
        """
        # Code for check_time_format method
        columns_replaced = columns.replace(':', '')
        try:
            datetime.datetime.strptime(columns_replaced, time_format)
        except ValueError:
            return f'時間形式エラー : {column_name}({columns})'
