import re
from datetime import datetime as dt

from weathercalculator.ValueTypes import ValueTypes


def head_tokenizer(string_value, header_charater):
    """Tokenize a row composed of tokens separated by spaces, disregarding tokens starting with a specific charter

    Args:
        string_value (str): The string to tokenize.
        header_charater (str): The charter defining an header row.

    Returns:
        item_positions (dict): A dictionary with tokens as keys and positions of the tokens as values.
    """

    positions = []
    items = []
    for m in re.finditer(r"\S+", string_value):
        position, item = m.start(), m.group()
        if item != header_charater:
            items.append(item)
            positions.append(position)

    item_positions = dict()
    for i in range(len(items) - 1):
        if i == 0:
            item_positions[items[i]] = [0, positions[i + 1] - 1]
            continue
        item_positions[items[i]] = [positions[i], positions[i + 1] - 1]

    item_positions[items[-1]] = positions[-1], len(string_value)

    return item_positions


def cast_string_value_to_type(string_value, string_type):
    """Casts a string to a value, as defined by the value type.

    Args:
        string_value (str): The string to cast.
        string_type (ValueTypes): The type of the casting operation.
    """

    result = None

    if string_value == "":
        return result

    if string_type == ValueTypes.TimeStamp:
        result = dt.strptime(string_value, "%Y-%m-%d %H:%M:%S")

    if string_type == ValueTypes.Float:
        result = float(string_value)

    if string_type == ValueTypes.String:
        result = str(string_value)

    if string_type == ValueTypes.Integer:
        result = int(string_value)

    return result


def row_tokenizer(string_value, item_positions, column_names, column_types):
    """Finds the tokens on a row at specified positions.

    Args:
        string_value (str): The string to tokenize.
        item_positions (dict): A dictionary with the tokens as keys and the positions of the tokens as values.
        column_names (list): The column names of each token
        column_types (list): The column types of each token
    """

    result = []
    num_requested_items = len(column_names)

    for column_name in column_names:

        if len(result) == num_requested_items:
            break

        if column_name not in item_positions.keys():
            continue

        var_value = string_value[
            item_positions[column_name][0] : item_positions[column_name][1]
        ].strip()

        result.append(cast_string_value_to_type(var_value, column_types[column_name]))

    return result


class FileParser:
    """Class parsing a large text file with PySpark"""

    def __init__(
        self,
        file_path,
        spark_context,
        header_charater,
        filter_column,
        filter_value,
        header_estimated_length,
        column_names,
        column_types,
    ):
        """Constructor

        Args:
            file_path (str): The file path.
            spark_context (sparkContext): The spark context.
            header_charater (str): The symbol defining an header row
            filter_column (str): The column to use for filtering the file rows
            filter_value (str): The value to use for filtering the file rows
            header_estimated_length (int): An estimated initial header length.
            column_names (list): The names of the columns to extract.
            column_types (list): The column types.
        """

        self.rdd = spark_context.textFile(file_path)
        self.file_path = file_path

        self.header_charater = header_charater
        self.header_estimated_length = header_estimated_length
        self.column_names = column_names
        self.column_types = column_types
        self.filter_column = filter_column
        self.filter_value = filter_value

        self.first_rows = self.rdd.take(self.header_estimated_length)

    def get_temporal_extend(self):
        self.header_extract_period_function(self.first_rows)

    def parse(self):

        # assume contained in the first self.length_header rows
        if self.first_rows[-1] == self.header_charater:
            raise ValueError(
                "Estimated length of the header is too small, please increase it"
            )

        num_header_rows = 0
        for row in self.first_rows:
            if row[0] == self.header_charater:
                num_header_rows += 1

        header = self.first_rows[num_header_rows - 1]
        item_positions = head_tokenizer(header, self.header_charater)
        all_items_found = all(
            item in item_positions.keys() for item in self.column_names
        )

        if not all_items_found:
            raise ValueError("Not all required columns are found for " + self.file_path)

        header_symbol = self.header_charater
        column_names = self.column_names
        column_types = self.column_types
        header_symbol = self.header_charater
        filter_column = self.filter_column
        filter_value = self.filter_value

        return self.rdd.filter(
            lambda line: line[0] != header_symbol
            and line[
                item_positions[filter_column][0] : item_positions[filter_column][1]
            ].strip()
            == filter_value
        ).map(lambda x: row_tokenizer(x, item_positions, column_names, column_types))
