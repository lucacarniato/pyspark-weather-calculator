import re
from datetime import datetime as dt

from weathercalculator.ValueTypes import ValueTypes


def header_tokenizer(string_value, header_character):
    """Tokenize a row composed of tokens separated by spaces, disregarding tokens starting with a specific charter

    Args:
        string_value (str): The string to tokenize.
        header_character (str): The charter defining an header row.

    Returns:
        item_positions (dict): A dictionary with tokens as keys and positions of the tokens as values.
    """

    positions = []
    items = []
    for m in re.finditer(r"\S+", string_value):
        position, item = m.start(), m.group()
        if item != header_character:
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
        header_character,
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
            header_character (str): The symbol defining an header row
            filter_column (str): The column to use for filtering the file rows
            filter_value (str): The value to use for filtering the file rows
            header_estimated_length (int): An estimated initial header length.
            column_names (list): The names of the columns to extract.
            column_types (list): The column types.
        """

        self.rdd = spark_context.textFile(file_path)
        self.file_path = file_path

        self.header_charter = header_character
        self.header_estimated_length = header_estimated_length
        self.column_names = column_names
        self.column_types = column_types
        self.filter_column = filter_column
        self.filter_value = filter_value

        self.first_rows = self.rdd.take(self.header_estimated_length)

    def parse(self):
        """Implements the parsing operations.

        First, the number of header rows is determined by loading only the first rows and counting the rows
        starting with the self.header_character.

        Second, the last row containing the header_charter contains the column names and
        it is tokenized for extracting the column names and the column positions.

        Once the positions are found the rows with a value equal
         to self.filter_value on self.filter_column are selected.

        The selected rows are tokenized and converted to appropriate types.

        """

        # If the last row of the first rows still contains the header_character, the estimated header_estimated_length
        # is too small
        if self.first_rows[-1] == self.header_charter:
            raise ValueError("header_estimated_length is too small, please increase it")

        # Find the header
        num_header_rows = 0
        for row in self.first_rows:
            if row[0] == self.header_charter:
                num_header_rows += 1
        header = self.first_rows[num_header_rows - 1]

        # Tokenize the header
        item_positions = header_tokenizer(header, self.header_charter)

        # Check all required columns are present
        all_items_found = all(
            item in item_positions.keys() for item in self.column_names
        )
        if not all_items_found:
            raise ValueError("Not all required columns are found for " + self.file_path)

        # Filter the rows and tokenize the filtered rows
        header_symbol = self.header_charter
        column_names = self.column_names
        column_types = self.column_types
        header_symbol = self.header_charter
        filter_column = self.filter_column
        filter_value = self.filter_value
        return self.rdd.filter(
            lambda line: line[0] != header_symbol
            and line[
                item_positions[filter_column][0] : item_positions[filter_column][1]
            ].strip()
            == filter_value
        ).map(lambda x: row_tokenizer(x, item_positions, column_names, column_types))
