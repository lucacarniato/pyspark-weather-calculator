from datetime import datetime as dt

from pytest import approx
from weathercalculator.FileParser import (cast_string_value_to_type,
                                          header_tokenizer, row_tokenizer)
from weathercalculator.ValueTypes import ValueTypes


def test_header_tokenizer():
    header = "# DTG                LOCATION            NAME                                            LATITUDE "
    positions = header_tokenizer(header, "#")
    assert positions["DTG"][0] == 0
    assert positions["DTG"][1] == 20
    assert positions["LOCATION"][0] == 21
    assert positions["LOCATION"][1] == 40
    assert positions["NAME"][0] == 41
    assert positions["NAME"][1] == 88
    assert positions["LATITUDE"][0] == 89
    assert positions["LATITUDE"][1] == 98


def test_row_tokenizer():
    header = "# DTG                LOCATION            NAME                                            LATITUDE"
    row = "2003-04-01 00:10:00  235_T_obs           De Kooy waarneemterrein                         52.92694"
    positions = header_tokenizer(header, "#")

    column_names = ["DTG", "LOCATION", "NAME", "LATITUDE"]
    column_types = {
        column_names[0]: ValueTypes.TimeStamp,
        column_names[1]: ValueTypes.String,
        column_names[2]: ValueTypes.String,
        column_names[3]: ValueTypes.Float,
    }
    result = row_tokenizer(row, positions, column_names, column_types)

    assert result[0] == dt(2003, 4, 1, 00, 10, 00)
    assert result[1] == "235_T_obs"
    assert result[2] == "De Kooy waarneemterrein"
    assert result[3] == approx(52.92694, 0.00001)


def test_cast_string_value_to_type():
    value = cast_string_value_to_type("", ValueTypes.TimeStamp)
    assert value == None
