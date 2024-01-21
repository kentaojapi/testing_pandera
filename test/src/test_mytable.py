from datetime import datetime
import operator

import pandas as pd
from pandas._testing import assert_frame_equal
from pandera.errors import SchemaError
import pytest

from src.mytable import (
    MyTableSchema, MyTable, MyTableLoader)


class TestMyTableSchema:
    def test_vaid_dataframe(self) -> None:
        df = pd.DataFrame({
            "date": [
                datetime(2020, 1, 1),
                datetime(2021, 5, 15),
                datetime(2023, 7, 23)],
            "amount": [100, 250, 175]
        })
        assert_frame_equal(df, MyTableSchema(df))

    @pytest.mark.parametrize(
        "df",
        [
            (  # case0: including invalid datetime.
                pd.DataFrame({
                    "date": [
                        datetime(2019, 12, 31),
                        datetime(2021, 5, 15),
                        datetime(2023, 7, 23)],
                    "amount": [100, 250, 175]
                })
            ),
            (  # case1: including invalid amount.
                pd.DataFrame({
                    "date": [
                        datetime(2019, 12, 31),
                        datetime(2021, 5, 15),
                        datetime(2023, 7, 23)],
                    "amount": [-1, 250, 175]
                })
            ),
        ]
    )
    def test_invaid_dataframe(self, df: pd.DataFrame) -> None:
        with pytest.raises(SchemaError):
            _ = MyTableSchema(df)


class TestMyTable:
    def test_where_method(self) -> None:
        df = pd.DataFrame({
            "date": [
                datetime(2020, 1, 1),
                datetime(2021, 5, 15),
                datetime(2023, 7, 23)],
            "amount": [100, 250, 175]
        })
        my_table = MyTable(df)
        filtered = my_table.where('amount', operator.gt, 150)
        answer = pd.DataFrame({
            "date": [
                datetime(2021, 5, 15),
                datetime(2023, 7, 23)],
            "amount": [250, 175]
        })
        assert_frame_equal(
            filtered.reset_index(drop=True),
            answer.reset_index(drop=True)
        )


class TestMyTableLoader:
    def test_load_valid_csv(self) -> None:
        file_path = './test/data/valid.csv'
        _ = MyTableLoader(file_path).run()

    def test_load_invalid_csv(self) -> None:
        file_path = './test/data/invalid.csv'
        with pytest.raises(SchemaError):
            _ = MyTableLoader(file_path).run()
