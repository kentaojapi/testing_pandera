from datetime import datetime
from typing import Any

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series, Index


class MyTableSchema(pa.DataFrameModel):
    index: Index[int]
    date: Series[pa.DateTime] = pa.Field(
        ge=datetime(2020, 1, 1),
        le=datetime(2024, 12, 31)
    )
    amount: Series[int] = pa.Field(ge=0)

    class Config:
        strict = True


class MyTable:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df: pd.DataFrame = df

    def where(self, column: str, op, value: Any) -> Any:
        condition = op(self.df[column], value)
        return self.df[condition]


class MyTableLoader:
    def __init__(self, file_path: str) -> None:
        self.file_path: str = file_path

    @pa.check_types
    def _load_dataset(self) -> DataFrame[MyTableSchema]:
        df = pd.read_csv(self.file_path, parse_dates=['date'])
        return df

    def run(self) -> MyTable:
        df = self._load_dataset()
        return MyTable(df)
