import os
import pandas as pd
import numpy as np
import json
import itertools
from itertools import combinations
from pyspark.sql.types import *
from pyspark.sql.functions import udtf

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    
# U30.	Extractfromdate: Reads a date (as a string) and returns 3 column values (year, month, day)

# @udtf(returnType="id string,year int, month int, day int", useArrow=False)
# class ExtractFromDate:
    # def eval(self, args: list):
            # (id,arg) = args
            # if arg:
                # try:
                    # yield (id,int(arg[:arg.find('-')]), \
                            # int(arg[arg.find('-')+1:arg.rfind('-')]), \
                            # int(arg[arg.rfind('-')+1:]))
                    
                # except:
                    # yield(id, -1,-1,-1)
            # else: yield(id,None,None,None)

schema = StructType([
    # StructField("id", StringType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("day", IntegerType())
])

@pandas_udf(schema)
def extractFromDateMod1(date_series: pd.Series) -> pd.DataFrame:
    date_series = date_series.fillna(np.nan).astype("string")

    # Split strings like "2024-11-03" into 3 parts
    parts = date_series.str.split("-", expand=True)

    # Ensure 3 columns exist even if some rows are invalid
    while parts.shape[1] < 3:
        parts[parts.shape[1]] = np.nan
    parts.columns = ["year", "month", "day"]

    # Convert to numbers safely (invalid values → NaN)
    parts = parts.apply(pd.to_numeric, errors="coerce").astype("Int64")

    return parts

    # parts = date.str.split("-", expand=True)
    # parts.columns = ["year", "month", "day"]
    # parts = parts.astype(int)
    # return parts
