#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch COVID data from the NYT GitHub repository.
"""

from __future__ import annotations

__version__ = '0.1.1'
required = ['pandas', 'duckdb']

import pathlib
import datetime
from typing import List, Dict, Any, Optional
from meerschaum import Pipe
from meerschaum.config._paths import PLUGINS_TEMP_RESOURCES_PATH
REPO_URL = 'https://github.com/nytimes/covid-19-data'
TMP_PATH = PLUGINS_TEMP_RESOURCES_PATH / 'US-covid_data'
RECENT_URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties-recent.csv'
ALL_URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'

def setup(**kw):
    TMP_PATH.mkdir(parents=True, exist_ok=True)
    success, msg = True, "Success"
    return success, msg
    

def register(pipe: Pipe, **kw) -> Dict[str, Any]:
    from meerschaum.utils.prompt import prompt, yes_no
    while True:
        fips_str = prompt("Please enter a list of FIPS codes separated by commas:")
        fips = fips_str.replace(' ', '').split(',')

        fips_question = "Is this correct?"
        for f in fips:
            fips_question += f"\n  - {f}"
        fips_question += '\n'

        if not fips or not yes_no(fips_question):
            continue
        break

    return {
        'columns': {
            'datetime': 'date',
            'id': 'fips',
            'value': 'cases',
        },
        'US-covid': {
            'fips': fips,
        },
    }


def fetch(
        pipe: Pipe,
        begin: Optional[datetime.datetime],
        end: Optional[datetime.datetime],
        debug: bool = False,
        **kw: Any
    ):
    import pandas as pd
    import duckdb
    from meerschaum.utils.misc import wget

    TMP_PATH.mkdir(parents=True, exist_ok=True)
    all_filepath = TMP_PATH / 'us-counties.csv'
    recent_filepath = TMP_PATH / 'us-counties-recent.csv'
    fips = pipe.parameters['US-covid']['fips']

    wget(RECENT_URL, recent_filepath, debug=debug)
    recent_df = _get_df(recent_filepath, fips, begin, end)
    st = pipe.get_sync_time(debug=debug)
    if st is not None and len(recent_df) > 0 and min(recent_df['date']) <= st:
        return recent_df

    wget(ALL_URL, all_filepath, debug=debug)
    return _get_df(all_filepath, fips, begin, end)

def _get_df(
        csv_path: pathlib.Path,
        fips: List[str],
        begin: Optional[datetime.datetime],
        end: Optional[datetime.datetime]
    ) -> 'pd.DataFrame':
    import duckdb
    dtypes = {
        'date': 'datetime64[ms]',
        'county': str,
        'state': str,
        'fips': str,
        'cases': int,
        'deaths': int,
    }
    fips_where = "'" + "', '".join(fips) + "'"

    query = """
        SELECT *
        FROM read_csv(
            '""" + str(csv_path) + """',
            header = True,
            columns = {
                'date': 'DATE',
                'county': 'VARCHAR',
                'state': 'VARCHAR',
                'fips': 'VARCHAR',
                'cases': 'INT',
                'deaths': 'INT'
            }
        )
        WHERE fips IN (""" + fips_where + """)
    """
    if begin is not None:
        begin -= datetime.timedelta(days=2)
        query += f"\n    AND CAST(date AS DATE) >= CAST('{begin}' AS DATE)"
    if end is not None:
        query += f"\n    AND CAST(date AS DATE) <= CAST('{end}' AS DATE)"
    result = duckdb.query(query)
    return result.df()[dtypes.keys()].astype(dtypes)

