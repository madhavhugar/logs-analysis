import re
import pandas as pd
from typing import Match, TypedDict, Optional, Dict


OperationMeta = TypedDict(
    'LogMeta', {'name': str, 'type': str, 'duration': str},
    )


def is_valid_logline(line: str) -> bool:
    extract: Match[str] = re.search(
        r'^(\[graphql\])',
        line,
    )
    if extract:
        return True
    return False


def get_operation_name(line: str) -> Optional[str]:
    name_extract: Match[str] = re.search(
        r'(\| operation: ([a-zA-Z]* )\|)',
        line,
    )
    if name_extract:
        return name_extract.group(2).strip()
    return None


def get_operation_type(line: str) -> Optional[str]:
    type_extract: Match[str] = re.search(
        r'(\| operationType: (query|subscription|mutation){1})',
        line,
    )
    if type_extract:
        return type_extract.group(2).strip()
    return None


def get_operation_duration(line: str) -> Optional[float]:
    type_extract: Match[str] = re.search(
        r'(\| duration: ([0-9]*.[0-9]*))',
        line,
    )
    if type_extract:
        return float(type_extract.group(2).strip())
    return None


def parse_logline(line: str) -> Optional[OperationMeta]:
    if not is_valid_logline(line):
        return None
    operation = {
        'name': get_operation_name(line),
        'type': get_operation_type(line),
        'duration': get_operation_duration(line),
    }
    return operation


def create_log_df(filename: str) -> pd.DataFrame:
    df: pd.DataFrame = pd.DataFrame(data={
        'name': [],
        'type': [],
        'duration': [],
    })
    with open(filename, 'r') as file:
        while True:
            line: str = file.readline()
            if not line:
                break
            operation: OperationMeta = parse_logline(line)
            df = df.append(operation, ignore_index=True)
    return df


def count_by_operation_type(df: pd.DataFrame) -> int:
    count_df = df.groupby(['type']).count()
    return count_df.name.to_dict()


def count_distinct_operations(df: pd.DataFrame) -> int:
    return df.name.nunique()


def compute_avg_duration_time(
    df: pd.DataFrame,
    groupby_key: str,
    ) -> Dict[str, float]:
    mean_df = df.groupby([groupby_key]).mean()
    return mean_df.duration.to_dict()


def get_max_duration(df: pd.DataFrame, groupby_key: str) -> Dict[str, str]:
    max_df = df.groupby([groupby_key]).max()
    return max_df.duration.to_dict()


def get_min_duration(df: pd.DataFrame, groupby_key: str) -> Dict[str, str]:
    min_df = df.groupby([groupby_key]).min()
    return min_df.duration.to_dict()
