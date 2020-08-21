import pandas as pd

from src.operation import (
    parse_logline,
    get_operation_name,
    get_operation_type,
    get_operation_duration,
    is_valid_logline,
    create_log_df,
    count_by_operation_type,
    count_distinct_operations,
    compute_avg_duration_time,
    get_max_duration,
    get_min_duration)


with open('./tests/fixtures/test_logs.log', 'r') as file:
    data = file.readline()


def test_parse_logline():
    logline = '[graphql] operation-responsetime | operation: GetExporterTemplates | duration: 0.750366427 | operationType: query'
    operation = parse_logline(logline)
    assert operation['name'] == 'GetExporterTemplates'
    assert operation['duration'] == 0.750366427
    assert operation['type'] == 'query'

    invalid_logline = 'operation-responsetime | operation: GetExporterTemplates'
    operation = parse_logline(invalid_logline)
    assert operation is None

    semi_valid_logline = '[graphql] operation-responsetime | operation: GetExporterTemplates |'
    operation = parse_logline(semi_valid_logline)
    assert operation['name'] == 'GetExporterTemplates'
    assert operation['type'] is None
    assert operation['duration'] is None


def test_is_valid_logline():
    logline = '[graphql] operation-responsetime | operation: GetExporterTemplates | duration: 0.750366427 | operationType: query'
    is_valid = is_valid_logline(logline)
    assert is_valid is True

    logline = 'operation-responsetime | operation: GetExporterTemplates'
    is_valid = is_valid_logline(logline)
    assert is_valid is False


def test_get_operation_name():
    logline = '[graphql] operation-responsetime | operation: GetExporterTemplates | duration: 0.750366427 | operationType: query'
    name = get_operation_name(logline)
    assert name == 'GetExporterTemplates'

    invalid_logline = '[graphql] operation-responsetime'
    name = get_operation_name(invalid_logline)
    assert name is None


def test_get_operation_type():
    logline = '[graphql] operation-responsetime | operation: GetExporterTemplates | duration: 0.750366427 | operationType: query'
    operation_type = get_operation_type(logline)
    assert operation_type == 'query'

    invalid_logline = '[graphql] operation-responsetime'
    operation_type = get_operation_type(invalid_logline)
    assert operation_type is None


def test_get_operation_duration():
    logline = '[graphql] operation-responsetime | operation: GetExporterTemplates | duration: 0.750366427 | operationType: query'
    duration = get_operation_duration(logline)
    assert duration == 0.750366427

    invalid_logline = '[graphql] operation-responsetime'
    duration = get_operation_duration(invalid_logline)
    assert duration is None


def test_create_log_df():
    file = './tests/fixtures/test_logs.log'
    df = create_log_df(file)
    assert len(df) == 5
    assert df.name.nunique() == 3
    assert df.duration.nunique() == 4
    assert df.type.nunique() == 3


def test_count_by_operation_type():
    file = './tests/fixtures/test_logs.log'
    df = create_log_df(file)
    counts = count_by_operation_type(df)
    expected = {
        'mutation': 1,
        'query': 1,
        'subscription': 2,
    }
    assert counts == expected


def test_count_distinct_operations():
    file = './tests/fixtures/test_logs.log'
    df = create_log_df(file)

    distinct_operations = count_distinct_operations(df)
    assert distinct_operations == 3


def test_avg_duration_time():
    file = './tests/fixtures/test_logs.log'
    df = create_log_df(file)

    avg = compute_avg_duration_time(df, groupby_key='name')
    expected = {
        'SearchPatients': 0.052525752,
        'onCaseEvent': 87.068694843,
        'onDigitalConsultationEvent': 87.0669925345,
    }
    assert len(avg) == 3
    assert avg == expected

    avg = compute_avg_duration_time(df, groupby_key='type')
    expected = {
        'mutation': 87.066061018,
        'query': 0.052525752,
        'subscription': 87.068309447,
    }
    assert len(avg) == 3
    assert avg == expected


def test_max_duration_time():
    file = './tests/fixtures/test_logs.log'
    df = create_log_df(file)

    max_duration = get_max_duration(df, groupby_key='name')
    expected = {
        'SearchPatients': 0.052525752,
        'onCaseEvent': 87.068694843,
        'onDigitalConsultationEvent': 87.067924051,
    }
    assert max_duration == expected

    max_duration = get_max_duration(df, groupby_key='type')
    expected = {
        'mutation': 87.066061018,
        'query': 0.052525752,
        'subscription': 87.068694843,
    }
    assert max_duration == expected


def test_min_duration_time():
    file = './tests/fixtures/test_logs.log'
    df = create_log_df(file)

    min_duration = get_min_duration(df, groupby_key='name')
    expected = {
        'SearchPatients': 0.052525752,
        'onCaseEvent': 87.068694843,
        'onDigitalConsultationEvent': 87.066061018,
    }
    assert min_duration == expected

    min_duration = get_min_duration(df, groupby_key='type')
    expected = {
        'mutation': 87.066061018,
        'query': 0.052525752,
        'subscription': 87.067924051,
    }
    assert min_duration == expected
