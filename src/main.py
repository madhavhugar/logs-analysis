from operation import (
    create_log_df,
    count_distinct_operations,
    compute_avg_duration_time,
    count_by_operation_type,
    get_max_duration,
    get_min_duration
)

if __name__ == "__main__":
    filename = './data/logs.log'
    df = create_log_df(filename)
    print('1. How many queries, mutations and subscriptions have been performed?')
    operation_types = count_by_operation_type(df)
    print(f'\t{operation_types}')

    print('2. What are the counts for the different operations?')
    distinct_operations = count_distinct_operations(df)
    print(f'\t{distinct_operations}')

    print('3. What are the average duration times grouped by \n  a) operation type?\n  b) operation?')
    avg_duration_type = compute_avg_duration_time(df, 'type')
    print(f'\ta) {avg_duration_type}')
    avg_duration_name = compute_avg_duration_time(df, 'name')
    print(f'\tb) {avg_duration_name}')

    print('4. What are the max duration times grouped by\n  a) operation type?\n  b) operation?')
    max_duration_type = get_max_duration(df, 'type')
    print(f'\ta) {max_duration_type}')
    max_duration_name = get_max_duration(df, 'name')
    print(f'\tb) {max_duration_name}')

    print('4. What are the min duration times grouped by\n  a) operation type?\n  b) operation?')
    min_duration_type = get_min_duration(df, 'type')
    print(f'\ta) {min_duration_type}')
    min_duration_name = get_min_duration(df, 'name')
    print(f'\tb) {min_duration_name}')
