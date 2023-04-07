import argparse
import json
import os
import random
import sys
import time
from _decimal import Decimal

import snowflake.connector
import sqlite3
from typing import Dict

snowflake_credentials = {
    "user": "...",
    "password": "...",
    "account": "...",
    "warehouse": "...",
    "database": "...",
    "role": "..."
}


# Implement your own fix_sql function, input an original_query, error_msg and output fixed_query
def fix_sql(schema, question: str, original_query, error_msg) -> str:
    raise NotImplementedError("fix_sql is not implemented yet")


# Implement your own gen_sql function, input a question, schema (which is part of Spider's database) and output a query
def gen_sql(schema, question: str) -> str:
    raise NotImplementedError("gen_sql is not implemented yet")


def get_db_results_sqlite(db_id: str, query: str, database_folder: str) -> Dict:
    db_path = f'{database_folder}/{db_id}/{db_id}.sqlite'
    print('db_path:', db_path)
    conn = sqlite3.connect(db_path)

    with conn:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        return {'columns': columns, 'rows': result}


def get_db_results_snowflake(db_id: str, query: str) -> Dict:
    with snowflake.connector.connect(
            user=snowflake_credentials['user'],
            password=snowflake_credentials['password'],
            account=snowflake_credentials['account'],
            warehouse=snowflake_credentials['warehouse'],
            database=snowflake_credentials['database'],
            role=snowflake_credentials['role']
    ) as ctx:
        with ctx.cursor() as cursor:
            cursor.execute(f"USE SCHEMA {db_id}")
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return {'columns': columns, 'rows': result}


def db_query_result_to_str(result: Dict) -> str:
    ret = str(result['columns']) + str(result['rows'])
    if len(ret) > 1000:
        ret = ret[:1000] + f'... (truncated, total {len(ret)} chars)'
    return ret


def compare_returned_results(expected_result, actual_result):
    failed = False
    error = ""
    try:
        def sort_row(row):
            return sorted(row, key=lambda x: (type(x).__name__, x))

        def sort_rows(rows):
            return sorted(rows, key=sort_row)

        # do a "distinct" operation on the expected and actual results
        expected_result['rows'] = list(set(expected_result['rows']))
        actual_result['rows'] = list(set(actual_result['rows']))

        sorted_expected_rows = sort_rows(expected_result['rows'])
        sorted_actual_rows = sort_rows(actual_result['rows'])

        if len(sorted_expected_rows) != len(sorted_actual_rows):
            failed = True
            error = f'expected {len(sorted_expected_rows)} rows, got {len(sorted_actual_rows)} rows'
        if not failed:
            for exp_row in sorted_expected_rows:
                found = False
                for i, act_row in enumerate(sorted_actual_rows):
                    row_match = True
                    sorted_exp_row = sort_row(exp_row)
                    sorted_act_row = sort_row(act_row)

                    if len(sorted_exp_row) > len(sorted_act_row):
                        continue

                    for item in sorted_exp_row:
                        item_found = False
                        if type(item) == float:
                            for k, item2 in enumerate(sorted_act_row):
                                if (type(item2) == float or type(item2) == Decimal) and abs(item - float(item2)) < 0.01:
                                    sorted_act_row.remove(item2)
                                    item_found = True
                                    break
                        else:
                            if item in sorted_act_row:
                                sorted_act_row.remove(item)
                                item_found = True
                        if not item_found:
                            row_match = False
                            break

                    if row_match:
                        sorted_actual_rows.remove(act_row)
                        found = True
                        break

                if not found:
                    failed = True
                    error = f'expected row {exp_row} not found in actual rows'
                    break
    except Exception as e:
        failed = True
        error = "error during value comparison:" + str(e)

    return not failed, error


def validate_record(record: Dict, db_folder: str) -> Dict:
    try:
        expected_result = get_db_results_sqlite(record['db_id'], record['query'], db_folder)
        print('expected_result:', db_query_result_to_str(expected_result))
    except Exception as e:
        return {'result': 'invalid', 'error': str(e)}

    try:
        get_db_results_snowflake(record['db_id'], f"DESCRIBE SCHEMA {record['db_id']}")
    except Exception as e:
        return {'result': 'invalid', 'error': f"Error getting schema for db_id: {record['db_id']}, error: {str(e)}"}

    try:
        sql_gen_start = time.time()
        generated_query = gen_sql(record['db_id'], record['question'])
        sql_gen_end = time.time() - sql_gen_start
        print('generated_query:', generated_query)
    except Exception as e:
        return {'result': 'invalid', 'error': str(e)}

    try:
        actual_result = get_db_results_snowflake(record['db_id'], generated_query)
        print('actual_result:', db_query_result_to_str(actual_result))
    except Exception as e:
        try:
            # when there's an error, try to fix it once and run again
            generated_query = fix_sql(schema=record['db_id'], original_query=generated_query, error_msg=str(e),
                                      question=record['question'])
            print('=== fixed_sql:', generated_query)
            actual_result = get_db_results_snowflake(record['db_id'], generated_query)
            print('actual_result:', db_query_result_to_str(actual_result))
        except Exception as e:
            return {'result': 'failed', 'error': str(e), 'expected_result': db_query_result_to_str(expected_result),
                    "generated_query": generated_query}

    failed = False
    error = ''
    if not failed:
        _succeeded, error = compare_returned_results(expected_result, actual_result)
        failed = not _succeeded

    if failed:
        return {'result': 'failed', 'error': error, 'expected_result': db_query_result_to_str(expected_result),
                'actual_result': db_query_result_to_str(actual_result), "generated_query": generated_query,
                "sql_gen_time": sql_gen_end}

    return {'result': 'succeeded', "generated_query": generated_query, "sql_gen_time": sql_gen_end,
            'expected_result': db_query_result_to_str(expected_result),
            'actual_result': db_query_result_to_str(actual_result)}


def main(train_json: str, output_json: str, offset: int = 0, max_n_records: int = 10000,
         sample_n_records: int = 10000000, random_seed: int = -1):
    with open(train_json) as f:
        records = json.load(f)

    # db folder is parent of train.json, and database/ folder below the parent
    db_folder = os.path.join(os.path.dirname(train_json), 'database')
    idx = 0
    succeeded = 0
    failed = 0
    invalid = 0

    with open(output_json, 'a') as f:
        # then sample the records
        if sample_n_records < len(records):
            if random_seed >= 0:
                random.seed(random_seed)
            records = random.sample(records, sample_n_records)

        for _, record in enumerate(records):
            # Check offset
            idx = idx + 1
            if idx < offset:
                continue
            if idx >= offset + max_n_records:
                break

            start = time.time()
            result = validate_record(record, db_folder)
            result.update({
                'db_id': record['db_id'],
                'question': record['question'],
                'expected_query': record['query'],
                'elapsed_time': time.time() - start
            })
            if result['result'] == 'succeeded':
                succeeded += 1
            elif result['result'] == 'failed':
                failed += 1
            elif result['result'] == 'invalid':
                invalid += 1
            if (succeeded + failed + invalid) % 10 == 0:
                print(
                    f'======\nProcessed {succeeded + failed + invalid} records, succeeded: {succeeded}, failed: {failed}, invalid: {invalid}\n======')
            print('time spent:', time.time() - start)
            json.dump(result, f, indent=4)
            f.write(',\n')
            f.flush()
    print(
        f'======\nProcessed {succeeded + failed + invalid} records, succeeded: {succeeded}, failed: {failed}, invalid: {invalid}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Spider database validation.')
    parser.add_argument('--input_json', required=True, help='Path to input (train/dev) .json file')
    parser.add_argument('--output_json', required=True, help='Path to output JSON file')
    parser.add_argument('--offset', type=int, default=0, help='Offset to start from')
    parser.add_argument('--max_n_records', default=10000, type=int, help='Max number of records to process')
    parser.add_argument('--sample_n_records', default=1000000, type=int, help='sample n records')
    parser.add_argument('--random_seed', type=int, default=-1, help='Random seed for sampling')
    args = parser.parse_args()

    main(args.input_json, args.output_json, args.offset, args.max_n_records, args.sample_n_records, args.random_seed)
