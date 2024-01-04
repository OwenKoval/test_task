"""
    Test Python Task anodot Koval Oleh
"""
import os
import json
import ast
import pandas as pd
import boto3

os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAWD3T74ZBE2POBW7F'
os.environ['AWS_SECRET_ACCESS_KEY'] = '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'

# Consts
FILE_PATH = 'data.csv'
REPORT_1_NAME = 'oleh_report_1.gzip'
REPORT_2_NAME = 'oleh_report_2.gzip'
REPORT_3_NAME = 'oleh_report_3.gzip'
S3_BUCKET_NAME = 'dev-test-task'
S3_FOLDER_PATH = 'oleh_data/'


def read_csv(file_path: str, separation: str = ',', json_cols: list = None):
    """
        Read csv file using pandas
    :param file_path: Path to csv file
    :param separation: Symbol to separate column in csv file
    :param json_cols: Column which need to convert to json type
    :return: DataFrame
    """
    converters = {}
    if json_cols:
        for col in json_cols:
            converters[col] = lambda x: json.loads(x.replace('null', 'null'))
    return pd.read_csv(file_path,
                       sep=separation,
                       converters=converters)


def write_to_parquet(data_frame: pd.DataFrame, file_path: str,
                     compression: str = '', index: bool = False):
    """
        Write DataFrame as csv file
    :param data_frame: DataFrame with data
    :param file_path: Path to write file
    :param compression: Type of compression
    :param index: Use index or not
    """
    data_frame.to_parquet(file_path,
                          compression=compression,
                          index=index)


def prepare_df(raw_df: pd.DataFrame):
    """
        Made transformation with DataFrame and clean data
    :param raw_df: DataFrame with raw data
    :return: prepared DataFrame
    """

    raw_df['date'] = pd.to_datetime(raw_df['date'])
    raw_df['service_name'] = raw_df['service'].apply(lambda x: x.get('description'))
    raw_df['sku_name'] = raw_df['sku'].apply(lambda x: x.get('description'))
    raw_df['resource'] = raw_df['resource'].apply(lambda x: x.get('name'))
    raw_df['project'] = raw_df['project'].apply(lambda x: x.get('name'))

    raw_df['usage'] = (
        raw_df['usage']
            .apply(lambda x: json.loads(x.replace("'", "\""))['amount'] if pd.notna(x) else None))

    raw_df['credits'] = (
        raw_df['credits']
            .apply(lambda x: ast.literal_eval(x) if pd.notna(x) else []))

    raw_df['net_usage_amount'] = (
        raw_df['credits']
            .apply(lambda x: sum(item.get('amount', 0) for item in x)))

    clean_df = raw_df[(raw_df['date'] >= '2022-09-01') &
                      (raw_df['date'] < '2022-10-01')]

    return clean_df


def upload_to_s3(file_path: str, bucket_name: str, folder_path: str = ''):
    """
        Upload multiple files to an S3 bucket
    :param file_path: File path
    :param bucket_name: S3 bucket name
    :param folder_path: Folder path in S3 (optional)
    :return: True if successful, else False
    """
    # Create an S3 client
    s3_client = boto3.client('s3')

    try:
        object_name = folder_path + file_path
        # Upload the file
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f'Successfully uploaded {file_path} to {bucket_name}/{object_name}')

        return True
    except Exception as e:
        print(f'Error uploading files to {bucket_name}/{folder_path}: {e}')
        return False


def get_report_1(clean_df: pd.DataFrame):
    """
        Find how much money per day and service was spent on September 2022
    :param clean_df: DataFrame with prepared data
    :return: DataFrame
    """

    report_1_df = (clean_df
                   .groupby(['date', 'service_name'])
                   .agg({'cost': 'sum'})
                   .reset_index())

    report_1_df.rename(columns={'cost': 'total_cost'}, inplace=True)

    return report_1_df


def get_report_2(clean_df: pd.DataFrame):
    """
        Find Total cost and Total usage for September 2022 per service, sku, resource, and project.
    :param clean_df: DataFrame with prepared data
    :return: DataFrame
    """

    report_2_df = (clean_df
                   .groupby(['service_name', 'sku_name', 'resource', 'project'])
                   .agg({'cost': 'sum',
                         'usage': 'sum'})
                   .reset_index())

    return report_2_df


def get_report_3(clean_df: pd.DataFrame):
    """
        Find billed usage amount (amount without credits) for each service in September 2022
    :param clean_df: DataFrame with prepared data
    :return: DataFrame
    """
    report_3_df = (clean_df
                   .groupby('service_name')
                   .agg({'usage': 'sum',
                         'net_usage_amount': 'sum'
                         })
                   .reset_index())

    report_3_df.rename(columns={'usage': 'total_amount'}, inplace=True)

    report_3_df['net_usage_amount'] = report_3_df['total_amount'] - report_3_df['net_usage_amount']

    return report_3_df


if __name__ == '__main__':
    raw_df = read_csv(file_path=FILE_PATH,
                      separation=';',
                      json_cols=['service', 'sku', 'resource', 'project'])

    clean_df = prepare_df(raw_df=raw_df)

    report_1_df = get_report_1(clean_df=clean_df)
    write_to_parquet(data_frame=report_1_df,
                     file_path=REPORT_1_NAME,
                     compression='gzip')
    upload_to_s3(file_path=REPORT_1_NAME,
                 bucket_name=S3_BUCKET_NAME,
                 folder_path=S3_FOLDER_PATH)

    report_2_df = get_report_2(clean_df=clean_df)
    write_to_parquet(data_frame=report_2_df,
                     file_path=REPORT_2_NAME,
                     compression='gzip')
    upload_to_s3(file_path=REPORT_2_NAME,
                 bucket_name=S3_BUCKET_NAME,
                 folder_path=S3_FOLDER_PATH)

    report_3_df = get_report_3(clean_df=clean_df)
    write_to_parquet(data_frame=report_3_df,
                     file_path=REPORT_3_NAME,
                     compression='gzip')
    upload_to_s3(file_path=REPORT_3_NAME,
                 bucket_name=S3_BUCKET_NAME,
                 folder_path=S3_FOLDER_PATH)
