import pm4py
import pyarrow.parquet as pq
import boto3
import tempfile
import pandas as pd
from pm4py.util import constants as pm4py_constants

bucket = 'elasticbeanstalk-us-east-2-809900290383'
enable_col_replacement = True
COLUMNS = "columns"


def get_list_parquets_from_s3(path, parameters=None):
    if parameters is None:
        parameters = {}

    s3_resource = boto3.resource('s3')
    buck = s3_resource.Bucket(bucket)
    files = ["s3:///" + obj.key for obj in buck.objects.filter(Delimiter=path) if
             obj.key.startswith(path[1]) and obj.key.endswith(".parquet")]
    return files


def get_parquet_from_s3(path, parameters=None):
    if parameters is None:
        parameters = {}

    filename = tempfile.NamedTemporaryFile(suffix=".parquet")
    filename.close()

    s3_resource = boto3.resource('s3')

    path = path.split("s3:///")[1]

    s3_resource.Object(bucket, path).download_file(filename.name)

    return filename.name


def import_parquet_file(path, parameters=None):
    """
    Import a Parquet file

    Parameters
    -------------
    path
        Path of the file to import
    parameters
        Parameters of the algorithm, possible values:
            columns -> columns to import from the Parquet file

    Returns
    -------------
    df
        Pandas dataframe
    """
    if parameters is None:
        parameters = {}

    columns = parameters[COLUMNS] if COLUMNS in parameters else None
    case_id_glue = parameters[
        pm4py_constants.PARAMETER_CONSTANT_CASEID_KEY] if pm4py_constants.PARAMETER_CONSTANT_CASEID_KEY in parameters else "case:concept:name"
    timestamp_key = parameters[
        pm4py_constants.PARAMETER_CONSTANT_TIMESTAMP_KEY] if pm4py_constants.PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else "time:timestamp"

    if path.startswith("s3dir:///"):
        path = "/" + path.split("s3dir:///")[1]
        all_files = get_list_parquets_from_s3(path)
        dataframes = []
        for f in all_files:
            dataframes.append(import_parquet_file(f))
        df = pd.concat(dataframes)
        df[timestamp_key] = pd.to_datetime(df[timestamp_key], utc=True)
        df = df.sort_values([case_id_glue, timestamp_key])
        return df
    elif path.startswith("s3:///"):
        path = get_parquet_from_s3(path)

    if columns:
        if enable_col_replacement:
            columns = [x.replace(":", "AAA") for x in columns]
        df = pq.read_pandas(path, columns=columns).to_pandas()
    else:
        df = pq.read_pandas(path, columns=columns).to_pandas()
    if enable_col_replacement:
        df.columns = [x.replace("AAA", ":") for x in df.columns]

    return df


__version__ = '0.0.1'
__doc__ = "Process Mining for Python - Amazon support"
__author__ = 'PADS'
__author_email__ = 'pm4py@pads.rwth-aachen.de'
__maintainer__ = 'PADS'
__maintainer_email__ = "pm4py@pads.rwth-aachen.de"

pm4py.objects.log.importer.parquet.versions.pyarrow.apply = import_parquet_file
pm4py.objects.log.importer.parquet.factory.VERSIONS["pyarrow"] = import_parquet_file
