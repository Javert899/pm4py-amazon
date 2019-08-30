import pm4py
import pyarrow.parquet as pq
import boto3
import tempfile


bucket = 'elasticbeanstalk-us-east-2-809900290383'
enable_col_replacement = True
COLUMNS = "columns"


def get_parquet_from_s3(path, parameters=None):
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

    if path.startswith("s3:///"):
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
