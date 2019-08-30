from os.path import dirname, join

from setuptools import setup

import pm4pyamazon


def read_file(filename):
    with open(join(dirname(__file__), filename)) as f:
        return f.read()


setup(
    name=pm4pyamazon.__name__,
    version=pm4pyamazon.__version__,
    description=pm4pyamazon.__doc__.strip(),
    long_description=read_file('README.md'),
    author=pm4pyamazon.__author__,
    author_email=pm4pyamazon.__author_email__,
    py_modules=[pm4pyamazon.__name__],
    include_package_data=True,
    packages=['pm4pyamazon'],
    url='http://www.pm4py.org',
    license='GPL 3.0',
    install_requires=[
        "pm4py",
        "boto3",
        "pyarrow"
    ],
    project_urls={
        'Documentation': 'http://pm4py.pads.rwth-aachen.de/documentation/',
        'Source': 'https://github.com/pm4py/pm4py-source',
        'Tracker': 'https://github.com/pm4py/pm4py-source/issues',
    }
)
