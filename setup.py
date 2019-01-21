#!/usr/bin/env python

from setuptools import setup

setup(name='tap-s3-csv',
      version='0.0.1',
      description='Singer.io tap for extracting CSV files from S3',
      author='Fishtown Analytics',
      url='http://fishtownanalytics.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_s3_csv'],
      install_requires=[
          'boto3==1.4.4',
          'smart_open==1.7.1',
          'singer-python==1.5.0',
          'voluptuous==0.10.5',
          'xlrd==1.0.0',
      ],
      entry_points='''
          [console_scripts]
          tap-s3-csv=tap_s3_csv:main
      ''',
      packages=['tap_s3_csv'])
