# -*- coding: utf-8 -*-

from setuptools import setup,find_packages,find_namespace_packages

setup(name='hisengine',
      version='1.0',
      description='Integration Engine',
      long_description='README.md',
      url='http://gitlabs.com/talexie/his-engine',
      author='Alex Tumwesigye',
      author_email='atumwesigye@gmail.com',
      license='MIT',
      package_dir ={'':'src'},
      packages=find_namespace_packages(where='src',exclude=['contrib', 'docs', 'tests']),
      install_requires=[
          'pandas',
          'riak',
          'jellyfish',
          'moment',
          'requests',
          'flashtext',
          'fuzzywuzzy',
          'py_expression_eval'
      ],
      zip_safe=False)
