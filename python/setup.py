#!/usr/bin/env python
# -*- coding: utf8 -*-
from setuptools import setup

setup(name='YakDB',
      version='0.1',
      description='YakDB python binding',
      author='Uli Köhler',
      author_email='ukoehler@btronik.de',
      url='http://techoverflow.net/',
      packages=['YakDB', 'YakDB.Graph', 'YakDB.InvertedIndex'],
      scripts=["yak"],
      requires=['zmq (>=13.0)'],
      test_suite="tests",
      classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Education',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Scientific/Engineering :: Physics',
        'Topic :: Scientific/Engineering :: Information Analysis'
      ]
     )
