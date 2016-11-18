#! /usr/bin/env python

from setuptools import setup

setup(name='smenos',
      version='0.0.1',
      author='Elisa Gaudchau, Birgit Schachler',
      author_email='elisa.gaudchau@rl-institut.de',
      description='Models of the heat and power systems of the region Eastern Germany and the city Greevesmühlen',
      package_dir={'smenos': 'smenos'},
      install_requires=['oemof == 0.0.9',
                        'feedinlib == 0.0.10',
                        'oemof.db',
                        'demandlib == 0.1.1',
                        'workalendar'],
      dependency_links = ['http://github.com/oemof/oemof.db/tarball/master'] 
      )