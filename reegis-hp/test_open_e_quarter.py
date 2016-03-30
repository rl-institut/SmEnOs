# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 14:35:28 2016

@author: uwe
"""

from Open_eQuarter.mole.stat_util import energy_demand as ed
from Open_eQuarter.mole.stat_util import building_evaluation as be


import pprint as pp
pp.pprint(ed.evaluate_building(
    population_density=52,
    area=166.7,
    floors=3.5,
    year_of_construction=2004,
    ))

pp.pprint(be.evaluate_building(
    population_density=52,
    area=166.7,
    perimeter=61.166,
    length=23.485,
    floors=3.5,
    year_of_construction=2004,
    ))
