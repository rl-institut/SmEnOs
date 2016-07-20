#!/usr/bin/python3
# -*- coding: utf-8

import pandas as pd
import warnings

from oemof import db
from oemof.tools import logger
from oemof.core import energy_system as es
from oemof.db import tools
from oemof.solph import predefined_objectives as predefined_objectives

# Basic inputs
warnings.simplefilter(action="ignore", category=RuntimeWarning)
logger.define_logging()
year = 2010
time_index = pd.date_range('1/1/{0}'.format(year), periods=8760, freq='H')
conn = db.connection()

# Create a simulation object
simulation = es.Simulation(
    timesteps=list(range(len(time_index))), verbose=True, solver='glpk',
    stream_solver_output=True,
    objective_options={'function': predefined_objectives.minimize_cost})

# Create an energy system
Regions = es.EnergySystem(time_idx=time_index, simulation=simulation)

regionsBB = pd.DataFrame(
    [{'abbr': 'PO', 'nutsID': ['DE40F', 'DE40D', 'DE40A']},
        {'abbr': 'UB', 'nutsID': ['DE40I', 'DE405']},
        {'abbr': 'HF', 'nutsID': ['DE408', 'DE40E', 'DE40H', 'DE401', 'DE404']},
        {'abbr': 'OS', 'nutsID': ['DE409', 'DE40C', 'DE403']},
        {'abbr': 'LS', 'nutsID': ['DE406', 'DE407', 'DE40B', 'DE40G', 'DE402']},
        {'abbr': 'BE', 'nutsID': 'DE3'}],
    index=['Prignitz-Oberhavel', 'Uckermark-Barnim', u'Havelland-Fläming',
        'Oderland-Spree', 'Lausitz-Spreewald', 'Berlin'])
for index, row in regionsBB.iterrows():
    Regions.regions.append(es.Region(
        geom=tools.get_polygon_from_nuts(conn, row['nutsID']),
        name=row['abbr']))