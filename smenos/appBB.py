#!/usr/bin/python3
# -*- coding: utf-8

import pandas as pd
import warnings
import logging

from oemof import db
from oemof.tools import logger
from oemof.core import energy_system as es
from oemof.db import tools
from oemof.db import feedin_pg
from oemof.solph import predefined_objectives as predefined_objectives
from oemof.core.network.entities import Bus
from oemof.core.network.entities.components import sinks as sink
from oemof.core.network.entities.components import transports as transport
from oemof.core.network.entities.components import sources as source

import helper_BBB as hls

# choose scenario
scenario = 'ES_2030'

# Basic inputs
warnings.simplefilter(action="ignore", category=RuntimeWarning)
logger.define_logging()
year = 2010
time_index = pd.date_range('1/1/{0}'.format(year), periods=8760, freq='H')
conn = db.connection()

cap_initial = 0.0
chp_faktor_flex = 0.84  # share of flexible generation of CHP

# parameters
(co2_emissions, co2_fix, eta_elec, eta_th, eta_th_chp, eta_el_chp,
 eta_chp_flex_el, sigma_chp, beta_chp, opex_var, opex_fix, capex,
 c_rate_in, c_rate_out, eta_in, eta_out,
 cap_loss, lifetime, wacc) = hls.get_parameters()

transmission = hls.get_data_from_csv('transmission_cap'+scenario+'.csv')

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