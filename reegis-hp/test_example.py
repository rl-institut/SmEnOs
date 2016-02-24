# -*- coding: utf-8 -*-

"""
General description:
"""

###############################################################################
# imports
###############################################################################
import os
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Outputlib
from oemof.outputlib import to_pandas as tpd
from oemof import db
from oemof.tools import logger
from oemof.solph import predefined_objectives as predefined_objectives
# import oemof base classes to create energy system objects
from oemof.core import energy_system as es
from oemof.core.network.entities import Bus
from oemof.core.network.entities.buses import HeatBus
from oemof.core.network.entities.components import sinks as sink
from oemof.core.network.entities.components import sources as source
from oemof.core.network.entities.components import transformers as transformer


def fix_labels(labels, replace_underscore=True):
    r"""Method to remove the 'val' string from the labels. If
    replace_underscore is True all underscores will be replaced by space.

    This method is supposed to be temporarily.
    """
    new_labels = []
    for lab in labels:
        lab = lab.replace("(val, ", "")
        lab = lab.replace(")", "")
        if replace_underscore:
            lab = lab.replace("_", " ")
        new_labels.append(lab)
    return new_labels

###############################################################################
# read data from csv file
###############################################################################
import time
start = time.time()
logger.define_logging()

periods=2

get_data_from_db = False

basic_path = os.path.join(os.path.expanduser("~"), '.oemof', 'data_files')
if not os.path.exists(basic_path):
    os.makedirs(basic_path)

if get_data_from_db:
    con = db.engine()
    data = pd.read_sql_table('test_data', con, 'app_reegis')
    data.to_csv(os.path.join(basic_path, "reegis_example.csv"), sep=",")

data = pd.read_csv(os.path.join(basic_path, "reegis_example.csv"), sep=",")
data.drop('Unnamed: 0', 1, inplace=True)

# set time index
time_index = pd.date_range('1/1/2010', periods=periods, freq='H')

transformer.Simple.optimization_options.update({'investment': True})

simulation = es.Simulation(
    solver='gurobi', timesteps=range(len(time_index)),
    stream_solver_output=True,
    debug=True, verbose=True,
    objective_options={
        'function': predefined_objectives.minimize_cost})

energysystem = es.EnergySystem(simulation=simulation, time_idx=time_index)

# Distribution buses
bel = Bus(uid="bel",
          type="el",
          excess=True)

district_heat_bus = HeatBus(
    uid="bus_distr_heat",
    temperature=np.array([380, 360]),
    re_temperature=np.ones(periods) * 340)

storage_heat_bus = HeatBus(
    uid="bus_stor_heat",
    temperature=370)

pp_gas = transformer.Simple(uid='pp_gas',
                            inputs=[bel], outputs=[bel], capex=5000,
                            opex_var=0, out_max=[10e10], eta=[0.55])

post_heating = transformer.PostHeating(
    uid='postheat_elec',
    inputs=[bel, storage_heat_bus], outputs=[district_heat_bus],
    opex_var=0, capex=99999,
    out_max=[999993],
#    in_max=[float("+inf"), 888],
    eta=[0.95, 1])

logging.info('Start optimisation....')
energysystem.optimize()
