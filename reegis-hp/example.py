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
import matplotlib.pyplot as plt

# Outputlib
from oemof.outputlib import to_pandas as tpd

from oemof_pg import db
from oemof.tools import logger
# import solph module to create/process optimization model instance
from oemof.solph import predefined_objectives as predefined_objectives
# import oemof base classes to create energy system objects
from oemof.core import energy_system as es
from oemof.core.network.entities import Bus
from oemof.core.network.entities.components import sinks as sink
from oemof.core.network.entities.components import sources as source
from oemof.core.network.entities.components import transformers as transformer


###############################################################################
# read data from csv file
###############################################################################

logger.define_logging()

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
time_index = pd.date_range('1/1/2010', periods=8760, freq='H')

# temp infos
logging.info(list(data.keys()))

###############################################################################
# set optimzation options for storage components
###############################################################################

transformer.Storage.optimization_options.update({'investment': True})

###############################################################################
# Create oemof objetc
###############################################################################

simulation = es.Simulation(
    solver='gurobi', timesteps=range(len(time_index)),
    stream_solver_output=True,
    debug=True, verbose=True,
    objective_options={
        'function': predefined_objectives.minimize_cost})

energysystem = es.EnergySystem(simulation=simulation, time_idx=time_index)

# Resource buses
bgas = Bus(uid="bgas",
           type="gas",
           price=0,
           balanced=True,
           excess=False)

boil = Bus(uid="boil",
           type="oil",
           price=0,
           balanced=True,
           excess=False)

# Resources
rgas = source.Commodity(uid='rgas',
                        outputs=[bgas],
                        out_max=[float('+inf')],
                        opex_var=10e10)

roil = source.Commodity(uid='roils',
                        outputs=[boil],
                        out_max=[float('+inf')],
                        opex_var=11e10)

# Distribution buses
bel = Bus(uid="bel",
          type="el",
          excess=True)

district_heat_bus = Bus(uid="bus_distr_heat",
                        type="distr_heat",
                        excess=True)

oil_heat_bus = Bus(uid="bus_oil_heat",
                   type="oil_heat",
                   excess=True)

# Demand
district_heat_demand = sink.Simple(uid="district_heat",
                                   inputs=[district_heat_bus],
                                   val=data['dst0'] * 20)

oil_heat_demand = sink.Simple(uid="oil_heat",
                              inputs=[oil_heat_bus],
                              val=data['thoi'] * 30)

elec_demand = sink.Simple(uid="demand_elec", inputs=[bel], val=data['elec'])

# Transformer
oil_boiler = transformer.Simple(uid='boiler_oil',
                                inputs=[boil], outputs=[oil_heat_bus],
                                opex_var=0, out_max=[10e10], eta=[0.88])

gas_boiler = transformer.Simple(uid='boiler_gas',
                                inputs=[bgas], outputs=[district_heat_bus],
                                opex_var=0, out_max=[10e10], eta=[0.88])
                                
pp_gas = transformer.Simple(uid='pp_gas',
                            inputs=[bgas], outputs=[bel],
                            opex_var=0, out_max=[10e10], eta=[0.55])

chp_gas = transformer.CHP(
    uid='chp_gas', inputs=[bgas], outputs=[bel, district_heat_bus],
    opex_var=0, out_max=[0.3e10, 0.5e10], eta=[0.3, 0.5])

heating_rod_distr = transformer.Simple(uid='heatrod_distr',
                                       inputs=[bel],
                                       outputs=[district_heat_bus],
                                       opex_var=0, out_max=[10e10], eta=[0.95])

heating_rod_oil = transformer.Simple(uid='heatrod_oil',
                                     inputs=[bel], outputs=[oil_heat_bus],
                                     opex_var=0, out_max=[10e10], eta=[0.95])
# Renewables
wind = source.FixedSource(uid="wind",
                          outputs=[bel],
                          val=data['rwin'],
                          out_max=[1000],
                          add_out_limit=0,
                          capex=1000,
                          opex_fix=20,
                          lifetime=25,
                          crf=0.08)

pv = source.FixedSource(uid="pv",
                        outputs=[bel],
                        val=data['rpvo'],
                        out_max=[582],
                        add_out_limit=0,
                        capex=900,
                        opex_fix=15,
                        lifetime=25,
                        crf=0.08)

# Storages
storage = transformer.Storage(uid='sto_simple',
                              inputs=[bel],
                              outputs=[bel],
                              eta_in=1,
                              eta_out=0.8,
                              cap_loss=0.00,
                              opex_fix=35,
                              opex_var=50,
                              capex=1000,
                              cap_max=0,
                              cap_initial=0,
                              c_rate_in=1/6,
                              c_rate_out=1/6)

###############################################################################
# Create, solve and postprocess OptimizationModel instance
###############################################################################
logging.info('Start optimisation....')
energysystem.optimize()
energysystem.dump()
logging.info(energysystem.restore())

# Creation of a multi-indexed pandas dataframe
es_df = tpd.EnergySystemDataFrame(energy_system=energysystem)

cdict = {'wind': '#5b5bae',
         'pv': '#ffde32',
         'sto_simple': '#42c77a',
         'pp_gas': '#636f6b',
         'boiler_oil': '#ce4aff',
         'chp_gas': '#b5aed8',
         'boiler_gas': '#a17680',
         'demand_elec': '#830000',
         'district_heat': '#830000',
         'oil_heat': '#830000',
         'boiler_oil': '#272e24',
         'heatrod_oil': '#7fffc7',
         'heatrod_distr': '#ff7f7f',
         }

## Plotting a combined stacked plot
#es_df.stackplot("bel", date_from="2010-06-01 00:00:00",
#                date_to="2010-06-8 00:00:00",
#                title="Electricity bus", autostyle=True,
#                ylabel="Power in MW", xlabel="Date",
#                linewidth=4,
#                tick_distance=24)

fig = plt.figure(figsize=(24, 14))
plt.rc('legend', **{'fontsize': 19})
plt.rcParams.update({'font.size': 14})
plt.style.use('ggplot')


plt.subplots_adjust(hspace=0.1, left=0.07, right=0.9)

ax = fig.add_subplot(3, 1, 1)
es_df.stackplot_part(
    "bel", ax, date_from="2010-06-01 00:00:00", date_to="2010-06-8 00:00:00",
    title="", colordict=cdict,
    ylabel="Power in MW", xlabel="",
    linewidth=4,
    tick_distance=24)

ax.set_xticklabels([])

ax = fig.add_subplot(3, 1, 2)
es_df.stackplot_part(
    "bus_distr_heat", ax, date_from="2010-06-01 00:00:00",
    date_to="2010-06-8 00:00:00",
    title="", colordict=cdict,
    ylabel="Power in MW", xlabel="",
    linewidth=4,
    tick_distance=24)

ax.set_xticklabels([])

ax = fig.add_subplot(3, 1, 3)
es_df.stackplot_part(
    "bus_oil_heat", ax, date_from="2010-06-01 00:00:00",
    date_to="2010-06-8 00:00:00",
    title="", colordict=cdict,
    ylabel="Power in MW", xlabel="Date",
    linewidth=4,
    tick_distance=24)

fig.savefig(os.path.join('/home/uwe/', 'test' + '.pdf'))
plt.show(fig)
plt.close(fig)
