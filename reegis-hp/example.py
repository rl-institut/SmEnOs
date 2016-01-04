# -*- coding: utf-8 -*-

"""
General description:
"""

###############################################################################
# imports
###############################################################################
import os
import pandas as pd
import matplotlib.pyplot as plt

from oemof.outputlib import devplots
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

con = db.connection()
basic_path = os.path.join(os.path.expanduser("~"), '.oemof', 'data_files')
if not os.path.exists(basic_path):
    os.makedirs(basic_path)
data = pd.read_sql_table('test_data', con, 'app_reegis')
data.to_csv(os.path.join(basic_path, "reegis_example.csv"), sep=",")
data = pd.read_csv(os.path.join(basic_path, "reegis_example.csv"), sep=",")
timesteps = [t for t in range(8760)]
print(data.keys())
#engine = db.engine()
#data.to_sql("test_data", engine, schema="app_reegis")
###############################################################################
# set optimzation options for storage components
###############################################################################

transformer.Storage.optimization_options.update({'investment': True})

###############################################################################
# Create oemof objetc
###############################################################################

# create bus
bgas = Bus(uid="bgas",
           type="gas",
           price=70,
           balanced=True,
           excess=False)

# create electricity bus
bel = Bus(uid="bel",
          type="el",
          excess=True)

# create heat buses
district_heat_bus = Bus(uid="bus_distr_heat",
                        type="distr_heat",
                        excess=True)

district_heat_demand = sink.Simple(uid="demand_distr_heat",
                                   inputs=[district_heat_bus],
                                   val=data['dst0'])

bus_domestic_oil_heat = Bus(uid="bus_distr_heat",
                            type="distr_heat",
                            excess=True)

# create commodity object for gas resource
rgas = source.Commodity(uid='rgas',
                        outputs=[bgas],
                        sum_out_limit=194397000)

# create fixed source object for wind
wind = source.FixedSource(uid="wind",
                          outputs=[bel],
                          val=data['rwin'],
                          out_max=[1000],
                          add_out_limit=0,
                          capex=1000,
                          opex_fix=20,
                          lifetime=25,
                          crf=0.08)

# create fixed source object for pv
pv = source.FixedSource(uid="pv",
                        outputs=[bel],
                        val=data['rpvo'],
                        out_max=[582],
                        add_out_limit=0,
                        capex=900,
                        opex_fix=15,
                        lifetime=25,
                        crf=0.08)

# create simple sink object for demand
demand = sink.Simple(uid="demand", inputs=[bel], val=data['elec'])

# create simple transformer object for gas powerplant
pp_gas = transformer.CHP(uid='chp_gas',
                         inputs=[bgas], outputs=[bel, district_heat_bus],
                         opex_var=50, out_max=[0.3e10, 0.5e10], eta=[0.3, 0.5])

# create storage transformer object for storage
storage = transformer.Storage(uid='sto_simple',
                              inputs=[bel],
                              outputs=[bel],
                              eta_in=1,
                              eta_out=0.8,
                              cap_loss=0.00,
                              opex_fix=35,
                              opex_var=10e10,
                              capex=1000,
                              cap_max=0,
                              cap_initial=0,
                              c_rate_in=1/6,
                              c_rate_out=1/6)

###############################################################################
# Create, solve and postprocess OptimizationModel instance
###############################################################################

# group busses
buses = [bgas, bel]

# create lists of components
transformers = [pp_gas]
renewable_sources = [pv, wind]
commodities = [rgas]
storages = [storage]
sinks = [demand]

# groupt components
components = transformers + renewable_sources + storages + sinks + commodities

# create list of all entities
entities = components + buses

# TODO: other solver libraries should be passable
simulation = es.Simulation(
    solver='gurobi', timesteps=timesteps,
    stream_solver_output=True,
    objective_options={
        'function': predefined_objectives.minimize_cost})

energysystem = es.EnergySystem(entities=entities, simulation=simulation)
energysystem.year = 2010

energysystem.optimize()

prange = pd.date_range(pd.datetime(energysystem.year, 6, 1, 0, 0),
                       periods=336, freq='H')

# Initialise the plot object with the energy system
plot = devplots.stackplot(es=energysystem)

# Prepare the time series to plot the balance around the electricity bus
plot.plot_dc = plot.create_io_df(bel.uid)

# Plotting the bus balance with an own color set.
c_in = ['#4536bb', '#ffcc00', '#7c7c7c', '#ff5e5e']
c_out = ['#0cce1e', '#ff5e5e']
fig = plot.create_fig()
ax = fig.add_subplot(2, 1, 1)
plot.part(prange, ax, in_color=c_in, out_color=c_out)

ax = fig.add_subplot(2, 1, 2)
plot.plot_dc = plot.create_io_df(district_heat_bus.uid)
plot.part(prange, ax, in_color=c_in, out_color=c_out)
plt.show()
