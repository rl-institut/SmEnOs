#!/usr/bin/python3
# -*- coding: utf-8

import pandas as pd
import warnings
import logging
import os

from oemof import db
from oemof.tools import logger
from oemof.core import energy_system as es
from oemof.db import tools
#from oemof.db import feedin_pg
from oemof.solph import predefined_objectives as predefined_objectives
from oemof.core.network.entities import Bus
from oemof.core.network.entities.components import sinks as sink
from oemof.solph.optimization_model import OptimizationModel
from oemof.core.network.entities.components import transformers as transformer

import helper_gvm as hlsg

# choose scenario
scenario = 'gvm1'
schema = 'oemof_test'
table = 'gvm_transformer'

# Basic inputs
warnings.simplefilter(action="ignore", category=RuntimeWarning)
logger.define_logging()
year = 2010
time_index = pd.date_range('1/1/{0}'.format(year), periods=2, freq='H')
time_index_demandlib = pd.date_range(
    '1/1/{0}'.format(year), periods=8736, freq='H')
conn = db.connection()

########### get data ###########################################
filename = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                        'timeseries_gvm.csv'))
times_gvm = pd.read_csv(filename, delimiter=',')

price_el_ts = times_gvm['price_el']
print(price_el_ts)
refund_el_ts = price_el_ts * (-1)
eta_import = 1 / price_el_ts
print(eta_import)
el_demand_ts = times_gvm['el_demand']

dh_demand_ts = times_gvm['dh_demand']

############## Create a simulation object ########################
simulation = es.Simulation(
    timesteps=list(range(len(time_index))), verbose=True, solver='cbc',
    stream_solver_output=True,
    objective_options={'function': predefined_objectives.minimize_cost})

############## Create an energy system ###########################
Regions = es.EnergySystem(time_idx=time_index, simulation=simulation)

Regions.regions.append(es.Region(
        geom=hlsg.get_polygon_gvm(conn),
        name='GVM'))

# Add electricity and heat sinks and buses for each region
for region in Regions.regions:
    # create electricity bus
    Bus(uid="('bus', '"+region.name+"', 'elec')",
        type='elec',
        regions=[region],
        excess=True)
    # create buses for import and export
    Bus(uid="('bus', '"+region.name+"', 'import')",
        type='elec',
        regions=[region],
        balanced = False)
    Bus(uid="('bus', '"+region.name+"', 'export')",
        type='elec',
        regions=[region],
        balanced = False)
    # cretae transformer for import and export with variable efficiencies 
    # depending on eex costs
    transformer.Simple(
        uid=("('transformer', '"+region.name+"', 'export')"),
        inputs=[obj for obj in Regions.entities if obj.uid ==
                "('bus', '"+region.name+"', 'elec')"],
        outputs=[[obj for obj in region.entities if obj.uid ==
                 "('bus', '"+region.name+"', 'export')"][0]],
        in_max=[None],
        out_max=[1000000],
        eta=[price_el_ts],
        opex_var=-1,
        regions=[region])

    transformer.Simple(
        uid=("('transformer', '"+region.name+"', 'import')"),
        inputs=[obj for obj in Regions.entities if obj.uid ==
                "('bus', '"+region.name+"', 'import')"],
        outputs=[[obj for obj in region.entities if obj.uid ==
                 "('bus', '"+region.name+"', 'elec')"][0]],
        in_max=[None],
        out_max=[1000],
        eta=[eta_import],
        input_costs=1,
        regions=[region])

    # create districtheat bus
    Bus(uid="('bus', '"+region.name+"', 'dh')",
        type='dh',
        regions=[region],
        excess=True)

    # create electricity sink
    demand = sink.Simple(uid=('demand', region.name, 'elec'),
                         inputs=[obj for obj in region.entities if obj.uid ==
                                 "('bus', '"+region.name+"', 'elec')"],
                         val=el_demand_ts,
                         regions=[region])

    # create district heat sink
    demand = sink.Simple(uid=('demand', region.name, 'dh'),
                         inputs=[obj for obj in region.entities if obj.uid ==
                                 "('bus', '"+region.name+"', 'dh')"],
                         val=dh_demand_ts,
                         regions=[region])

# Add global buses
typeofgen_global = ['natural_gas', 'biogas', 'natural_gas_cc', 'lignite',
                    'oil', 'waste', 'hard_coal', 'sewage_gas']
# Add biomass bus for Berlin and Brandenburg
for typ in typeofgen_global:
    Bus(uid="('bus', 'global', '"+typ+"')",
        type=typ,
        excess=False, balanced=False,
        regions=Regions.regions)

# create all powerplants from database table
for region in Regions.regions:
    logging.info('Processing region: {0}'.format(region.name))

    hlsg.create_powerplants(Regions, region, conn, scenario, schema, table,
                            year)

# Remove orphan buses
buses = [obj for obj in Regions.entities if isinstance(obj, Bus)]
for bus in buses:
    if len(bus.inputs) > 0 or len(bus.outputs) > 0:
        logging.debug('Bus {0} has connections.'.format(bus.type))
    else:
        logging.debug('Bus {0} has no connections and will be deleted.'.format(
            bus.type))
        Regions.entities.remove(bus)


# print all entities of every region
for entity in Regions.entities:
    print(entity.uid)
    if entity.uid[0] == 'transformer' or entity.uid[0] == 'FixedSrc':
        print('out_max')
        print(entity.out_max)
        print('type(out_max)')
        print(type(entity.out_max))

# change uid tuples to strings
for entity in Regions.entities:
    entity.uid = str(entity.uid)
    print(entity.uid)

# Optimize the energy system
om = OptimizationModel(energysystem=Regions)
om.write_lp_file()
om.solve()
Regions.results = om.results()
logging.info(Regions.dump())
