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

import helper_SmEnOs as hls
import helper_BBB as hlsb

# choose scenario
scenario = 'ES2030'

# Basic inputs
warnings.simplefilter(action="ignore", category=RuntimeWarning)
logger.define_logging()
year = 2010
time_index = pd.date_range('1/1/{0}'.format(year), periods=100, freq='H')
conn = db.connection()
conn_oedb = db.connection(section='open_edb')

########### get data ###########################################

cap_initial = 0.0
chp_faktor_flex = 0.84  # share of flexible generation of CHP
max_biomass = 19333333
# parameters
(co2_emissions, co2_fix, eta_elec, eta_th, eta_th_chp, eta_el_chp,
 eta_chp_flex_el, sigma_chp, beta_chp, opex_var, opex_fix, capex,
 c_rate_in, c_rate_out, eta_in, eta_out,
 cap_loss, lifetime, wacc) = hlsb.get_parameters(conn_oedb)

print(eta_elec)

transmission = hlsb.get_transmission(conn_oedb, scenario)
demands_df = hlsb.get_demand(conn_oedb, scenario)
transformer = hlsb.get_transformer(conn_oedb, scenario)
st = hlsb.get_st_timeline(conn, year)
print(st.sum())

############## Create a simulation object ########################
simulation = es.Simulation(
    timesteps=list(range(len(time_index))), verbose=True, solver='cbc',
    stream_solver_output=True,
    objective_options={'function': predefined_objectives.minimize_cost})

############## Create an energy system ###########################
Regions = es.EnergySystem(time_idx=time_index, simulation=simulation)

regionsBBB = pd.DataFrame(
    [{'abbr': 'PO', 'nutsID': ['DE40F', 'DE40D', 'DE40A']},
        {'abbr': 'UB', 'nutsID': ['DE40I', 'DE405']},
        {'abbr': 'HF', 'nutsID': [
            'DE408', 'DE40E', 'DE40H', 'DE401', 'DE404']},
        {'abbr': 'OS', 'nutsID': ['DE409', 'DE40C', 'DE403']},
        {'abbr': 'LS', 'nutsID': [
            'DE406', 'DE407', 'DE40B', 'DE40G', 'DE402']},
        {'abbr': 'BE', 'nutsID': 'DE3'}],
    index=['Prignitz-Oberhavel', 'Uckermark-Barnim', u'Havelland-Fläming',
           'Oderland-Spree', 'Lausitz-Spreewald', 'Berlin'])

for index, row in regionsBBB.iterrows():
    Regions.regions.append(es.Region(
        geom=tools.get_polygon_from_nuts(conn, row['nutsID']),
        name=row['abbr']))

region_bb = []
for region in Regions.regions:
    if region.name == 'BE':
        region_ber = region
    else:
        region_bb.append(region)  # list

# Add global buses
typeofgen_global = ['natural_gas', 'natural_gas_cc', 'lignite',
                    'oil', 'waste']

for typ in typeofgen_global:
    Bus(uid="('bus', 'global', '"+typ+"')", type=typ, price=0,
        excess=False, balanced=False, regions=Regions.regions)

# Add electricity sink and bus for each region
for region in Regions.regions:
    # create electricity bus
    Bus(uid="('bus', '"+region.name+"', 'elec')", type='elec', price=0,
        regions=[region], excess=True, shortage=True, shortage_costs=1000000.0)

    # create districtheat bus
    Bus(uid="('bus', '"+region.name+"', 'dh')", type='dh', price=0,
        regions=[region], excess=True, shortage=True, shortage_costs=1000000.0)

    # create electricity sink
    demand = sink.Simple(uid=('demand', region.name, 'elec'),
                         inputs=[obj for obj in region.entities if obj.uid ==
                                 "('bus', '"+region.name+"', 'elec')"],
                         regions=[region])
    el_demands = {}
    el_demands['h0'] = float(demands_df.query(
        'region==@region.name and sector=="HH" and type=="electricity"')[
        'demand'])
    el_demands['g0'] = float(demands_df.query(
        'region==@region.name and sector=="GHD" and type=="electricity"')[
        'demand'])
    el_demands['g1'] = 0
    el_demands['g2'] = 0
    el_demands['g3'] = 0
    el_demands['g4'] = 0
    el_demands['g5'] = 0
    el_demands['g6'] = 0
    el_demands['i0'] = float(demands_df.query(
        'region==@region.name and sector=="IND" and type=="electricity"')[
        'demand'])
    hls.call_el_demandlib(demand, method='calculate_profile', year=year,
                          ann_el_demand_per_sector=el_demands)

# Add biomass bus for Berlin and Brandenburg
Bus(uid="('bus', 'BB', 'biomass')",
    type='biomass',
    price=0,
    balanced=False,
    sum_out_limit=max_biomass,
    regions=region_bb,
    excess=False)

Bus(uid="('bus', 'BE', 'biomass')",
    type='biomass',
    price=0,
    balanced=False,
    regions=[region_ber],
    excess=False)

################# create transformers ######################
# renewable parameters
site = hls.get_res_parameters()

# add biomass and powertoheat in typeofgen
# because its needed to generate powerplants from db
typeofgen_global.append('biomass')
typeofgen_global.append('powertoheat')

for region in Regions.regions:
    logging.info('Processing region: {0}'.format(region.name))

    #TODO Problem mit Erdwärme??!!

    feedin_df, cap = feedin_pg.Feedin().aggregate_cap_val(
        conn, region=region, year=year, bustype='elec', **site)
    ee_capacities = {}
    ee_capacities['pv_pwr'] = float(transformer.query(
        'region==@region.name and ressource=="pv"')['power'])
    ee_capacities['wind_pwr'] = float(transformer.query(
        'region==@region.name and ressource=="wind"')['power'])

    opex = {}
    opex['pv_pwr'] = opex_fix['solar_power']
    opex['wind_pwr'] = opex_fix['wind_power']

    for stype in feedin_df.keys():
        source.FixedSource(
            uid=('FixedSrc', region.name, stype),
            outputs=[obj for obj in region.entities if obj.uid ==
                     "('bus', '"+region.name+"', 'elec')"],
            val=feedin_df[stype],
            out_max=[ee_capacities[stype]],
            opex_fix=opex[stype])

    # Get power plants from database and write them into a DataFrame
# TODO: anpassen!!
    hlsb.create_transformer(
        Regions, region, transformer, conn=conn_oedb,
        cap_initial=cap_initial,
        chp_faktor_flex=chp_faktor_flex,  # share of flexible generation of CHP
        typeofgen=typeofgen_global)

# Remove orphan buses
buses = [obj for obj in Regions.entities if isinstance(obj, Bus)]
for bus in buses:
    if len(bus.inputs) > 0 or len(bus.outputs) > 0:
        logging.debug('Bus {0} has connections.'.format(bus.type))
    else:
        logging.debug('Bus {0} has no connections and will be deleted.'.format(
            bus.type))
        Regions.entities.remove(bus)

Import_Regions = ('MV', 'ST', 'SN', 'KJ')
Export_Regions = ('MV', 'ST', 'SN', 'KJ', 'BE')

for region_name in Import_Regions:
    Regions.regions.append(es.Region(name=region_name))

for region in Regions.regions:
    if region.name in Import_Regions:
        Bus(uid="('bus', '"+region.name+"', 'elec')", type='elec',
            shortage=True,
            shortage_costs=opex_var['import_el'],
            regions=[region],
            excess=False)

# print all entities of every region
for entity in Regions.entities:
    print(entity.uid)
    if entity.uid[0] == 'transformer' or entity.uid[0] == 'FixedSrc':
        print('out_max')
        print(entity.out_max)
        print('type(out_max)')
        print(type(entity.out_max))

# Connect the electrical buses of federal states

# change uid tuples to strings
for entity in Regions.entities:
    entity.uid = str(entity.uid)

for con in transmission['from']:  # Zeilen in transmission-Tabelle
    reg1 = transmission['from'][con]  # zeile x,Spalte 'from'
    print(reg1)
    reg2 = transmission['to'][con]  # zeile x,Spalte 'from'
    capacity = transmission['cap'][con]
    for entity in Regions.entities:
        if entity.uid == "('bus', '"+reg1+"', 'elec')":
            ebus_1 = entity
        if entity.uid == "('bus', '"+reg2+"', 'elec')":
            ebus_2 = entity
    print(ebus_1)
    print(ebus_2)
    Regions.connect(ebus_1, ebus_2,
                    in_max=capacity,
                    out_max=capacity,
                    eta=0.985,  # TODO: eta_elec['transmission'],
                    transport_class=transport.Simple)

# Optimize the energy system
Regions.optimize()
logging.info(Regions.dump())
