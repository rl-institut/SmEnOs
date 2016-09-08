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

transmission = hls.get_data_from_csv('transmission_cap_'+scenario+'.csv')

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
        {'abbr': 'HF', 'nutsID': [
            'DE408', 'DE40E', 'DE40H', 'DE401', 'DE404']},
        {'abbr': 'OS', 'nutsID': ['DE409', 'DE40C', 'DE403']},
        {'abbr': 'LS', 'nutsID': [
            'DE406', 'DE407', 'DE40B', 'DE40G', 'DE402']},
        {'abbr': 'BE', 'nutsID': 'DE3'}],
    index=['Prignitz-Oberhavel', 'Uckermark-Barnim', u'Havelland-Fläming',
           'Oderland-Spree', 'Lausitz-Spreewald', 'Berlin'])
for index, row in regionsBB.iterrows():
    Regions.regions.append(es.Region(
        geom=tools.get_polygon_from_nuts(conn, row['nutsID']),
        name=row['abbr']))

# Add global buses
typeofgen_global = ['natural_gas', 'natural_gas_cc', 'lignite',
                    'oil', 'waste']

for typ in typeofgen_global:
    Bus(uid=('bus', 'global', typ), type=typ, price=0,
        excess=False, regions=Regions.regions)

# Add electricity sink and bus for each region
# TODO: anpassen!  demands_df = hls.get_demand(conn, tuple(regionsBB['nutsID']))
for region in Regions.regions:
    # create electricity bus
    Bus(uid=('bus', region.name, 'elec'), type='elec', price=0,
        regions=[region], excess=False)
    # create biomass bus
    Bus(uid=('bus', region.name, 'biomass'), type='biomass', price=0,
        regions=[region], excess=False)
    # create districtheat bus
    Bus(uid=('bus', region.name, 'dh'), type='dh', price=0,
        regions=[region], excess=False)
    # create electricity sink
    demand = sink.Simple(uid=('demand', region.name, 'elec'),
                         inputs=[obj for obj in Regions.entities
                                 if obj.uid == ('bus', region.name, 'elec')],
                         region=region)

# TODO:
#    # get regional electricity demand [MWh/a]
#    nutID = regionsBB.query('abbr==@region.name')['nutsID'].values[0]
#    el_demand = demands_df.query(
#        'nuts_id==@nutID and energy=="electricity"').sum(axis=0)['demand']
#    # create el. profile and write to sink object
#    hls.call_el_demandlib(demand, method='scale_profile_csv', year=year,
#                          path='', filename='50Hertz2010_y.csv',
#                          annual_elec_demand=el_demand)

# renewable parameters
site = hls.get_res_parameters()

# add biomass in typeofgen because its needed to generate powerplants from db
typeofgen_global.append('biomass')

for region in Regions.regions:
    logging.info('Processing region: {0}'.format(region.name))

    #TODO Problem mit Erdwärme??!!
# TODO: Leistungen pv und wind in transformer_bbb

    feedin_df, cap = feedin_pg.Feedin().aggregate_cap_val(
        conn, region=region, year=year, bustype='elec', **site)
    ee_capacities = hls.get_data_from_csv('ee_capacities_'+scenario+'.csv')
    for stype in feedin_df.keys():
        source.FixedSource(
            uid=('FixedSrc', region.name, stype),
            outputs=[obj for obj in region.entities if obj.uid == (
                'bus', region.name, 'elec')],
            val=feedin_df[stype],
            out_max=[ee_capacities[stype][region.name]])

    # Get power plants from database and write them into a DataFrame
    pps_df = hls.get_opsd_pps(conn, region.geom)
    hls.create_opsd_summed_objects(
        Regions, region, pps_df,
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

Import_Regions = ('MV', 'ST', 'SN', 'KJ')
Export_Regions = ('MV', 'ST', 'SN', 'KJ', 'BE')

for region_name in Import_Regions:
    Regions.regions.append(es.Region(name=region_name))

for region in Regions.regions:
    if region.name in Import_Regions:
        Bus(uid=('bus', region.name, 'elec'), type='elec', price=0,
            regions=[region], excess=False)
        source.Commodity(uid=('source', region.name, 'elec'),
                         regions=[region],
                         outputs=[obj for obj in region.entities if obj.uid == (
                             'bus', region.name, 'elec')],
                         opex_var=opex_var['import_el'])

# TODO: aktualisieren mit transmission, 
# Connect the electrical buses of federal states

for con in transmission:  # Zeilen in transmission-Tabelle
    reg1 = transmission[con]['from']  # zeile x,Spalte 'from',muss string sein!
    reg2 = transmission[con]['to']  # zeile x,Spalte 'from',muss string sein!
    ebus_1 = [obj for obj in Regions.entities if obj.uid ==
              "('bus', "+reg1+" , 'elec')"][0]
    ebus_2 = [obj for obj in Regions.entities if obj.uid ==
              "('bus', "+reg2+" , 'elec')"][0]
    Regions.connect(ebus_1, ebus_2,
                    in_max=transmission[con]['cap'],
                    out_max=transmission[con]['cap'],
                    eta=eta_elec['transmission'],
                    transport_class=transport.Simple)
#
#ebusPO = [obj for obj in Regions.entities if obj.uid ==
#          "('bus', 'PO', 'elec')"][0]
#ebusUB = [obj for obj in Regions.entities if obj.uid ==
#          "('bus', 'UB', 'elec')"][0]
#ebusOS = [obj for obj in Regions.entities if obj.uid ==
#          "('bus', 'OS', 'elec')"][0]
#ebusHF = [obj for obj in Regions.entities if obj.uid ==
#          "('bus', 'HF', 'elec')"][0]
#ebusLS = [obj for obj in Regions.entities if obj.uid ==
#          "('bus', 'LS', 'elec')"][0]
#ebusMV = [obj for obj in Regions.entities if obj.uid ==
#          "('bus', 'MV', 'elec')"][0]
#ebusST = [obj for obj in Regions.entities if obj.uid ==
#          "('bus', 'ST', 'elec')"][0]
#ebusSN = [obj for obj in Regions.entities if obj.uid ==
#          "('bus', 'SN', 'elec')"][0]
#ebusBE = [obj for obj in Regions.entities if obj.uid ==
#          "('bus', 'BE', 'elec')"][0]
#ebusKJ = [obj for obj in Regions.entities if obj.uid ==
#          "('bus', 'KJ', 'elec')"][0]
#
#Regions.connect(ebusPO, ebusOS,
#                in_max=transmission['PO']['OS'], out_max=transmission['PO']['OS'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusPO, ebusHF,
#                in_max=transmission['PO']['HF'], out_max=transmission['PO']['HF'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusUB, ebusOS,
#                in_max=transmission['UB']['OS'], out_max=transmission['UB']['OS'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusOS, ebusLS,
#                in_max=transmission['OS']['LS'], out_max=transmission['OS']['LS'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusHF, ebusLS,
#                in_max=transmission['HF']['LS'], out_max=transmission['HF']['LS'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusMV, ebusPO,
#                in_max=transmission['MV']['PO'], out_max=transmission['MV']['PO'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusMV, ebusUB,
#                in_max=transmission['MV']['UB'], out_max=transmission['MV']['UB'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusST, ebusPO,
#                in_max=transmission['ST']['PO'], out_max=transmission['ST']['PO'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusST, ebusHF,
#                in_max=transmission['ST']['HF'], out_max=transmission['ST']['HF'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusSN, ebusLS,
#                in_max=transmission['SN']['LS'], out_max=transmission['SN']['LS'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusBE, ebusOS,
#                in_max=transmission['BE']['OS'], out_max=transmission['BE']['OS'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusBE, ebusHF,
#                in_max=transmission['BE']['HF'], out_max=transmission['BE']['HF'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
#Regions.connect(ebusKJ, ebusUB,
#                in_max=transmission['KJ']['UB'], out_max=transmission['KJ']['UB'],
#                eta=eta_elec['transmission'],
#                transport_class=transport.Simple)
# Optimize the energy system
Regions.optimize()
logging.info(Regions.dump())
