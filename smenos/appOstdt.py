#!/usr/bin/python3
# -*- coding: utf-8

## todo: opex_var aufteilen???
## in_max bei Pumpspeicher - wo kommt der Wirkungsgrad rein?
## Zeitreihen Wasserkraft
## umrechnen der wärmeverbräuche in installierte Leistungen der Heizungen
##    Faktor für Überdimensionierung + Faktor für Gleichzeitigkeit?
## prüfen bei feedinlib: absolute Zahlen oder normierte Zeitreihen???
## braunkohlekraftwerk Dessau: 150 MW wärme (bis jetzt in Datenbank nur strom,
##                          nicht wärme)



import logging
import pandas as pd
import numpy as np

from oemof import db
from oemof.db import tools
from oemof.db import powerplants as db_pps
from oemof.db import feedin_pg
from oemof.tools import logger
from oemof.core import energy_system as es
from oemof.solph import predefined_objectives as predefined_objectives
from oemof.core.network.entities import Bus
from oemof.core.network.entities.components import sources as source
from oemof.core.network.entities.components import sinks as sink
from oemof.core.network.entities.components import transformers as transformer
from oemof.core.network.entities.components import transports as transport
#neu:
import helper_SmEnOs as hls
from oemof.demandlib import demand as dm
#only for demand:
from oemof.tools import helpers

import warnings
warnings.simplefilter(action="ignore", category=RuntimeWarning)


# Basic inputs
logger.define_logging()
year = 2010
time_index = pd.date_range('1/1/{0}'.format(year), periods=8760, freq='H')
overwrite = True
conn = db.connection()

# Create a simulation object
simulation = es.Simulation(
    timesteps=list(range(len(time_index))), verbose=True, solver='glpk',
    stream_solver_output=True,
    objective_options={'function': predefined_objectives.minimize_cost})

# Create an energy system
SmEnOsReg = es.EnergySystem(time_idx=time_index, simulation=simulation)

# Add regions to the energy system
regionsOstdt = pd.DataFrame(
    [{'abbr': 'BE', 'nutsID': 'DE3'},
        {'abbr': 'BB', 'nutsID': 'DE4'},
        {'abbr': 'MV', 'nutsID': 'DE8'},
        {'abbr': 'SN', 'nutsID': 'DED'},
        {'abbr': 'ST', 'nutsID': 'DEE'},
        {'abbr': 'TH', 'nutsID': 'DEG'}],
    index=['Berlin', 'Brandenburg', 'Mecklenburg-Vorpommern',
        'Sachsen', 'Sachsen-Anhalt', u'Thüringen'])
for index, row in regionsOstdt.iterrows():
    SmEnOsReg.regions.append(es.Region(
        geom=tools.get_polygon_from_nuts(conn, row['nutsID']),
        name=row['abbr']))
# Add global buses
typeofgen_global = ['natural_gas', 'lignite', 'hard_coal', 'oil', 'waste']
(co2_emissions, co2_fix, eta_elec, eta_th, opex_var, capex,
    price) = hls.get_parameters()
for typ in typeofgen_global:
    Bus(uid=('bus', 'global', typ), type=typ, price=price[typ],
        excess=False, regions=SmEnOsReg.regions)
# Add electricity buses, district heating buses for each region
for region in SmEnOsReg.regions:
    # el
    Bus(uid=('bus', region.name, 'elec'), type='elec', price=0,
        regions=[region], excess=False)
    # dh
    Bus(uid=('bus', region.name, 'dh'), type='dh', price=0,
        regions=[region], excess=False)

# Add electricity sink for each region
demands_df = hls.get_demand(conn, tuple(regionsOstdt['nutsID']))
annual_el_dem = {}
for region in SmEnOsReg.regions:
    # create electricity sink
    demand = sink.Simple(uid=('demand', region.name, 'elec'),
        inputs=[obj for obj in SmEnOsReg.entities
            if obj.uid == ('bus', region.name, 'elec')], region = region)
    # get regional electricity demand
    nutID = regionsOstdt.query('abbr==@region.name')['nutsID'].values[0]
    el_demand = demands_df.query(
        'nuts_id==@nutID and energy=="electricity"').sum(axis=0)['demand']
    # create el. profile and write to sink object
    hls.call_demandlib(demand, method='scale_profile_csv', year=year, path='',
        filename='50Hertz2010_y.csv', annual_elec_demand=el_demand)

#{'TH': {'el_GHD': 3501389, 'el_HH': 2427253, 'el_I': 6052778, 'el_all': 11981420},
    #'BB': {'el_GHD': 4430278, 'el_HH': 3133230, 'el_I': 7041111, 'el_all': 14604619},
    #'BE': {'el_GHD': 4223056, 'el_HH': 3680877, 'el_I': 1886111, 'el_all': 9790044},
    #'MV': {'el_GHD': 8396100, 'el_HH': 6966378, 'el_I': 1608083, 'el_all': 16970561},
    #'SN': {'el_GHD': 3718056, 'el_HH': 4491697, 'el_I': 10094444, 'el_all': 18304197},
    #'ST': {'el_GHD': 2018056, 'el_HH': 2622503, 'el_I': 9607222, 'el_all': 14247781}}

#annual_heat_dem = {}
#for region in SmEnOsReg.regions:
    #annual_heat_dem[region.name] = {}
    #for sec in ['th_I', 'th_GHD', 'th_HH']:
        #annual_heat_dem[region.name][sec] = {}
        #annual_heat_dem_tmp = list(demands_df.query(
            #'sector == ' + str(sectorcode[sec]) + ' and fstate ==' + dbcode[
            #region.name])['demands'])[0]
        #print (annual_heat_dem_tmp)
        #for ressource in list(annual_heat_dem_tmp.keys()):
            #print (ressource)
            #annual_heat_dem[region.name][sec][ressource] = \
                #annual_heat_dem_tmp[ressource]
#print (annual_heat_dem)

#########################################################
#######################################################

#ror_cap = hls.get_small_runofriver_pps(conn)
#pumped_storage = hls.get_pumped_storage_pps(conn)

 #Create entity objects for each region
#for region in SmEnOsReg.regions:
    #logging.info('Processing region: {0} ({1})'.format(
        #region.name, region.code))
    #print('get Datafrom db')

 ###achtung: kontrollieren!!!! absolute oder normierte Zeitreihen????
## todo: Problem mit Erdwärme??!!
    #feedin_pg.Feedin().create_fixed_source(
        #conn, region=region, year=SmEnOsReg.time_idx.year[0], bustype='elec', **site)

     #Get power plants from database and write them into a DataFrame
    #pps_df = hls.get_opsd_pps(conn, region.geom)
    #print('got data from db')

    #hls.create_opsd_summed_objects(SmEnOsReg, region, pps_df, bclass=Bus,
                  #chp_faktor=float(0.2),
                    #typeofgen=typeofgen_global,
                    #ror_cap=ror_cap,
                    #pumped_storage=pumped_storage,
                    #filename_hydro='50Hertz2010.csv' )



 #Connect the electrical buses of federal states

#ebusBB = [obj for obj in SmEnOsReg.entities if obj.uid == (
    #'bus', 'BB', 'elec')][0]
#ebusST = [obj for obj in SmEnOsReg.entities if obj.uid == (
    #'bus', 'ST', 'elec')][0]
#ebusBE = [obj for obj in SmEnOsReg.entities if obj.uid == (
    #'bus', 'BE', 'elec')][0]
#ebusMV = [obj for obj in SmEnOsReg.entities if obj.uid == (
    #'bus', 'MV', 'elec')][0]
#ebusTH = [obj for obj in SmEnOsReg.entities if obj.uid == (
    #'bus', 'TH', 'elec')][0]
#ebusSN = [obj for obj in SmEnOsReg.entities if obj.uid == (
    #'bus', 'SN', 'elec')][0]

#SmEnOsReg.connect(ebusBB, ebusST, in_max=5880, out_max=5880,
                      #eta=0.997, transport_class=transport.Simple)
#SmEnOsReg.connect(ebusBB, ebusBE, in_max=3000, out_max=3000,
                      #eta=0.997, transport_class=transport.Simple)
#SmEnOsReg.connect(ebusBB, ebusSN, in_max=5040, out_max=5040,
                      #eta=0.997, transport_class=transport.Simple)
#SmEnOsReg.connect(ebusBB, ebusMV, in_max=3640, out_max=3640,
                      #eta=0.997, transport_class=transport.Simple)
#SmEnOsReg.connect(ebusMV, ebusST, in_max=1960, out_max=1960,
                      #eta=0.997, transport_class=transport.Simple)
#SmEnOsReg.connect(ebusTH, ebusST, in_max=1680, out_max=1680,
                      #eta=0.997, transport_class=transport.Simple)
#SmEnOsReg.connect(ebusSN, ebusST, in_max=0, out_max=0,
                      #eta=0.997, transport_class=transport.Simple)
#SmEnOsReg.connect(ebusTH, ebusSN, in_max=6720, out_max=6720,
                      #eta=0.997, transport_class=transport.Simple)


 #Remove orphan buses
#buses = [obj for obj in SmEnOsReg.entities if isinstance(obj, Bus)]
#for bus in buses:
    #if len(bus.inputs) > 0 or len(bus.outputs) > 0:
        #logging.debug('Bus {0} has connections.'.format(bus.type))
    #else:
        #logging.debug('Bus {0} has no connections and will be deleted.'.format(
            #bus.type))
        #SmEnOsReg.entities.remove(bus)

#for obj in SmEnOsReg.entities:
        #print(obj.uid)
        #if obj.uid[0] == 'transformer' or obj.uid[0] == 'FixedSrc':
            #print(obj.out_max)
            #print(type(obj.out_max))

#for entity in SmEnOsReg.entities:
    #entity.uid = str(entity.uid)


 #Optimize the energy system
#SmEnOsReg.optimize()

#logging.info(SmEnOsReg.dump())
#