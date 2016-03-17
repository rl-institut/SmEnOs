#!/usr/bin/python3
# -*- coding: utf-8

## todo: opex_var aufteilen???
## in_max bei Pumpspeicher - wo kommt der Wirkungsgrad rein?
## Zeitreihen Wasserkraft
## demandzeitreihen strom erstellen! achtung: standardlastprofile nicht gut, 
##      lieber 50 hertz oder entsoe
## umrechnen der wärmeverbräuche in installierte Leistungen der Heizungen
##    Faktor für Überdimensionierung + Faktor für Gleichzeitigkeit?
## prüfen bei feedinlib: absolute Zahlen oder normierte Zeitreihen???
## changed: pp[1].chp = "yes" or "no"
## pp[1].cap_el / cap_th
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

# Plant and site parameter
site = {'module_name': 'Yingli_YL210__2008__E__',
        'azimuth': 0,
        'tilt': 0,
        'albedo': 0.2,
        'hoy': 8760,
        'h_hub': 135,
        'd_rotor': 127,
        'wka_model': 'ENERCON E 126 7500',
        'h_hub_dc': {
            1: 135,
            2: 78,
            3: 98,
            4: 138,
            0: 135},
        'd_rotor_dc': {
            1: 127,
            2: 82,
            3: 82,
            4: 82,
            0: 127},
        'wka_model_dc': {
            1: 'ENERCON E 126 7500',
            2: 'ENERCON E 82 3000',
            3: 'ENERCON E 82 2300',
            4: 'ENERCON E 82 2300',
            0: 'ENERCON E 126 7500'},
        }
	
co2_emissions, co2_fix, eta_elec, eta_th, opex_var, capex, price = hls.get_parameters()
#print(eta_elec)	

# sources for power generation on global buses
typeofgen_global=['natural_gas', 'lignite', 'hard_coal', 'oil', 'waste']

logger.define_logging()
year = 2010
time_index = pd.date_range('1/1/{0}'.format(year), periods=8760, freq='H')
overwrite = False
overwrite = True
conn = db.connection()

# Create a simulation object
simulation = es.Simulation(
    timesteps=range(len(time_index)), verbose=True, solver='glpk', 
    stream_solver_output=True,
    objective_options={'function': predefined_objectives.minimize_cost})

# Create an energy system
SmEnOsReg = es.EnergySystem(time_idx=time_index, simulation=simulation)

# Add regions to the energy system
SmEnOsReg.regions.append(es.Region(
    geom=tools.get_polygon_from_nuts(conn, 'DE3'),
    name='BE'))
	# Berlin

SmEnOsReg.regions.append(es.Region(
    geom=tools.get_polygon_from_nuts(conn, 'DE4'),
    name='BB'))
	# Brandenburg
	
SmEnOsReg.regions.append(es.Region(
    geom=tools.get_polygon_from_nuts(conn, 'DE8'),
    name='MV'))
	# MeckPomm
	
SmEnOsReg.regions.append(es.Region(
    geom=tools.get_polygon_from_nuts(conn, 'DED'),
    name='SN'))
	# Sachsen
	
SmEnOsReg.regions.append(es.Region(
    geom=tools.get_polygon_from_nuts(conn, 'DEE'),
    name='ST'))
	# Sachsen-Anhalt
	
SmEnOsReg.regions.append(es.Region(
    geom=tools.get_polygon_from_nuts(conn, 'DEG'),
    name='TH'))
	# Thüringen

#global buses:
for typ in typeofgen_global:
    Bus(uid=('bus', 'global', typ), type=typ, price=price[typ],
    excess=False, regions=SmEnOsReg.regions ) # sum_out_limit=10e10
#Bus(uid=('bus', 'global', 'lignite'), type='lignite', price=0,
#    excess=False, regions=SmEnOsReg.regions )
#Bus(uid=('bus', 'global', 'mineral_oil'), type='mineral_oil', price=0,
#    excess=False, regions=SmEnOsReg.regions )
#Bus(uid=('bus', 'global', 'natural_gas'), type='natural_gas', price=0,
#    excess=False, regions=SmEnOsReg.regions )
#Bus(uid=('bus', 'global', 'waste'), type='waste', price=0,
#    excess=False, regions=SmEnOsReg.regions )


####################rausschmeißen, wenn es über ausgelagerte Funktion läuft!!!
demands_df = hls.get_dec_heat_demand(conn)
#print (demands_df)

    ####################################################################
    ##################  #######################

dbcode = {}
dbcode['BE'] = '362'
dbcode['BB'] = '365'
dbcode['MV'] = '422'
dbcode['SN'] = '590'
dbcode['ST'] = '607'
dbcode['TH'] = '640'

sectorcode = {}
sectorcode['el_I'] = 1
sectorcode['el_GHD'] = 2
sectorcode['el_HH'] = 3
sectorcode['th_I'] = 4
sectorcode['th_GHD'] = 5
sectorcode['th_HH'] = 6

###############################################################
###############################################################

# create el demandseries and electrical buses for each region
annual_el_dem = {}

for region in SmEnOsReg.regions:
    annual_el_dem[region.name] = {}
    for sec in ['el_I','el_GHD','el_HH']:
        annual_el_dem[region.name][sec] = int(demands_df.query(
        'sector == '+str(sectorcode[sec])+' and fstate =='+ dbcode[
        region.name])['demands']) 
    # sum up sectors demands for each federal state:    
    annual_el_dem[region.name]['el_all'] = (annual_el_dem[
        region.name]['el_I']) + (annual_el_dem[region.name][
        'el_GHD']) + (annual_el_dem[region.name]['el_HH'])
        
    # One elecbus for each region.#    
    Bus(uid=('bus', region.name, 'elec'), type='elec', price=60,
            regions=[region], excess=False)
    # One dh-bus for each region.#  (district heating)  
    Bus(uid=('bus', region.name, 'dh'), type='dh', price=60,
            regions=[region], excess=False)


    demand = sink.Simple(
            uid=("demand", region.name, 'elec'),
            inputs=[obj for obj in SmEnOsReg.entities
                    if obj.uid == ('bus', region.name, 'elec')],
            region = region)
#    demand.val = dm.electrical_demand(
#                     method='scale_profile_csv',
#                     year=2010,
#                     path='',
#                     filename='50Hertz2010.csv',
#                     annual_elec_demand = annual_el_dem[region.name]['el_all'])
    helpers.call_demandlib(demand,
            method='scale_profile_csv',
            year=year,
            path='',
            filename='50Hertz2010.csv',
            annual_elec_demand = annual_el_dem[region.name]['el_all'])
#print(demand.val)
print(SmEnOsReg.regions)
#########################################################
#######################################################

# Create entity objects for each region
for region in SmEnOsReg.regions:
    logging.info('Processing region: {0} ({1})'.format(
        region.name, region.code))

    # Create source object 
	#################################
	#################################

# ##achtung: kontrollieren!!!! absolute oder normierte Zeitreihen????
## todo: Problem mit Erdwärme??!!
#    feedin_pg.Feedin().create_fixed_source(
#        conn, region=region, year=SmEnOsReg.time_idx.year[0], bustype='elec', **site)

    # Get power plants from database and write them into a DataFrame
    pps_df = hls.get_opsd_pps(conn, region.geom)
    print(pps_df)
    #test = pps_df.query('cap_el_uba == NaN')
    #test2 = sum(test['cap_el'])
    #print(test)
    #print(test2)
    ########################################################TEST!!!!!

    # TODO: Summerize power plants of the same type
    hls.create_opsd_summed_objects(SmEnOsReg, region, pps_df, bclass=Bus,
                                   chp_faktor=float(0.2), typeofgen=typeofgen_global)
# alt:
#    for pwrp in pps_df.iterrows():
#        if pwrp[1].type != 'pumped_storage' and \
#        pwrp[1].type != 'biomass' and \
#        pwrp[1].status == 'operating':
#            hls.create_opsd_entity_objects(SmEnOsReg, region, pwrp,
#                bclass=Bus, filename_hydro='50Hertz2010.csv')
                
    #print(region.entities)
                    
                    
  #  for pwrp in pps_df.iterrows():
  #      if pwrp[1].type != 'pumped_storage':
  #          hls.create_kwdb_entity_objects(SmEnOsReg, region, pwrp,
  #                  tclass=transformer.Simple, bclass=Bus, path_hydro='',
#					filename_hydro='todo.csv')


    # create storage transformer object for storage
#    transformer.Storage.optimization_options.update({'investment': True})
 #   bel = [obj for obj in SmEnOsReg.entities
 #          if obj.uid == ('bus', region.name, 'elec')]
 #   transformer.Storage(uid=('sto_simple', region.name, 'elec'),
 #                       inputs=bel,
 #                       outputs=bel,
 #                       eta_in=1,
 #                       eta_out=0.8,
 #                       cap_loss=0.00,
 #                       opex_fix=35,
 #                       opex_var=0,
 #                       capex=1000,
 #                       cap_max=10 ** 12,
 #                       cap_initial=0,
 #                       c_rate_in=1/6,
 #                       c_rate_out=1/6)

# Connect the electrical buses of federal states

ebusBB = [obj for obj in SmEnOsReg.entities if obj.uid == (
    'bus', 'BB', 'elec')][0]
ebusST = [obj for obj in SmEnOsReg.entities if obj.uid == (
    'bus', 'ST', 'elec')][0]
ebusBE = [obj for obj in SmEnOsReg.entities if obj.uid == (
    'bus', 'BE', 'elec')][0]
ebusMV = [obj for obj in SmEnOsReg.entities if obj.uid == (
    'bus', 'MV', 'elec')][0]
ebusTH = [obj for obj in SmEnOsReg.entities if obj.uid == (
    'bus', 'TH', 'elec')][0]
ebusSN = [obj for obj in SmEnOsReg.entities if obj.uid == (
    'bus', 'SN', 'elec')][0]    
    
SmEnOsReg.connect(ebusBB, ebusST, in_max=5880, out_max=5880,
                      eta=0.997, transport_class=transport.Simple)
SmEnOsReg.connect(ebusBB, ebusBE, in_max=3000, out_max=3000,
                      eta=0.997, transport_class=transport.Simple)
SmEnOsReg.connect(ebusBB, ebusSN, in_max=5040, out_max=5040,
                      eta=0.997, transport_class=transport.Simple)                     
SmEnOsReg.connect(ebusBB, ebusMV, in_max=3640, out_max=3640,
                      eta=0.997, transport_class=transport.Simple)
SmEnOsReg.connect(ebusMV, ebusST, in_max=1960, out_max=1960,
                      eta=0.997, transport_class=transport.Simple)
SmEnOsReg.connect(ebusTH, ebusST, in_max=1680, out_max=1680,
                      eta=0.997, transport_class=transport.Simple)
SmEnOsReg.connect(ebusSN, ebusST, in_max=0, out_max=0,
                      eta=0.997, transport_class=transport.Simple)
SmEnOsReg.connect(ebusTH, ebusSN, in_max=6720, out_max=6720,
                      eta=0.997, transport_class=transport.Simple)                      
                      
#pv_lk_wtb = ([obj for obj in SmEnOsReg.entities if obj.uid == (
#    'FixedSrc', 'Landkreis Wittenberg', 'pv_pwr')][0])
#
# Multiply PV with 25
#pv_lk_wtb.val = pv_lk_wtb.val * 25

# Remove orphan buses
buses = [obj for obj in SmEnOsReg.entities if isinstance(obj, Bus)]
for bus in buses:
    if len(bus.inputs) > 0 or len(bus.outputs) > 0:
        logging.debug('Bus {0} has connections.'.format(bus.type))
    else:
        logging.debug('Bus {0} has no connections and will be deleted.'.format(
            bus.type))
        SmEnOsReg.entities.remove(bus)

for entity in SmEnOsReg.entities:
    entity.uid = str(entity.uid)
    

# Optimize the energy system
SmEnOsReg.optimize()

logging.info(SmEnOsReg.dump())
