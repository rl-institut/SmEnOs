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
## Skalierung 50Hertz Lastgang - Beachtung von Feiertagen?


import logging
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import warnings

from oemof import db
from oemof.db import tools
from oemof.db import feedin_pg
from oemof.db import coastdat
from oemof.tools import logger
from oemof.core import energy_system as es
from oemof.solph import predefined_objectives as predefined_objectives
from oemof.core.network.entities import Bus
from oemof.core.network.entities.components import sinks as sink
from oemof.core.network.entities.components import transports as transport

import helper_SmEnOs as hls


# Basic inputs
warnings.simplefilter(action="ignore", category=RuntimeWarning)
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
    price, c_rate) = hls.get_parameters()
for typ in typeofgen_global:
    Bus(uid=('bus', 'global', typ), type=typ, price=price[typ],
        excess=False, regions=SmEnOsReg.regions)

# Add electricity sink for each region
demands_df = hls.get_demand(conn, tuple(regionsOstdt['nutsID']))
for region in SmEnOsReg.regions:
    # create electricity bus
    Bus(uid=('bus', region.name, 'elec'), type='elec', price=0,
        regions=[region], excess=False)
    # create electricity sink
    demand = sink.Simple(uid=('demand', region.name, 'elec'),
        inputs=[obj for obj in SmEnOsReg.entities
            if obj.uid == ('bus', region.name, 'elec')], region=region)
    # get regional electricity demand [MWh/a]
    nutID = regionsOstdt.query('abbr==@region.name')['nutsID'].values[0]
    el_demand = demands_df.query(
        'nuts_id==@nutID and energy=="electricity"').sum(axis=0)['demand']
    # create el. profile and write to sink object
    hls.call_el_demandlib(demand, method='scale_profile_csv', year=year,
        path='', filename='50Hertz2010_y.csv', annual_elec_demand=el_demand)

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
    eta=eta_elec['transmission'], transport_class=transport.Simple)
SmEnOsReg.connect(ebusBB, ebusBE, in_max=3000, out_max=3000,
    eta=eta_elec['transmission'], transport_class=transport.Simple)
SmEnOsReg.connect(ebusBB, ebusSN, in_max=5040, out_max=5040,
    eta=eta_elec['transmission'], transport_class=transport.Simple)
SmEnOsReg.connect(ebusBB, ebusMV, in_max=3640, out_max=3640,
    eta=eta_elec['transmission'], transport_class=transport.Simple)
SmEnOsReg.connect(ebusMV, ebusST, in_max=1960, out_max=1960,
    eta=eta_elec['transmission'], transport_class=transport.Simple)
SmEnOsReg.connect(ebusTH, ebusST, in_max=1680, out_max=1680,
    eta=eta_elec['transmission'], transport_class=transport.Simple)
SmEnOsReg.connect(ebusSN, ebusST, in_max=0, out_max=0,
    eta=eta_elec['transmission'], transport_class=transport.Simple)
SmEnOsReg.connect(ebusTH, ebusSN, in_max=6720, out_max=6720,
    eta=eta_elec['transmission'], transport_class=transport.Simple)

# Add heat sinks for each region, sector and ressource
heat_params = hls.get_bdew_heatprofile_parameters()
for region in SmEnOsReg.regions:
    # get regional electricity demand
    nutID = regionsOstdt.query('abbr==@region.name')['nutsID'].values[0]
    demand_sectors = demands_df.query('nuts_id==@nutID and energy=="heat"')
    # get temperature of region as np array [°C]
    multiWeather = coastdat.get_weather(conn, region.geom, year)
    temp = np.zeros([len(multiWeather[0].data.index), ])
    for weather in multiWeather:
        temp += weather.data['temp_air'].as_matrix()
    temp = pd.Series(temp / len(multiWeather) - 273.15)
    region.temp = temp
    # residential sector
    sec = 'residential'
    demand_sector = list(demand_sectors.query('sector==@sec')['demand'])[0]
    for ressource in list(demand_sector.keys()):
        # create bus
        Bus(uid=('bus', region.name, sec, ressource), type=ressource,
            price=0, regions=[region], excess=False)
        # create sink
        demand = sink.Simple(uid=('demand', region.name, sec, ressource),
            inputs=[obj for obj in SmEnOsReg.entities
                if obj.uid == ('bus', region.name, sec, ressource)],
            region=region)
        # create heat load profile and write to sink object
        # heat load in [MWh/a]
        #TODO Umwandlungswirkungsgrade beachten!
        #TODO Bei WP Heizung und WW getrennt...
        region.share_efh = heat_params.ix[region.name]['share_EFH']
        region.building_class = heat_params.ix[region.name]['building_class']
        region.wind_class = heat_params.ix[region.name]['wind_class']
        profile_efh = hls.call_heat_demandlib(region, year,
            annual_heat_demand=demand_sector[ressource] * region.share_efh,
            shlp_type='EFH', ww_incl=True)
        profile_mfh = hls.call_heat_demandlib(region, year,
            annual_heat_demand=(demand_sector[ressource] *
                (1 - region.share_efh)),
            shlp_type='MFH', ww_incl=True)
        demand.val = profile_efh + profile_mfh
        #TODO Fehler Summe entspricht nicht dem vorgegebenen Demand

    # commercial sector
    sec = 'commercial'
    demand_sector = list(demand_sectors.query('sector==@sec')['demand'])[0]
    for ressource in list(demand_sector.keys()):
        # create bus
        Bus(uid=('bus', region.name, sec, ressource), type=ressource,
            price=0, regions=[region], excess=False)
        # create sink
        demand = sink.Simple(uid=('demand', region.name, sec, ressource),
            inputs=[obj for obj in SmEnOsReg.entities
                if obj.uid == ('bus', region.name, sec, ressource)],
            region=region)
        # create heat load profile and write to sink object
        # heat load in [MWh/a]
        #TODO Umwandlungswirkungsgrade beachten!
        region.wind_class = heat_params.ix[region.name]['wind_class']
        demand.val = hls.call_heat_demandlib(region, year,
            annual_heat_demand=demand_sector[ressource],
            shlp_type='GHD', ww_incl=True)

    # industrial sector
    sec = 'industrial'
    demand_sector = list(demand_sectors.query('sector==@sec')['demand'])[0]
    for ressource in list(demand_sector.keys()):
        # create bus
        Bus(uid=('bus', region.name, sec, ressource), type=ressource,
            price=0, regions=[region], excess=False)
        # create sink
        demand = sink.Simple(uid=('demand', region.name, sec, ressource),
            inputs=[obj for obj in SmEnOsReg.entities
                if obj.uid == ('bus', region.name, sec, ressource)],
            region=region)
        # create heat load profile and write to sink object
        # heat load in [MWh/a]
        #TODO Umwandlungswirkungsgrade beachten!
        #TODO Industrielastprofil einfügen
        region.wind_class = heat_params.ix[region.name]['wind_class']
        demand.val = hls.call_heat_demandlib(region, year,
            annual_heat_demand=demand_sector[ressource],
            shlp_type='GHD', ww_incl=True)
        ax = demand.val.plot()
        ax.set_xlabel("Hour of the year")
        ax.set_ylabel("Heat demand in MW")
        plt.show()

# Get run-of-river and pumped storage capacities
# ror_cap is Series with state abbr, capacity_mw and energy_mwh
ror_cap = hls.get_hydro_energy(conn, tuple(regionsOstdt['abbr']))
# pumped_storage is Series with state abbr, power_mw and capacity_mwh
pumped_storage = hls.get_pumped_storage_pps(conn, tuple(regionsOstdt['abbr']))
# renewable parameters
site = hls.get_res_parameters()

# Create entity objects for each region
for region in SmEnOsReg.regions:
    logging.info('Processing region: {0}'.format(region.name))

    #TODO kontrollieren absolute oder normierte Zeitreihen????
    # Sollten normierte Zeitreihen sein, feedin_pg erstellt aber absolute...
    #TODO Problem mit Erdwärme??!!
    #TODO CAPEX und OPEX fehlen noch
  #  feedin_pg.Feedin().create_fixed_source(
  #      conn, region=region, year=year, bustype='elec', **site)

    # Get power plants from database and write them into a DataFrame
    #TODO replace hard coded chp_faktor, cap_initial
    pps_df = hls.get_opsd_pps(conn, region.geom)
    hls.create_opsd_summed_objects(
            SmEnOsReg, region, pps_df,
            chp_faktor=0.2,
            cap_initial=0.0,
            typeofgen=typeofgen_global,
            ror_cap=ror_cap,
            pumped_storage=pumped_storage,
            filename_hydro='50Hertz2010.csv')

# Remove orphan buses
buses = [obj for obj in SmEnOsReg.entities if isinstance(obj, Bus)]
for bus in buses:
    if len(bus.inputs) > 0 or len(bus.outputs) > 0:
        logging.debug('Bus {0} has connections.'.format(bus.type))
    else:
        logging.debug('Bus {0} has no connections and will be deleted.'.format(
            bus.type))
        SmEnOsReg.entities.remove(bus)

# print all entities of every region
for entity in SmEnOsReg.entities:
    print(entity.uid)
    if entity.uid[0] == 'transformer' or entity.uid[0] == 'FixedSrc':
        print('out_max')
        print(entity.out_max)
        print('type(out_max)')
        print(type(entity.out_max))

# Optimize the energy system
SmEnOsReg.optimize()
logging.info(SmEnOsReg.dump())

## Beispiel results aufrufen
#results_pv = SmEnOsReg.results[[obj for obj in SmEnOsReg.entities
    #if obj.uid == ('bus', region.name, 'elec')][0]]
#results_pv_feedin = results_pv[[obj for obj in SmEnOsReg.entities
    #if obj.uid == ('demand', region.name, 'elec')][0]]
#print (sum(results_pv_feedin))