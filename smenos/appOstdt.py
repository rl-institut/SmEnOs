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
import warnings
import numpy as np
import pickle

from oemof import db
from oemof.db import tools
from oemof.tools import logger
from oemof.core import energy_system as es
from oemof.solph import predefined_objectives as predefined_objectives
from oemof.solph.optimization_model import OptimizationModel
from oemof.core.network.entities import Bus
from oemof.core.network.entities.components import sinks as sink
from oemof.core.network.entities.components import transports as transport
from oemof.core.network.entities.components import sources as source
from oemof.core.network.entities.components import transformers as transformer

import helper_SmEnOs as hls
import feedin_offs
import helper_heat_pump as hhp

# Basic inputs
warnings.simplefilter(action="ignore", category=RuntimeWarning)
logger.define_logging()
year = 2010
time_index = pd.date_range('1/1/{0}'.format(year), periods=8760, freq='H')
overwrite = True
conn = db.connection()
Offshore_Scenario = 2016  # which parks are already running in this year
# there are some planned for 2017, 18 and 19

cap_initial = 0.0
chp_faktor_flex = 0.84  # share of flexible generation of CHP
max_biomass = 2500000 


# Create a simulation object
simulation = es.Simulation(
    timesteps=list(range(len(time_index))), verbose=True, solver='cbc',
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
typeofgen_global = ['natural_gas', 'natural_gas_cc', 'lignite', 'hard_coal',
                    'oil', 'waste']
(co2_emissions, co2_fix, eta_elec, eta_th, eta_th_chp, eta_el_chp,
 eta_chp_flex_el, sigma_chp, beta_chp, opex_var, opex_fix, capex,
 c_rate_in, c_rate_out, eta_in, eta_out,
 cap_loss, lifetime, wacc) = hls.get_parameters()

for typ in typeofgen_global:
    Bus(uid="('bus', 'global', '" +typ+ "')", type=typ, price=0,
        excess=False, balanced=False, regions=SmEnOsReg.regions)

# Add electricity sink for each region
demands_df = hls.get_demand(conn, tuple(regionsOstdt['nutsID']))
for region in SmEnOsReg.regions:
    # create electricity bus
    Bus(uid="('bus', '"+region.name+"', 'elec')", type='elec', price=0,
        regions=[region], excess=True, shortage=True, shortage_costs=1000000.0)
    # create biomass bus
    Bus(uid="('bus', '"+region.name+"', 'biomass')", type='biomass', price=0,
        regions=[region], excess=False, balanced=False, 
    sum_out_limit=max_biomass)

    # create districtheat bus
    Bus(uid="('bus', '"+region.name+"', 'dh')", type='dh', price=0,
        regions=[region], excess=True, shortage=True, shortage_costs=1000000.0)

    # create electricity sink
    demand = sink.Simple(uid=('demand', region.name, 'elec'),
                         inputs=[obj for obj in SmEnOsReg.entities
                                 if obj.uid ==
                                 "('bus', '"+region.name+"', 'elec')"],
                         region=region)

    # get regional electricity demand [MWh/a]
    nutID = regionsOstdt.query('abbr==@region.name')['nutsID'].values[0]
    el_demand = demands_df.query(
        'nuts_id==@nutID and energy=="electricity"').sum(axis=0)['demand']
    # create el. profile and write to sink object
    hls.call_el_demandlib(demand, method='scale_profile_csv', year=year,
                          path='', filename='50Hertz2010_y.csv',
                          annual_elec_demand=el_demand)

# Add heat sinks, buses and transformer for each region, sector and ressource

#TODO solar_heat_dec wird nicht betrachtet
heating_system_commodity = {
    'hard_coal_dec': 'hard_coal',  # bedarf und Bus
    'lignite_dec': 'lignite',
    'mineral_oil_dec': 'oil',
    'gas_dec': 'natural_gas',
    'waste_dec': 'waste'}
heat_params = hls.get_bdew_heatprofile_parameters()
(share_sfh_hp, share_ww, share_air_hp,
    share_heating_rod, share_heat_storage) = hls.get_hp_parameters()
am, pm, profile_factors = hls.ind_profile_parameters()
for region in SmEnOsReg.regions:
    print(region)
    # get regional heat demand for different ressources
    nutID = regionsOstdt.query('abbr==@region.name')['nutsID'].values[0]
    demand_sectors = demands_df.query('nuts_id==@nutID and energy=="heat"')
    # get temperature of region as np array [°C]
#    multiWeather = coastdat.get_weather(conn, region.geom, year)
#    temp = np.zeros([len(multiWeather[0].data.index), ])
#    for weather in multiWeather:
#        temp += weather.data['temp_air'].as_matrix()
#    temp = pd.Series(temp / len(multiWeather) - 273.15)
#    region.temp = temp
    temp = pd.read_pickle('temp')
    region.temp = temp
    # create empty dataframe for district heating demand
    dh_demand = pd.Series(0, index=time_index)
    # residential sector
    sec = 'residential'
    demand_sector = list(demand_sectors.query('sector==@sec')['demand'])[0]
    region.share_efh = heat_params.ix[region.name]['share_EFH']
    region.building_class = heat_params.ix[region.name]['building_class']
    region.wind_class = heat_params.ix[region.name]['wind_class']
    for ressource in list(demand_sector.keys()):
        if demand_sector[ressource] != 0:
            if ressource == 'heat_pump_dec':
                elec_bus = [obj for obj in SmEnOsReg.entities
                    if obj.uid == "('bus', '"+region.name+"', 'elec')"][0]
                hhp.create_hp_entities(region, year, demand_sector[ressource],
                    elec_bus, temp, share_sfh_hp, share_ww,
                    share_air_hp, share_heating_rod, share_heat_storage,
                    eta_th, eta_in, eta_out, cap_loss, opex_fix)
            elif ressource in list(heating_system_commodity.keys()):
                # create bus(bedarfsbus)
                Bus(uid=('bus', region.name, sec, ressource), type=ressource,
                    price=0, regions=[region], excess=False)
                # create sink
                demand = sink.Simple(uid=('demand', region.name, sec,
                    ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    region=region)
                # create heat load profile and write to sink object
                # heat load in [MWh/a]
                profile_efh = hls.call_heat_demandlib(region, year,
                    annual_heat_demand=(
                        demand_sector[ressource] * region.share_efh),
                    shlp_type='EFH', ww_incl=True)
                profile_mfh = hls.call_heat_demandlib(region, year,
                    annual_heat_demand=(demand_sector[ressource] *
                        (1 - region.share_efh)),
                    shlp_type='MFH', ww_incl=True)
                demand.val = (profile_efh + profile_mfh) * eta_th[ressource]
                # create transformer
                transformer.Simple(
                    uid=('transformer', region.name, sec, ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == "('bus', 'global', '" +
                            heating_system_commodity[ressource]+ "')"],
                    outputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    out_max=[max(demand.val)],
                    eta=[eta_th[ressource]],
                    regions=[region])
            elif ressource == 'biomass_dec':
                # create bus
                Bus(uid=('bus', region.name, sec, ressource), type=ressource,
                    price=0, regions=[region], excess=False)
                # create sink
                demand = sink.Simple(uid=('demand', region.name, sec,
                    ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    region=region)
                # create heat load profile and write to sink object
                # heat load in [MWh/a]
                profile_efh = hls.call_heat_demandlib(region, year,
                    annual_heat_demand=(
                        demand_sector[ressource] * region.share_efh),
                    shlp_type='EFH', ww_incl=True)
                profile_mfh = hls.call_heat_demandlib(region, year,
                    annual_heat_demand=(demand_sector[ressource] *
                        (1 - region.share_efh)),
                    shlp_type='MFH', ww_incl=True)
                demand.val = (profile_efh + profile_mfh) * eta_th[ressource]
                # create transformer
                transformer.Simple(
                    uid=('transformer', region.name, sec, ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == "('bus', '"+region.name+"', 'biomass')"],
                    outputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    out_max=[max(demand.val)],
                    eta=[eta_th[ressource]],
                    regions=[region])
            elif ressource == 'dh':
                # create heat load profile and write to sink object
                # heat load in [MWh/a]
                profile_efh = hls.call_heat_demandlib(region, year,
                    annual_heat_demand=(
                        demand_sector[ressource] * region.share_efh),
                    shlp_type='EFH', ww_incl=True)
                profile_mfh = hls.call_heat_demandlib(region, year,
                    annual_heat_demand=(demand_sector[ressource] *
                        (1 - region.share_efh)),
                    shlp_type='MFH', ww_incl=True)
                dh_demand += profile_efh + profile_mfh
            else:
                print('Folgender Bedarf wird nicht berücksichtigt:')
                print(region.name)
                print(sec)
                print(ressource)
    # commercial sector
    sec = 'commercial'
    demand_sector = list(demand_sectors.query('sector==@sec')['demand'])[0]
    region.wind_class = heat_params.ix[region.name]['wind_class']
    region.building_class = None
    for ressource in list(demand_sector.keys()):
        if demand_sector[ressource] != 0:
            if ressource in list(heating_system_commodity.keys()):
                # create bus
                Bus(uid=('bus', region.name, sec, ressource), type=ressource,
                    price=0, regions=[region], excess=False)
                # create sink
                demand = sink.Simple(uid=('demand', region.name, sec,
                    ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    region=region)
                # create heat load profile and write to sink object
                # heat load in [MWh/a]
                demand.val = hls.call_heat_demandlib(region, year,
                    annual_heat_demand=demand_sector[ressource],
                    shlp_type='GHD', ww_incl=True) * eta_th[ressource]
                # create transformer
                transformer.Simple(
                    uid=('transformer', region.name, sec, ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == "('bus', 'global', '" +
                            heating_system_commodity[ressource]+ "')"],
                    outputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    out_max=[max(demand.val)],
                    eta=[eta_th[ressource]],
                    regions=[region])
            elif ressource == 'biomass_dec':
                # create bus
                Bus(uid=('bus', region.name, sec, ressource), type=ressource,
                    price=0, regions=[region], excess=False)
                # create sink
                demand = sink.Simple(uid=('demand', region.name, sec,
                    ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    region=region)
                # create heat load profile and write to sink object
                # heat load in [MWh/a]
                demand.val = hls.call_heat_demandlib(region, year,
                    annual_heat_demand=demand_sector[ressource],
                    shlp_type='GHD', ww_incl=True) * eta_th[ressource]
                # create transformer
                transformer.Simple(
                    uid=('transformer', region.name, sec, ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == "('bus', '"+region.name+"', 'biomass')"],
                    outputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    out_max=[max(demand.val)],
                    eta=[eta_th[ressource]],
                    regions=[region])
            elif ressource == 'dh':
                # create heat load profile and write to sink object
                # heat load in [MWh/a]
                dh_demand += hls.call_heat_demandlib(region, year,
                    annual_heat_demand=demand_sector[ressource],
                    shlp_type='GHD', ww_incl=True)
            else:
                print('Folgender Bedarf wird nicht berücksichtigt:')
                print(region.name)
                print(sec)
                print(ressource)
    # industrial sector
    sec = 'industrial'
    demand_sector = list(demand_sectors.query('sector==@sec')['demand'])[0]
    for ressource in list(demand_sector.keys()):
        if demand_sector[ressource] != 0:
            if ressource in list(heating_system_commodity.keys()):
                # create bus
                Bus(uid=('bus', region.name, sec, ressource), type=ressource,
                    price=0, regions=[region], excess=False)
                # create sink
                demand = sink.Simple(uid=('demand', region.name, sec,
                    ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    region=region)
                # create heat load profile and write to sink object
                # heat load in [MWh/a]
                try:
                    eta_ressource = eta_th[ressource + '_ind']
                except:
                    eta_ressource = eta_th[ressource]
                demand.val = (hls.call_ind_profile(
                    year, demand_sector[ressource],
                    am=am, pm=pm, profile_factors=profile_factors) *
                    eta_ressource)
                # create transformer
                transformer.Simple(
                    uid=('transformer', region.name, sec, ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == "('bus', 'global', '" +
                            heating_system_commodity[ressource]+ "')"],
                    outputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    out_max=[max(demand.val)],
                    eta=[eta_ressource],
                    regions=[region])
            elif ressource == 'biomass_dec':
                # create bus
                Bus(uid=('bus', region.name, sec, ressource), type=ressource,
                    price=0, regions=[region], excess=False)
                # create sink
                demand = sink.Simple(uid=('demand', region.name, sec,
                    ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    region=region)
                # create heat load profile and write to sink object
                # heat load in [MWh/a]
                try:
                    eta_ressource = eta_th[ressource + '_ind']
                except:
                    eta_ressource = eta_th[ressource]
                demand.val = (hls.call_ind_profile(
                    year, demand_sector[ressource],
                    am=am, pm=pm, profile_factors=profile_factors) *
                    eta_ressource)
                # create transformer
                transformer.Simple(
                    uid=('transformer', region.name, sec, ressource),
                    inputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == "('bus', '"+region.name+"', 'biomass')"],
                    outputs=[obj for obj in SmEnOsReg.entities
                        if obj.uid == ('bus', region.name, sec, ressource)],
                    out_max=[max(demand.val)],
                    eta=[eta_ressource],
                    regions=[region])
            elif ressource == 'dh':
                # create heat load profile and write to sink object
                # heat load in [MWh/a]
                dh_demand += hls.call_ind_profile(
                    year, demand_sector[ressource],
                    am=am, pm=pm, profile_factors=profile_factors)

    # create dh sink
        # create sink
    demand = sink.Simple(uid=('demand', region.name,
                              ressource),
        inputs=[obj for obj in SmEnOsReg.entities
            if obj.uid == "('bus', '"+region.name+"', 'dh')"],
        region=region)
    demand.val = dh_demand

# Get run-of-river and pumped storage capacities
# ror_cap is Series with state abbr, capacity_mw and energy_mwh
ror_cap = hls.get_hydro_energy(conn, tuple(regionsOstdt['abbr']))
print('hydro energy:')
print(ror_cap)

# pumped_storage is Series with state abbr, power_mw and capacity_mwh
pumped_storage = hls.get_pumped_storage_pps(conn, tuple(regionsOstdt['abbr']))

# renewable parameters
site = hls.get_res_parameters()
site_os = hls.get_offshore_parameters(conn)

# add biomass in typeofgen because its needed to generate powerplants from db
typeofgen_global.append('biomass')

# create fixedSource object for Offshore WindPower
feedin_df, cap = feedin_offs.Feedin().aggregate_cap_val(
    conn, year=year, schema='oemof_test', table='baltic_wind_farms',
    start_year=Offshore_Scenario, bustype='elec', **site_os)

source.FixedSource(
    uid=('FixedSrc', 'MV', 'Offshore'),
    outputs=[obj for obj in SmEnOsReg.entities if obj.uid == 
        "('bus', 'MV', 'elec')"],
    val=feedin_df,
    out_max=[float(cap)])

for entity in SmEnOsReg.entities:
    if entity.uid[0] == 'FixedSrc':
        print(entity.outputs)


#region_fixed_source = SmEnOsReg.regions[0]
#feedin_df, cap = feedin_pg.Feedin().aggregate_cap_val(
#    conn, region=region_fixed_source, year=year, bustype='elec', **site)
#for stype in feedin_df.keys():
#    source.FixedSource(
#        uid=('FixedSrc', 'BE', stype),
#        outputs=[obj for obj in region.entities if obj.uid == (
#            'bus', 'BE', 'elec')],
#        val=feedin_df[stype],
#        out_max=[cap[stype]])
# Create entity objects for each region
status_Quo_EE = pickle.load(open("statusquoee.p", "rb"))
for region in SmEnOsReg.regions:
    logging.info('Processing region: {0}'.format(region.name))

    #TODO Problem mit Erdwärme??!!

#    feedin_df, cap = feedin_pg.Feedin().aggregate_cap_val(
#        conn, region=region, year=year, bustype='elec', **site)
#    feedin_df.to_csv('res_timeseries_smenos'+region.name+'_.csv')
    feedin_df = pd.read_csv(
        'res_timeseries_smenos'+region.name+'_.csv', delimiter=',', index_col=0)
    for stype in feedin_df.keys():
        source.FixedSource(
            uid=('FixedSrc', region.name, stype),
            outputs=[obj for obj in region.entities if obj.uid == 
                "('bus', '"+region.name+"', 'elec')"],
            val=feedin_df[stype],
            out_max=[status_Quo_EE[region.name][stype]])

    # Get power plants from database and write them into a DataFrame
    pps_df = hls.get_opsd_pps(conn, region.geom)
    hls.create_opsd_summed_objects(
        SmEnOsReg, region, pps_df,
        cap_initial=cap_initial,
        chp_faktor_flex=chp_faktor_flex,  # share of flexible generation of CHP
        typeofgen=typeofgen_global,
        ror_cap=ror_cap,
        pumped_storage=pumped_storage,
        filename_hydro='waterfeedin2010.csv')

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
#for entity in SmEnOsReg.entities:
#    if entity.uid[0] == 'transformer':
#        print(entity.uid)
#        print('opex')
#        print(entity.opex_var)


# Connect the electrical buses of federal states
ebusBB = [obj for obj in SmEnOsReg.entities if obj.uid ==
          "('bus', 'BB', 'elec')"][0]
ebusST = [obj for obj in SmEnOsReg.entities if obj.uid ==
          "('bus', 'ST', 'elec')"][0]
ebusBE = [obj for obj in SmEnOsReg.entities if obj.uid ==
          "('bus', 'BE', 'elec')"][0]
ebusMV = [obj for obj in SmEnOsReg.entities if obj.uid ==
          "('bus', 'MV', 'elec')"][0]
ebusTH = [obj for obj in SmEnOsReg.entities if obj.uid ==
          "('bus', 'TH', 'elec')"][0]
ebusSN = [obj for obj in SmEnOsReg.entities if obj.uid ==
          "('bus', 'SN', 'elec')"][0]

#TODO replace transport capacities
SmEnOsReg.connect(ebusBB, ebusST, in_max=5880, out_max=5880,
                  eta=eta_elec['transmission'],
                  transport_class=transport.Simple)
SmEnOsReg.connect(ebusBB, ebusBE, in_max=3000, out_max=3000,
                  eta=eta_elec['transmission'],
                  transport_class=transport.Simple)
SmEnOsReg.connect(ebusBB, ebusSN, in_max=5040, out_max=5040,
                  eta=eta_elec['transmission'],
                  transport_class=transport.Simple)
SmEnOsReg.connect(ebusBB, ebusMV, in_max=3640, out_max=3640,
                  eta=eta_elec['transmission'],
                  transport_class=transport.Simple)
SmEnOsReg.connect(ebusMV, ebusST, in_max=1960, out_max=1960,
                  eta=eta_elec['transmission'],
                  transport_class=transport.Simple)
SmEnOsReg.connect(ebusTH, ebusST, in_max=1680, out_max=1680,
                  eta=eta_elec['transmission'],
                  transport_class=transport.Simple)
SmEnOsReg.connect(ebusSN, ebusST, in_max=0, out_max=0,
                  eta=eta_elec['transmission'],
                  transport_class=transport.Simple)
SmEnOsReg.connect(ebusTH, ebusSN, in_max=6720, out_max=6720,
                  eta=eta_elec['transmission'],
                  transport_class=transport.Simple)

for entity in SmEnOsReg.entities:
    entity.uid = str(entity.uid)
    print(entity.uid)

# Optimize the energy system
om = OptimizationModel(energysystem=SmEnOsReg)
om.write_lp_file()
om.solve()
SmEnOsReg.results = om.results()
logging.info(SmEnOsReg.dump())

### Beispiel results aufrufen
##results_pv = SmEnOsReg.results[[obj for obj in SmEnOsReg.entities
    ##if obj.uid == ('bus', region.name, 'elec')][0]]
##results_pv_feedin = results_pv[[obj for obj in SmEnOsReg.entities
    ##if obj.uid == ('demand', region.name, 'elec')][0]]
##print (sum(results_pv_feedin))
#