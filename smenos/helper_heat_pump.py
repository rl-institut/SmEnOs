#!/usr/bin/python
# -*- coding: utf-8
import numpy as np


def calc_brine_supply_temp():
    """
    Returns the supply temperature for a brine heat pump.
    """
    t_brine = np.zeros((8760,))
    for i in range(8760):
        t_brine[i] = (3.42980994954735 * 10 ** -18) * i ** 5 \
                    - (6.28828944308818 * 10 ** -14) * i ** 4 \
                    + (2.44607151047512 * 10 ** -10) * i ** 3 \
                    + (6.25819661168072 * 10 ** -7) * i ** 2 \
                    - 0.0020535109 * i \
                    + 4.1855152734
    return t_brine


def calc_hp_heating_supply_temp(temp, heating_system, **kwargs):
    """
    Generates an hourly supply temperature profile depending on the ambient
    temperature.
    For ambient temperatures below t_heat_period the supply temperature
    increases linearly to t_sup_max with decreasing ambient temperature.

    Parameters
    temp -- pandas Series with ambient temperature
    heating_system -- string specifying the heating system (floor heating or
        radiator)
    """

    t_heat_period = kwargs.get('t_heat_period', 20)  # outdoor temp upto
                                                     # which is heated
    t_amb_min = kwargs.get('t_amb_min', -14)  # design outdoor temp

    # minimum and maximum supply temperature of heating system
    if heating_system == 'floor heating':
        t_sup_max = kwargs.get('t_sup_max', 35)
        t_sup_min = kwargs.get('t_sup_min', 20)
    elif heating_system == 'radiator':
        t_sup_max = kwargs.get('t_sup_max', 55)
        t_sup_min = kwargs.get('t_sup_min', 20)
    else:
        # TODO Raise Warning
        t_sup_max = kwargs.get('t_sup_max', 55)
        t_sup_min = kwargs.get('t_sup_min', 20)

    # calculate parameters for linear correlation for supply temp and
    # ambient temp
    slope = (t_sup_min - t_sup_max) / (t_heat_period - t_amb_min)
    y_intercept = t_sup_max - slope * t_amb_min

    # calculate supply temperature
    t_sup_heating = slope * temp + y_intercept
    t_sup_heating[t_sup_heating < t_sup_min] = t_sup_min
    t_sup_heating[t_sup_heating > t_sup_max] = t_sup_max

    return t_sup_heating


def cop_heating(temp, type_hp, **kwargs):
    """
    Returns the COP of a heat pump for heating.

    Parameters
    temp -- pandas Series with ambient or brine temperature
    type_hp -- string specifying the heat pump type (air or brine)
    """
    # share of heat pumps in new buildings
    share_hp_new_building = kwargs.get('share_hp_new_building', 0.5)
    # share of floor heating in old buildings
    share_fh_old_building = kwargs.get('share_fbh_old_building', 0.25)
    cop_max = kwargs.get('cop_max', 7)
    if type_hp == 'air':
        eta_g = kwargs.get('eta_g', 0.3)  # COP/COP_max
    elif type_hp == 'brine':
        eta_g = kwargs.get('eta_g', 0.4)  # COP/COP_max
    else:
        # TODO Raise Warning
        eta_g = kwargs.get('eta_g', 0.4)  # COP/COP_max
    # get supply temperatures
    t_sup_fh = calc_hp_heating_supply_temp(temp, 'floor heating')
    t_sup_radiator = calc_hp_heating_supply_temp(temp, 'radiator')
    # share of floor heating systems and radiators
    share_fh = (share_hp_new_building + (1 - share_hp_new_building) *
        share_fh_old_building)
    share_rad = (1 - share_hp_new_building) * (1.0 - share_fh_old_building)

    # calculate COP for floor heating and radiators
    cop_hp_heating_fh = eta_g * ((273.15 + t_sup_fh) / (t_sup_fh - temp))
    cop_hp_heating_fh[cop_hp_heating_fh > cop_max] = cop_max
    cop_hp_heating_rad = eta_g * ((273.15 + t_sup_radiator) /
        (t_sup_radiator - temp))
    cop_hp_heating_rad[cop_hp_heating_rad > cop_max] = cop_max
    cop_hp_heating = (share_fh * cop_hp_heating_fh + share_rad *
        cop_hp_heating_rad)

    return cop_hp_heating


def cop_ww(temp, ww_profile_sfh, ww_profile_mfh, **kwargs):
    """
    Returns the COP of a heat pump for warm water

    Parameters
    temp -- pandas Series with temperature profile (ambient or brine temp.)
    ww_profile_sfh -- pandas Dataframe with warm water profile for
        single family houses
    ww_profile_mfh -- pandas Dataframe with warm water profile for
        multi family houses
    """

    t_ww_sfh = kwargs.get('t_ww_sfh', 50)  # warm water temp. in SFH
    t_ww_mfh = kwargs.get('t_ww_mfh', 60)  # warm water temp. in MFH
    cop_max = kwargs.get('cop_max', 7)
    type_hp = kwargs.get('type_hp', 'air')
    if type_hp == 'air':
        eta_g = kwargs.get('eta_g', 0.3)  # COP/COP_max
    elif type_hp == 'brine':
        eta_g = kwargs.get('eta_g', 0.4)  # COP/COP_max
    else:
        # TODO Raise Warning
        eta_g = kwargs.get('eta_g', 0.4)  # COP/COP_max

    # calculate the share of the warm water demand of sfh and mfh for each hour
    share_sfh = ww_profile_sfh.values / (ww_profile_sfh.values +
        ww_profile_mfh.values)

    # calculates mixed WW supply temperature for single and multi family houses
    t_sup_ww = share_sfh * t_ww_sfh + (1 - share_sfh) * t_ww_mfh

    # calculate COP
    cop = eta_g * ((t_sup_ww + 273.15) / (t_sup_ww - temp))
    cop[cop > cop_max] = cop_max

    return cop


import pandas as pd

from oemof import db
from oemof.db import coastdat
from oemof.db import tools
from oemof.core import energy_system as es
from oemof.solph import predefined_objectives as predefined_objectives
from oemof.core.network.entities import Bus
from oemof.core.network.entities.components import sinks as sink
from oemof.core.network.entities.components import transformers as transformer
from oemof.db import feedin_pg
from oemof.core.network.entities.components import sources as source

import helper_SmEnOs as hls

year = 2010
time_index = pd.date_range('1/1/{0}'.format(year), periods=8760, freq='H')
conn = db.connection()

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

region = SmEnOsReg.regions[0]
demands_df = hls.get_demand(conn, tuple(regionsOstdt['nutsID']))
nutID = regionsOstdt.query('abbr==@region.name')['nutsID'].values[0]
demand_sectors = demands_df.query('nuts_id==@nutID and energy=="heat"')
sec = 'residential'
demand_sector = list(demand_sectors.query('sector==@sec')['demand'])[0]
heat_params = hls.get_bdew_heatprofile_parameters()
region.share_efh = heat_params.ix[region.name]['share_EFH']
region.building_class = heat_params.ix[region.name]['building_class']
region.wind_class = heat_params.ix[region.name]['wind_class']

(co2_emissions, co2_fix, eta_elec, eta_th, eta_th_chp, eta_el_chp,
 eta_chp_flex_el, sigma_chp, beta_chp, opex_var, opex_fix, capex,
 c_rate_in, c_rate_out, eta_in, eta_out,
 cap_loss) = hls.get_parameters()

# get temperature of region as np array [°C]
multiWeather = coastdat.get_weather(conn, region.geom, year)
temp = np.zeros([len(multiWeather[0].data.index), ])
for weather in multiWeather:
    temp += weather.data['temp_air'].as_matrix()
temp = pd.Series(temp / len(multiWeather) - 273.15)
region.temp = temp

ressource = 'heat_pump_dec'

# Add electricity sink for each region
demands_df = hls.get_demand(conn, tuple(regionsOstdt['nutsID']))
# create electricity bus
Bus(uid=('bus', region.name, 'elec'), type='elec', price=0,
    regions=[region], excess=False)

##TODO Wärmespeicher und Heizstab anlegen
## sole wp monovalent (also nur wp)
## luft wp monoenergetische (mit Heizstab)
## 85% der wp mit wärmespeicher, der spitzenlast 2h lang decken kann
## share_hp_new_building und share_fbh_old_building können evtl auch angepasst
## werden für ausbauszenarien (JAZ im Auge behalten)

### Inputs
## share of single family houses of all residential buildings that have a
## heat pump (share_mfh_hp = 1 - share_sfh_hp)
#share_sfh_hp = 1
#share_ww = 0.2  # share of warm water of total heating demand
## share of air hp of all heat pumps (share_brine_hp = 1 - share_air_hp)
#share_air_hp = 0.6  # Anm.: Sole-WP hauptsächlich in Neubauten, sodass Anteil
                    ## von Luft-WP bei Sanierungsszenarien steigt

### Calculation
## splitting of heat load profiles in sfh and mfh as well as ww and heating
#profile_sfh_heating = pd.Series(0, index=time_index)
#profile_mfh_heating = pd.Series(0, index=time_index)
#profile_sfh_ww = pd.Series(0, index=time_index)
#profile_mfh_ww = pd.Series(0, index=time_index)
## heat pump demand is derived from value for "Sonstige EE" from the energy
## balance under the assumption that this is mostly "Umgebungswärme" used in
## heat pumps with 2/3 environmental heat and 1/3 electricity
#demand_hp = demand_sector[ressource] * 1.5
#if share_sfh_hp != 0:
    #profile_sfh_heating = hls.call_heat_demandlib(
        #region, year,
        #annual_heat_demand=(
            #demand_hp * share_sfh_hp * (1 - share_ww)),
        #shlp_type='EFH', ww_incl=False)
    #profile_sfh_heating_ww = hls.call_heat_demandlib(
        #region, year,
        #annual_heat_demand=demand_hp * share_sfh_hp,
        #shlp_type='EFH', ww_incl=True)
    #profile_sfh_ww = profile_sfh_heating_ww - profile_sfh_heating

#if share_sfh_hp != 1:
    #profile_mfh_heating = hls.call_heat_demandlib(
        #region, year,
        #annual_heat_demand=(
            #demand_hp * (1 - share_sfh_hp) * (1 - share_ww)),
        #shlp_type='MFH', ww_incl=False)
    #profile_mfh_heating_ww = hls.call_heat_demandlib(
        #region, year,
        #annual_heat_demand=demand_hp * (1 - share_sfh_hp),
        #shlp_type='MFH', ww_incl=True)
    #profile_mfh_ww = profile_mfh_heating_ww - profile_mfh_heating

## create buses and sinks for each heat pump as well as heating and ww
#if share_air_hp != 0:
    ## air heat pump heating
        ## bus
    #Bus(uid=('bus', region.name, sec, ressource, 'air', 'heating'),
        #type=ressource, price=0, regions=[region], excess=False)
        ## sink
    #demand = sink.Simple(
        #uid=('demand', region.name, sec, ressource, 'air', 'heating'),
        #inputs=[obj for obj in SmEnOsReg.entities
            #if obj.uid == (
                #'bus', region.name, sec, ressource, 'air', 'heating')],
        #region=region)
    #demand.val = (profile_sfh_heating + profile_mfh_heating) * share_air_hp
        ## transformer
    #cop = cop_heating(temp, 'air')
    #transformer.Simple(
        #uid=('transformer', region.name, 'hp', 'air', 'heating'),
        #inputs=[obj for obj in SmEnOsReg.entities
            #if obj.uid == ('bus', region.name, 'elec')],
        #outputs=[[obj for obj in region.entities if obj.uid == (
            #'bus', region.name, sec, ressource, 'air', 'heating')][0]],
        #in_max=[None],
        #out_max=[max(demand.val)],
        #eta=[cop],
        #opex_var=opex_var['heat_pump_dec'],
        #regions=[region])
    ## air heat pump warm water
        ## bus
    #Bus(uid=('bus', region.name, sec, ressource, 'air', 'ww'),
        #type=ressource, price=0, regions=[region], excess=False)
        ## sink
    #demand = sink.Simple(
        #uid=('demand', region.name, sec, ressource, 'air', 'ww'),
        #inputs=[obj for obj in SmEnOsReg.entities
            #if obj.uid == (
                #'bus', region.name, sec, ressource, 'air', 'ww')],
        #region=region)
    #demand.val = (profile_sfh_ww + profile_mfh_ww) * share_air_hp
        ## transformer
    #cop = cop_ww(temp, profile_sfh_ww, profile_mfh_ww)
    #transformer.Simple(
        #uid=('transformer', region.name, 'hp', 'air', 'ww'),
        #inputs=[obj for obj in SmEnOsReg.entities
            #if obj.uid == ('bus', region.name, 'elec')],
        #outputs=[[obj for obj in region.entities if obj.uid == (
            #'bus', region.name, sec, ressource, 'air', 'ww')][0]],
        #in_max=[None],
        #out_max=[max(demand.val)],
        #eta=[cop],
        #opex_var=opex_var['heat_pump_dec'],
        #regions=[region])
#if share_air_hp != 1:
    ## brine heat pump heating
    #Bus(uid=('bus', region.name, sec, ressource, 'brine', 'heating'),
        #type=ressource, price=0, regions=[region], excess=False)
    #demand = sink.Simple(
        #uid=('demand', region.name, sec, ressource, 'brine', 'heating'),
        #inputs=[obj for obj in SmEnOsReg.entities
            #if obj.uid == (
                #'bus', region.name, sec, ressource, 'brine', 'heating')],
        #region=region)
    #demand.val = ((profile_sfh_heating + profile_mfh_heating) *
        #(1 - share_air_hp))
    ## brine heat pump warm water
    #Bus(uid=('bus', region.name, sec, ressource, 'brine', 'ww'),
        #type=ressource, price=0, regions=[region], excess=False)
    #demand = sink.Simple(
        #uid=('demand', region.name, sec, ressource, 'brine', 'ww'),
        #inputs=[obj for obj in SmEnOsReg.entities
            #if obj.uid == (
                #'bus', region.name, sec, ressource, 'brine', 'ww')],
        #region=region)
    #demand.val = (profile_sfh_ww + profile_mfh_ww) * (1 - share_air_hp)

site = hls.get_res_parameters()
feedin_df, cap = feedin_pg.Feedin().aggregate_cap_val(
    conn, region=region, year=year, bustype='elec', **site)
for stype in feedin_df.keys():
    print(feedin_df[stype].values)
    source.FixedSource(
        uid=('FixedSrc', region.name, stype),
        outputs=[obj for obj in region.entities if obj.uid == (
            'bus', region.name, 'elec')],
        val=feedin_df[stype].values,
        out_max=[cap[stype]])

# Optimize the energy system
SmEnOsReg.optimize()
#logging.info(SmEnOsReg.dump())