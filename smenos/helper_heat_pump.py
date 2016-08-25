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
    For ambient temperatures below t_heat_period the supply temperature increases
    linearly to t_sup_max with decreasing ambient temperature.

    Parameters
    temp -- pandas Series with ambient temperature
    heating_system -- string specifying the heating system (floor heating or radiator)
    """

    t_heat_period = kwargs.get('t_heat_period', 20)  # outdoor temp upto which is heated
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

    # calculate parameters for linear correlation for supply temp and ambient temp
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
    share_hp_new_building = kwargs.get('share_hp_new_building', 0.5)  # share of heat pumps in new buildings
    share_fh_old_building = kwargs.get('share_fbh_old_building', 0.25)  # share of floor heating in old buildings
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
    share_fh = share_hp_new_building + (1 - share_hp_new_building) * share_fh_old_building
    share_rad = (1 - share_hp_new_building) * (1.0 - share_fh_old_building)

    # calculate COP for floor heating and radiators
    cop_hp_heating_fh = eta_g * ((273.15 + t_sup_fh) / (t_sup_fh - temp))
    cop_hp_heating_fh[cop_hp_heating_fh > cop_max] = cop_max
    cop_hp_heating_rad = eta_g * ((273.15 + t_sup_radiator) / (t_sup_radiator - temp))
    cop_hp_heating_rad[cop_hp_heating_rad > cop_max] = cop_max
    cop_hp_heating = share_fh * cop_hp_heating_fh + share_rad * cop_hp_heating_rad

    return cop_hp_heating


def cop_ww(temp, ww_profile_sfh, ww_profile_mfh, **kwargs):
    """
    Returns the COP of a heat pump for warm water

    Parameters
    temp -- pandas Series with temperature profile (ambient or brine temp.)
    ww_profile_sfh -- pandas Dataframe with warm water profile for single family houses
    ww_profile_mfh -- pandas Dataframe with warm water profile for multi family houses
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
    share_sfh = ww_profile_sfh.values / (ww_profile_sfh.values + ww_profile_mfh.values)

    # calculates mixed WW supply temperature for single and multi family houses
    t_sup_ww = share_sfh * t_ww_sfh + (1 - share_sfh) * t_ww_mfh

    # calculate COP
    cop = eta_g * ((t_sup_ww + 273.15) / (t_sup_ww - temp))
    ones = np.ones((8760, 1))
    cop[cop > cop_max] = cop_max

    return cop


import pandas as pd

from oemof import db
from oemof.db import coastdat
from oemof.db import tools
from oemof.core import energy_system as es
from oemof.solph import predefined_objectives as predefined_objectives

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

# get temperature of region as np array [°C]
multiWeather = coastdat.get_weather(conn, region.geom, year)
temp = np.zeros([len(multiWeather[0].data.index), ])
for weather in multiWeather:
    temp += weather.data['temp_air'].as_matrix()
temp = pd.Series(temp / len(multiWeather) - 273.15)
region.temp = temp

ressource = 'heat_pump_dec'
profile_efh = hls.call_heat_demandlib(region, year,
                                      annual_heat_demand=demand_sector[ressource] * region.share_efh,
                                      shlp_type='EFH', ww_incl=True)
profile_mfh = hls.call_heat_demandlib(region, year,
                                      annual_heat_demand=(demand_sector[ressource] *
                                                          (1 - region.share_efh)),
                                      shlp_type='MFH', ww_incl=True)

# print((profile_efh))
# cop_ww_hp(temp, profile_efh, profile_mfh, type_HP='air')
calc_cop_hp_air_heating(temp, 'air')