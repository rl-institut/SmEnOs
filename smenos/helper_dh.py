#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import pandas as pd

import helper_heat_pump as hhp
from oemof.core.network.entities.components import transformers as transformer
from oemof.core.network.entities import Bus


def calc_supply_temp(temp, **kwargs):
    '''
    Generates an hourly supply temperature profile depending on the ambient
    temperature.
    For ambient temperatures above T_heat_period the load for tap water
    preparation dominates the heat laod. The district heating system is then
    mass flow controlled and the supply temperature kept at a constant
    temperature of T_supply_min.
    For ambient temperatures below T_heat_period the supply temperature
    increases linearly to T_supply_max with decreasing ambient temperature.
    
    Parameters
    -----------
    temp : pandas Series
    ---------------------
    kwargs : T_supply_max, T_supply_min, T_heat_period, T_amb_min
    '''
    T_supply_max = kwargs.get('T_supply_max', 135)  # max supply temperature
    T_supply_min = kwargs.get('T_supply_min', 70)  # min supply temperature
    T_heat_period = kwargs.get('T_heat_period', 15)  # amb. temp. at which heating
                                                     # systems are turned on
    T_amb_min = kwargs.get('T_amb_design', -15)  # amb. temp. where max. supply
                                                 # temp. is reached

    # linear correlation between Q and T_sup
    T_supply = pd.Series(0, index=temp.index)
    slope = (T_supply_min - T_supply_max) / (T_heat_period - T_amb_min)
    y_intercept = T_supply_max - slope * T_amb_min

    T_supply = slope * temp + y_intercept
    T_supply[T_supply < T_supply_min] = T_supply_min
    T_supply[T_supply > T_supply_max] = T_supply_max

    return T_supply


def create_heat_storage_entity(uid, cap, dh_bus_entity, region, **kwargs):
    '''
    Function to create a district heating heat storage entity.
    
    Parameters
    -----------
    uid : string
        unique id of entity
    cap : float
        maximum capacity of heat storage
    dh_bus_entity : oemof.core bus object
        district heating bus entity
    region : oemof.core Region object
        region entity
    ----------------------------------------
    kwargs : cap_min, out_max, in_max, eta_in, eta_out, cap_loss, cap_initial,
        opex_fix, opex_var, capex
    '''
    transformer.Storage(uid=uid,
                        inputs=[dh_bus_entity],
                        outputs=[dh_bus_entity],
                        cap_max=cap,
                        cap_min=kwargs.get('cap_min', None),
                        out_max=kwargs.get('out_max', cap/2),
                        in_max=kwargs.get('in_max', kwargs.get(
                            'out_max', cap/2)),
                        eta_in=kwargs.get('eta_in', 1),
                        eta_out=kwargs.get('eta_out', 1),
                        cap_loss=kwargs.get('cap_loss', 0),
                        cap_initial=kwargs.get('cap_initial', None),
                        opex_fix=kwargs.get('opex_fix', 0),
                        opex_var=kwargs.get('opex_var', 0),
                        capex=kwargs.get('capex', 0),
                        regions=[region])
    return
    

def calc_cop(supply_temp, max_supply_temp_hp, type_hp, **kwargs):
    """
    Returns the COP of a district heating heat pump.

    Parameters
    -----------
    supply_temp : pandas Series
        supply temperature of district heating system
    max_supply_temp_hp : float or numpy array or pandas Series
        maximum supply temperature from heat pump to district heating system
    type_hp : string
        heat pump type (air or brine; in case of another heat source the
        heat_source_temp must be given)
    -------------------------------------
    kwargs : cop_max, eta_g, heat_source_temp
    """
    # get efficiency and supply temperature of heat source
    if type_hp == 'air':
        eta_g = kwargs.get('eta_g', 0.3)  # COP/COP_max
        #TODO change retrieving of air temperature profile
        heat_source_temp = kwargs.get('heat_source_temp', None)
        if heat_source_temp is None:
            filename = os.path.abspath(
                os.path.join(os.path.dirname(__file__), 'temp'))
            heat_source_temp = pd.read_pickle(filename)
    elif type_hp == 'brine':
        eta_g = kwargs.get('eta_g', 0.4)  # COP/COP_max
        heat_source_temp = hhp.calc_brine_supply_temp()
    else:
        eta_g = kwargs.get('eta_g', 0.4)  # COP/COP_max
        heat_source_temp = kwargs.get('heat_source_temp', None)
    # limit supply temperature to maximum supply temperature of the heat pump
    supply_temp[supply_temp > max_supply_temp_hp] = max_supply_temp_hp
    # calculate COP
    cop = eta_g * ((273.15 + supply_temp) / (supply_temp - heat_source_temp))
    cop_max = kwargs.get('cop_max', 7)
    cop[cop > cop_max] = cop_max
    return cop
    
    
def create_hp_entity(uid, cap, dh_bus_entity, el_bus_entity, region,
                     max_supply_temp_hp, type_hp, **kwargs):
    '''
    Creates a heat pump entity in a district heating system. The heat pump uses
    some kind of heat source to heat the return flow from the district
    heating system to max_supply_temp_hp.
    
    Parameters
    ----------
    uid : string
        unique id of heat pump entity
    cap : float
        capacity of heat pump
    dh_bus_entity : oemof.core bus object
        district heating bus entity
    el_bus_entity : oemof.core bus object
        electricity bus entity
    region : oemof.core Region object
        region entity
    max_supply_temp_hp : float
        maximum temperature the heat pump can deliver
    type_hp : string
        heat pump type (air or brine; in case of another heat source the
        heat_source_temp must be given)
    ------------------------------------
    kwargs : see definitions calc_supply_temp and calc_cop
    '''
    # district heating supply temperature
    supply_temp = calc_supply_temp(region.temp, **kwargs)
    # COP
    cop = calc_cop(supply_temp, max_supply_temp_hp, type_hp, **kwargs)
    # create entity
    transformer.Simple(
            uid=uid,
            inputs=[el_bus_entity],
            outputs=[dh_bus_entity],
            out_max=[cap],
            eta=[cop],
            regions=[region])
    return
    
 
def create_heat_storage_with_immersion_heater_entities(
    storage_uid, storage_cap, immersion_heater_uid, immersion_heater_cap,
    immersion_heater_eta, dh_bus_entity, el_bus_entity, region, **kwargs):
    '''
    Creates entities heat storage and immersion heater and connects them
    to a newly created storage bus that is connected to the district
    heating bus via two transport transformer. 
    
    Parameters
    ----------
    storage_uid : string
        unique id of heat storage entity
    storage_cap : float
        capacity of heat storage
    immersion_heater_uid : string
        unique id of immersion heater entity
    immersion_heater_cap : float
        capacity of immersion heater
    immersion_heater_eta : float
        efficiency of immersion heater
    dh_bus_entity : oemof.core bus object
        district heating bus entity
    el_bus_entity : oemof.core bus object
        electricity bus entity
    region : oemof.core Region object
        region entity
    ------------------------------------
    kwargs : see definitions calc_supply_temp and calc_cop
    '''
    # create storage bus
    storage_bus = Bus(uid=dh_bus_entity.uid+'_storage_bus',
        type='dh', regions=[region], excess=False)
    # create heat storage entity
    create_heat_storage_entity(storage_uid, storage_cap, storage_bus, region)
    # create immersion heater entity
    transformer.Simple(
        uid=immersion_heater_uid,
        inputs=[el_bus_entity], outputs=[storage_bus],
        out_max=[immersion_heater_cap], eta=[immersion_heater_eta])
    # create transports to and from dh bus to storage bus
    transformer.Simple(
        uid=dh_bus_entity.uid + '_transport_1',
        inputs=[el_bus_entity], outputs=[storage_bus],
        out_max=[max(immersion_heater_cap, storage_cap)], eta=[1])
    transformer.Simple(
        uid=dh_bus_entity.uid + '_transport_2',
        inputs=[storage_bus], outputs=[el_bus_entity],
        out_max=[max(immersion_heater_cap, storage_cap)], eta=[1])
    return
    
    
#def add_constraint_dh_heating_storage(om, temp, **kwargs):
#    '''
#    Make constraint for post heating of district heating thermal storage in 
#    case the supply temperatures is higher than the storage temperature.
#    '''
#    T_dh_storage = kwargs.get('T_dh_storage', 99)  # storage temperature
#    T_dh_return = kwargs.get('T_dh_return', 50)  # return temperature of dh
#    # get dh supply temperature
#    T_supply = calc_dh_supply_temp(temp)
#    # returns all district heating storages
#    storages = [obj for obj in om.energysystem.entities
#        if 'dh_thermal_storage' in obj.uid]
#    # write list to hand over to constraint
#    transports_ex = []
#    for export in exports:
#        transports_ex += [(export.uid, export.outputs[0].uid)]
#    # write list to hand over to constraint
#    transports_im = []
#    for imp in imports:
#        transports_im += [(imp.uid, imp.outputs[0].uid)]
#    # add new constraint
#    om.export_minimum_constraint = po.Constraint(expr=(
#        sum(om.w[i, o, t] for i, o in transports_ex for t in om.timesteps) -
#        sum(om.w[i, o, t] for i, o in transports_im for t in om.timesteps)
#        >= float(constraints.query('constr=="export_min"')['val'])))
#        
#    # iterate through thermal storages
#    for storage in storages:
#        for hour in list(range(len(T_supply.index))):
#            if T_supply[hour] > T_dh_storage:
#                prob += (lp_variables['DH Thermal Storage Boiler'][hour]
#                    * T_dh_storage - T_dh_return)
#                    - lp_variables['DH Storage Thermal Discharge'][hour]
#                    * (T_sup[hour] - T_dh_storage)
#                    == 0,
#                    'Provide supply temp ' + str(hour))
#
#            else:
#                prob += (lp_variables['DH Thermal Storage Boiler'][hour]
#                    == 0,
#                    'Provide supply temp ' + str(hour)
#    return
#    