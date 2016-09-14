# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 08:19:23 2016

@author: Elisa.Gaudchau
"""

import logging
import pandas as pd
import numpy as np
from datetime import time as settime

from oemof.core.network.entities.components import transformers as transformer
from oemof.core.network.entities.components import sources as source
from oemof.demandlib import demand as dm
from oemof.demandlib import energy_buildings as eb
from oemof.demandlib import bdew_heatprofile as bdew_heat
from oemof.tools import helpers
from oemof import db
import helper_SmEnOs as hls

def get_parameters(conn_oedb):
    'returns emission and cost parameters'

    sql = """
        SELECT technology, co2_var, co2_fix, eta_elec,
             eta_th, eta_el_chp,
             eta_th_chp, eta_chp_flex_el, sigma_chp, beta_chp,
             opex_var, opex_fix, capex, c_rate_in, c_rate_out,
             eta_in, eta_out, cap_loss, lifetime, wacc
        FROM model_draft.abbb_simulation_parameter AS d
        """
    read_parameter = pd.DataFrame(
        conn_oedb.execute(sql).fetchall(),
        columns=['technology', 'co2_emissions', 'co2_fix', 'eta_elec',
                 'eta_th', 'eta_el_chp',
                 'eta_th_chp', 'eta_chp_flex_el', 'sigma_chp', 'beta_chp',
                 'opex_var', 'opex_fix', 'capex', 'c_rate_in', 'c_rate_out',
                 'eta_in', 'eta_out', 'cap_loss', 'lifetime', 'wacc'])

    parameters = {}
    for col in read_parameter.columns:
        parameters[col] = {}
        for row in read_parameter.index:
            key = read_parameter.technology[row]
            try:
                parameters[col][key] = float(read_parameter.loc[row][col])
            except:
                parameters[col][key] = read_parameter.loc[row][col]

    # emission factors [t/MWh]
    co2_emissions = parameters['co2_emissions']
    co2_fix = parameters['co2_fix']

    # efficiencies [-]
    eta_elec = parameters['eta_elec']
    eta_th = parameters['eta_th']
    eta_el_chp = parameters['eta_el_chp']
    eta_th_chp = parameters['eta_th_chp']
    eta_chp_flex_el = parameters['eta_chp_flex_el']
    sigma_chp = parameters['sigma_chp']
    beta_chp = parameters['beta_chp']

    opex_var = parameters['opex_var']
    opex_fix = parameters['opex_fix']
    capex = parameters['capex']
    lifetime = parameters['lifetime']
    wacc = parameters['wacc']

    c_rate_in = parameters['c_rate_in']
    c_rate_out = parameters['c_rate_out']
    eta_in = parameters['eta_in']
    eta_out = parameters['eta_out']
    cap_loss = parameters['cap_loss']

    return(co2_emissions, co2_fix, eta_elec, eta_th, eta_th_chp, eta_el_chp,
           eta_chp_flex_el, sigma_chp, beta_chp, opex_var, opex_fix, capex,
           c_rate_in, c_rate_out, eta_in, eta_out, cap_loss, lifetime, wacc)


def get_transmission(conn_oedb, scenario_name):
    'transmission capacities between BBB regions'

    sql = """
        SELECT from_region, to_region, capacity
        FROM model_draft.abbb_transmission_capacity AS d
        WHERE scenario = '""" + str(scenario_name) + """'"""
    read_parameter = pd.DataFrame(
        conn_oedb.execute(sql).fetchall(),
        columns=['from', 'to', 'cap'])

    transmission = get_dict_from_df(read_parameter)

    return(transmission)


def get_demand(conn_oedb, scenario_name):
    'heat and electrical demands in BBB regions'

    sql = """
        SELECT region, sector, type, demand
        FROM model_draft.abbb_demand AS d
        WHERE scenario = '""" + str(scenario_name) + """'"""
    read_parameter = pd.DataFrame(
        conn_oedb.execute(sql).fetchall(),
        columns=['region', 'sector', 'type', 'demand'])

    return(read_parameter)


def get_transformer(conn_oedb, scenario_name):
    'transformers in BBB regions'

    sql = """
        SELECT region, ressource, transformer, power
        FROM model_draft.abbb_transformer AS d
        WHERE scenario = '""" + str(scenario_name) + """'"""
    read_parameter = pd.DataFrame(
        conn_oedb.execute(sql).fetchall(),
        columns=['region', 'ressource', 'transformer', 'power'])

    return(read_parameter)


def get_dict_from_df(data_frame):
    '''returns "data". data is a dict which includes the columns of the table
    as dicts: columns of table: data[column]; cells of table: data[column][row]
    '''
    data = {}

    for col in data_frame.columns:
        data[col] = {}
        for row in data_frame.index:
            try:
                data[col][row] = float(data_frame.loc[row][col])
            except:
                data[col][row] = data_frame.loc[row][col]
    return data


def create_transformer(esystem, region, pp, conn, **kwargs):

    'Creates entities for each type of generation'

    typeofgen = kwargs.get('typeofgen')
    chp_faktor_flex = kwargs.get('chp_faktor_flex', 0.84)
    cap_initial = kwargs.get('cap_initial', 0)

    (co2_emissions, co2_fix, eta_elec, eta_th, eta_th_chp, eta_el_chp,
     eta_chp_flex_el, sigma_chp, beta_chp, opex_var, opex_fix, capex,
     c_rate_in, c_rate_out, eta_in, eta_out,
     cap_loss, lifetime, wacc) = get_parameters(conn)

    for typ in typeofgen:
        if typ == 'biomass':
            if region.name == 'BE':
                resourcebus = [obj for obj in esystem.entities if obj.uid ==
                    "('bus', 'BE', '"+typ+"')"]
            else:
                resourcebus = [obj for obj in esystem.entities if obj.uid ==
                    "('bus', 'BB', '"+typ+"')"]
        else:
            resourcebus = [obj for obj in esystem.entities if obj.uid ==
                "('bus', 'global', '"+typ+"')"]

        ########################## CHP #################################
        try:
            capacity = float(pp.query(
                'region==@region.name and ressource==@typ and transformer=="chp"')[
                'power'])
        except:
            capacity = 0
        print(capacity)
        if capacity > 0:
            transformer.CHP(
                uid=('transformer', region.name, typ, 'chp'),
                inputs=resourcebus,
                outputs=[[obj for obj in region.entities if obj.uid ==
                         "('bus', '"+region.name+"', 'elec')"][0],
                        [obj for obj in region.entities if obj.uid ==
                         "('bus', '"+region.name+"', 'dh')"][0]],
                in_max=[None],
                out_max=get_out_max_chp(
                        capacity, eta_th_chp[typ], eta_el_chp[typ]),
                eta=[eta_el_chp[typ], eta_th_chp[typ]],
                opex_var=opex_var[typ],
                regions=[region])

        ########################## SE_chp #################################
        try:
            capacity = float(pp.query(
                'region==@region.name and ressource==@typ and transformer=="SE_chp"')[
                'power'])
        except:
            capacity = 0
        if capacity > 0:
            transformer.SimpleExtractionCHP(
                uid=('transformer', region.name, typ, 'SEchp'),
                inputs=resourcebus,
                outputs=[[obj for obj in region.entities if obj.uid ==
                         "('bus', '"+region.name+"', 'elec')"][0],
                        [obj for obj in region.entities if obj.uid ==
                         "('bus', '"+region.name+"', 'dh')"][0]],
                in_max=[None],
                out_max=get_out_max_chp_flex(capacity, sigma_chp[typ]),
                out_min=[0.0, 0.0],
                eta_el_cond=eta_chp_flex_el[typ],
                sigma=sigma_chp[typ],	 # power to heat ratio in backpr. mode
                beta=beta_chp[typ],		# power loss index
                opex_var=opex_var[typ],
                regions=[region])

        ########################## T_el #################################
        try:
            capacity = float(pp.query(
                'region==@region.name and ressource==@typ and transformer=="T_el"')[
                'power'])
        except:
            capacity = 0
        if capacity > 0:
            transformer.Simple(
                uid=('transformer', region.name, typ),
                inputs=resourcebus,
                outputs=[[obj for obj in region.entities if obj.uid ==
                         "('bus', '"+region.name+"', 'elec')"][0]],
                in_max=[None],
                out_max=[capacity],
                eta=[eta_elec[typ]],
                opex_var=opex_var[typ],
                regions=[region])

        ########################## T_heat #################################
        try:
            capacity = float(pp.query(
                'region==@region.name and ressource==@typ and transformer=="T_heat"')[
                'power'])
        except:
            capacity = 0
        if capacity > 0:
            if typ == 'powertoheat':
                transformer.Simple(
                    uid=('transformer', region.name, typ),
                    inputs=[[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'elec')"][0]],
                    outputs=[[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'dh')"][0]],
                    in_max=[None],
                    out_max=[capacity],
                    eta=[eta_th['heat_rod']],
                    opex_var=0,
                    regions=[region])
            else:
                transformer.Simple(
                    uid=('heat_transformer', region.name, typ),
                    inputs=resourcebus,
                    outputs=[[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'dh')"][0]],
                    in_max=[None],
                    out_max=[capacity],
                    eta=[eta_th[typ]],
                    opex_var=opex_var[typ],
                    regions=[region])


def get_out_max_chp(capacity, eta_th_chp, eta_el_chp):
    out_max_th = capacity * eta_th_chp / eta_el_chp
    out = [capacity, out_max_th]
    return out


def get_out_max_chp_flex(capacity, sigma_chp):
    out_max_th = capacity / sigma_chp
    out = [capacity, out_max_th]
    return out

