# -*- coding: utf-8 -*-
"""
@author: Elisa
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
from shapely.wkt import loads as wkt_loads


def get_parameters():
    'returns emission and cost parameters'

    # emission factors [t/MWh]
    co2_emissions = {}
    co2_emissions['lignite'] = 0.111 * 3.6
    co2_emissions['hard_coal'] = 0.0917 * 3.6
    co2_emissions['natural_gas'] = 0.0556 * 3.6
    co2_emissions['oil'] = 0.0750 * 3.6
    co2_emissions['waste'] = 0.1
    co2_emissions['biomass'] = 0.1
    co2_emissions['pumped_storage'] = 0.001

    # emission factors [t/MW]
    co2_fix = {}
    co2_fix['lignite'] = 0.1
    co2_fix['hard_coal'] = 0.1
    co2_fix['natural gas'] = 0.1
    co2_fix['oil'] = 0.1
    co2_fix['waste'] = 0.1
    co2_fix['pumped_storage'] = 0.1
        # decentralized pp
    co2_fix['lignite_dec'] = 0.1
    co2_fix['hard_coal_dec'] = 0.1
    co2_fix['biomass_dec'] = 0.1
    co2_fix['mineral_oil_dec'] = 0.1
    co2_fix['gas_dec'] = 0.1
    co2_fix['solar_heat_dec'] = 0.1
    co2_fix['heat_pump_dec'] = 0.1
    co2_fix['waste_dec'] = 0.1
    co2_fix['el_heat_dec'] = 0.1
        # renewables
    co2_fix['pv'] = 0.1
    co2_fix['wind'] = 0.1
    co2_fix['waste_dec'] = 0.1
    co2_fix['biomass'] = 0.1
    co2_fix['waste_dec'] = 0.1
    co2_fix['hydro'] = 0.1

    # efficiencies [-]
    eta_elec = {}
    eta_elec['lignite'] = 0.35
    eta_elec['hard_coal'] = 0.39
    eta_elec['natural_gas'] = 0.45
    eta_elec['oil'] = 0.40
    eta_elec['waste'] = 0.40
    eta_elec['biomass'] = 0.40
    eta_elec['pumped_storage'] = 0.40
    eta_elec['pumped_storage_in'] = 0.98
    eta_elec['pumped_storage_out'] = 0.98
    eta_elec['transmission'] = 0.997

    eta_th = {}
    eta_th['lignite'] = 0.35
    eta_th['hard_coal'] = 0.39
    eta_th['natural_gas'] = 0.45
    eta_th['oil'] = 0.40
    eta_th['waste'] = 0.40
    eta_th['biomass'] = 0.40
    eta_th['pumped_storage'] = 0.40
    
            # decentralized pp
    eta_th['dh'] = 0.7  #TODO In Abhängigkeit der Region?
    eta_th['lignite_dec'] = 0.8
    eta_th['hard_coal_dec'] = 0.8
    eta_th['biomass_dec'] = 0.8
    eta_th['mineral_oil_dec'] = 0.1
    eta_th['gas_dec'] = 0.1
    eta_th['solar_heat_dec'] = 0.5
    eta_th['heat_pump_dec'] = 1.5  # aus Energieb.-Glossar Verhältnis 1/3 zu 2/3
    eta_th['waste_dec'] = 0.7
    eta_th['el_heat_dec'] = 0.9

    eta_th_chp = {}
    eta_th_chp['lignite'] = 0.35
    eta_th_chp['hard_coal'] = 0.39
    eta_th_chp['natural_gas'] = 0.45
    eta_th_chp['oil'] = 0.40
    eta_th_chp['waste'] = 0.40
    eta_th_chp['biomass'] = 0.40
    eta_th_chp['bhkw'] = 0.40

    eta_el_chp = {}
    eta_el_chp['lignite'] = 0.35
    eta_el_chp['hard_coal'] = 0.39
    eta_el_chp['natural_gas'] = 0.45
    eta_el_chp['oil'] = 0.40
    eta_el_chp['waste'] = 0.40
    eta_el_chp['biomass'] = 0.40
    eta_el_chp['bhkw'] = 0.30

    eta_chp_flex_el = {} # eta el in condensing mode SimpleExtractionCHP
    eta_chp_flex_el['lignite'] = 0.3
    eta_chp_flex_el['hard_coal'] = 0.3
    eta_chp_flex_el['natural_gas'] = 0.4
    eta_chp_flex_el['oil'] = 0.3
    eta_chp_flex_el['waste'] = 0.3
    eta_chp_flex_el['biomass'] = 0.3
	
    sigma_chp = {}    # power to heat ratio for SimpleExtractionCHP
    sigma_chp['lignite'] = 1.2
    sigma_chp['hard_coal'] = 1.2
    sigma_chp['natural_gas'] = 1.2
    sigma_chp['oil'] = 1.2
    sigma_chp['waste'] = 1.2
    sigma_chp['biomass'] = 1.2
	
    beta_chp = {}    # power loss index for SimpleExtractionCHP
    beta_chp['lignite'] = 0.12
    beta_chp['hard_coal'] = 0.12
    beta_chp['natural_gas'] = 0.12
    beta_chp['oil'] = 0.12
    beta_chp['waste'] = 0.12
    beta_chp['biomass'] = 0.12

    # costs [??]
    opex_var = {}
    opex_var['lignite'] = 22
    opex_var['hard_coal'] = 25
    opex_var['natural_gas'] = 22
    opex_var['oil'] = 22
    opex_var['solar_power'] = 1
    opex_var['wind_power'] = 1
    opex_var['waste'] = 1
    opex_var['biomass'] = 1
    opex_var['pumped_storage'] = 1
    opex_var['run_of_river'] = 1
    
    opex_fix = {}
    opex_fix['lignite'] = 22
    opex_fix['hard_coal'] = 25
    opex_fix['natural_gas'] = 22
    opex_fix['oil'] = 22
    opex_fix['solar_power'] = 1
    opex_fix['wind_power'] = 1
    opex_fix['waste'] = 1
    opex_fix['biomass'] = 1
    opex_fix['pumped_storage'] = 1
    opex_fix['run_of_river'] = 1

    capex = {}
    capex['lignite'] = 22
    capex['hard_coal'] = 25
    capex['natural_gas'] = 22
    capex['oil'] = 22
    capex['solar_power'] = 1
    capex['wind_power'] = 1
    capex['waste'] = 1
    capex['biomass'] = 1
    capex['pumped_storage'] = 1
    capex['run_of_river'] = 1

    # price for ressource
    price = {}
    price['lignite'] = 60
    price['hard_coal'] = 60
    price['natural_gas'] = 60
    price['oil'] = 60
    price['waste'] = 60
    price['biomass'] = 60
    price['pumped_storage'] = 0
    price['hydro_power'] = 0

    # C-rate for storages
    c_rate = {}
    c_rate['pumped_storage_in'] = 1
    c_rate['pumped_storage_out'] = 1

    return(co2_emissions, co2_fix, eta_elec, eta_th, eta_th_chp, eta_el_chp, 
           eta_chp_flex_el, sigma_chp, beta_chp, opex_var, opex_fix, capex, 
           price, c_rate)


def get_res_parameters():
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
    return site

def get_offshore_parameters(conn):
    site = {'h_hub': 80,
        'd_rotor': 120,
        'wka_model': 'SIEMENS SWT 3.6 120',
        }

    return site

def get_bdew_heatprofile_parameters():
    #TODO Werte recherchieren
    bdew_heatprofile_parameters = pd.DataFrame(
        [{'share_EFH': 0.5, 'wind_class': 1, 'building_class': 1},
            {'share_EFH': 0.5, 'wind_class': 1, 'building_class': 1},
            {'share_EFH': 0.5, 'wind_class': 1, 'building_class': 1},
            {'share_EFH': 0.5, 'wind_class': 1, 'building_class': 1},
            {'share_EFH': 0.5, 'wind_class': 1, 'building_class': 1},
            {'share_EFH': 0.5, 'wind_class': 1, 'building_class': 1}],
        index=['BE', 'BB', 'MV', 'SN', 'ST', 'TH'])
    return bdew_heatprofile_parameters


def get_demand(conn, regions):
    sql = """
        SELECT nuts.nuts_id,
            ds.energy_sector AS energy,
            ds.consumption_sector AS sector,
            d.demand
        FROM oemof.demand AS d
        JOIN oemof.demand_sector AS ds ON d.sector=ds.id
        JOIN oemof.geo_nuts_rg_2013 AS nuts ON nuts.gid=d.region
        WHERE nuts.nuts_id IN
        """ + str(regions)
    df = pd.DataFrame(
        conn.execute(sql).fetchall(),
        columns=['nuts_id', 'energy', 'sector', 'demand'])
    return df

def get_biomass_between_5and10MW(conn, geometry):
    sql = """
    SELECT sum(p_nenn_kwp)
    FROM oemof_test.energy_map as ee 
    WHERE anlagentyp='Biomasse'
    AND p_nenn_kwp >= 5000
    AND p_nenn_kwp < 10000
    AND st_contains(ST_GeomFromText('{wkt}',4326), ee.geom)
        """.format(wkt=geometry.wkt)
    cap = pd.DataFrame(conn.execute(sql).fetchall(), columns=['capacity'])
    return cap

def get_biomass_under_5MW(conn, geometry):
    sql = """
    SELECT sum(p_nenn_kwp)
    FROM oemof_test.energy_map as ee 
    WHERE anlagentyp='Biomasse'
    AND p_nenn_kwp < 5000
    AND st_contains(ST_GeomFromText('{wkt}',4326), ee.geom)
        """.format(wkt=geometry.wkt)
    cap = pd.DataFrame(conn.execute(sql).fetchall(), columns=['capacity'])
    return cap

def get_opsd_pps(conn, geometry):
    de_en = {
        'Braunkohle': 'lignite',
        'lignite': 'lignite',
        'Steinkohle': 'hard_coal',
        'coal': 'hard_coal',
        'Erdgas': 'natural_gas',
        'Öl': 'oil',
        'oil': 'oil',
        'Solarstrom': 'solar_power',
        'Windkraft': 'wind_power',
        'Biomasse': 'biomass',
        'biomass': 'biomass',
        'Wasserkraft': 'hydro_power',
        'run_of_river': 'hydro_power',
        'Gas': 'methan',
        'Mineralölprodukte': 'mineral_oil',
        'Abfall': 'waste',
        'waste': 'waste',
        'Sonstige Energieträger\n(nicht erneuerbar) ': 'waste',
        'other_non_renewable': 'waste',
        'Pumpspeicher': 'pumped_storage',
        'pumped_storage': 'pumped_storage',
        'Erdwärme': 'geo_heat',
        'gas': 'natural_gas'}
    translator = lambda x: de_en[x]

    sql = """
        SELECT fuel, status, chp, capacity, capacity_uba, chp_capacity_uba,
        efficiency_estimate
        FROM oemof_test.kraftwerke_de_opsd as pp
        WHERE st_contains(
        ST_GeomFromText('{wkt}',4326), ST_Transform(pp.geom, 4326))
        """.format(wkt=geometry.wkt)
    df = pd.DataFrame(
        conn.execute(sql).fetchall(), columns=['type', 'status', 'chp',
            'cap_el', 'cap_el_uba', 'cap_th_uba', 'efficiency'])
    df['type'] = df['type'].apply(translator)
    return df


def get_hydro_energy(conn, regions):
    sql = """
        SELECT state_short, capacity_2013, energy_2013
        FROM oemof_test.hydro_energy_aee as ror
        WHERE state_short IN
        """ + str(regions)
    df = pd.DataFrame(
        conn.execute(sql).fetchall(), columns=[
        'state_short', 'capacity_mw', 'energy_mwh'])
    return df


def get_pumped_storage_pps(conn, regions):
    sql = """
        SELECT state_short, power_mw, capacity_mwh
        FROM oemof_test.pumped_storage_germany as pspp
        WHERE state_short IN
        """ + str(regions)
    df = pd.DataFrame(
        conn.execute(sql).fetchall(), columns=['state_short', 'power_mw',
            'capacity_mwh'])
    return df
    
    
def get_offshore_pps(conn, schema, table, start_year):
    sql = """
        SELECT farm_capacity, st_astext(geom)
        FROM {schema}.{table}
        WHERE start_year <= {start_year}
        """.format(**{'schema': schema, 'table': table, 
                      'start_year':start_year})
    pps = pd.DataFrame(conn.execute(sql).fetchall())
    return pps


def entity_exists(esystem, uid):
    return len([obj for obj in esystem.entities if obj.uid == uid]) > 0


def create_opsd_summed_objects(esystem, region, pp, **kwargs):

    'Creates entities for each type of generation'

    typeofgen = kwargs.get('typeofgen')
    ror_cap = kwargs.get('ror_cap')
    pumped_storage = kwargs.get('pumped_storage')
    chp_faktor_flex = kwargs.get('chp_faktor_flex', 0.84)
    cap_initial = kwargs.get('cap_initial', 0)

    (co2_emissions, co2_fix, eta_elec, eta_th, eta_th_chp, eta_el_chp, 
         eta_chp_flex_el, sigma_chp, beta_chp, opex_var, opex_fix, capex, 
         price, c_rate) = get_parameters()
        
    # replace NaN with 0
    mask = pd.isnull(pp)
    pp = pp.where(~mask, other=0)

    capacity = {}
    capacity_chp_el = {}
    
#_______________________________________get biomass under 10 MW from energymap
    conn = db.connection()
    p_bio = get_biomass_between_5and10MW(conn, region.geom)
    p_bio_bhkw = get_biomass_under_5MW(conn, region.geom)
    p_bio5to10 = float(p_bio['capacity']) / 1000 #from kW to MW
    p_el_bhkw = float(p_bio_bhkw['capacity']) / 1000 # from kW to MW
    p_th_bhkw = p_el_bhkw * eta_th_chp['bhkw'] / eta_el_chp['bhkw']
   
#___________________________________________________________

#    efficiency = {} 
    
    for typ in typeofgen:
        # capacity for simple transformer (chp = no)
        capacity[typ] = sum(pp[pp['type'].isin([typ])][pp['status'].isin([
            'operating'])][pp['chp'].isin(['no'])]['cap_el'])
        # el capacity for chp transformer (cap_el_uba +
           # cap_el(where cap_th_uba=0 and chp=yes))
        capacity_chp_el[typ] = sum(pp[pp['type'].isin([typ])][pp[
            'status'].isin(['operating'])][pp['chp'].isin(['yes'])][
            'cap_el_uba']) + sum(pp[pp['type'].isin([typ])][pp['status'].isin([
            'operating'])][pp['chp'].isin(['yes'])][pp['cap_th_uba'].isin(
            [0])]['cap_el'])

        # efficiency only for simple transformer from OPSD-List

#        efficiency[typ] = np.mean(pp[pp['type'].isin([typ])][pp[
#        'status'].isin(['operating'])][pp['chp'].isin(['no'])]['efficiency'])

        # choose the right bus for type of generation (biomass: regional bus)
        # and add biomass capacity from energymap between 5 and 10 MW
        if typ == 'biomass':
            resourcebus = [obj for obj in esystem.entities if obj.uid == (
                'bus', region.name, typ)]
            
            capacity_chp_el[typ] = capacity_chp_el[typ] + p_bio5to10
            
            #create biomass bhkw transformer (under 5 MW)           
            transformer.CHP(
                uid=('transformer', region.name, typ, 'bhkw'),
                # takes from resource bus
                inputs=resourcebus,
                # puts in electricity and heat bus
                outputs=[[obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'elec')][0],
                    [obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'dh')][0]],
#TODO: Wärme auf den richtigen bus legen!!!!
                in_max=[None],
                out_max=[p_el_bhkw,p_th_bhkw],
                eta=[eta_el_chp['bhkw'], eta_th_chp['bhkw']],
                opex_var=opex_var[typ],
                regions=[region])
        else: # not biomass
            resourcebus = [obj for obj in esystem.entities if obj.uid == (
                'bus', 'global', typ)]

        if capacity_chp_el[typ] > 0:
            
            transformer.CHP(
                uid=('transformer', region.name, typ, 'chp'),
                inputs=resourcebus,
                outputs=[[obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'elec')][0],
                    [obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'dh')][0]],
                in_max=[None],
                out_max=get_out_max_chp(capacity_chp_el[typ], chp_faktor_flex, 
                                        eta_th_chp[typ], eta_el_chp[typ]),
                eta=[eta_el_chp[typ], eta_th_chp[typ]],
                opex_var=opex_var[typ],
                regions=[region])
                

            transformer.SimpleExtractionCHP(
                uid=('transformer', region.name, typ, 'SEchp'),
                inputs=resourcebus,
                outputs=[[obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'elec')][0],
                    [obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'dh')][0]],
                in_max=[None],
                out_max=get_out_max_chp_flex(capacity_chp_el[typ], 
                                chp_faktor_flex, sigma_chp[typ]),
                out_min=[0.0, 0.0],
                eta_el_cond=eta_chp_flex_el[typ],
                sigma=sigma_chp[typ],	#power to heat ratio in backpressure mode
                beta=beta_chp[typ],		#power loss index
                opex_var=opex_var[typ],
                regions=[region])

        if capacity[typ] > 0:
            transformer.Simple(
                uid=('transformer', region.name, typ),
                inputs=resourcebus,
                outputs=[[obj for obj in region.entities if obj.uid == (
                'bus', region.name, 'elec')][0]],
                in_max=[None],
                out_max=[float(capacity[typ])],
                eta=[eta_elec[typ]],
                opex_var=opex_var[typ],
                regions=[region])

    # pumped storage
    typ = 'pumped_storage'
    power = sum(pumped_storage[pumped_storage[
        'state_short'].isin([region.name])]['power_mw'])
    if power > 0:
        transformer.Storage(
            uid=('Storage', region.name, typ),
            # nimmt von strombus
            inputs=[obj for obj in esystem.entities if obj.uid == (
               'bus', region.name, 'elec')],
            # speist auf strombus ein
            outputs=[obj for obj in region.entities if obj.uid == (
               'bus', region.name, 'elec')],
            cap_max=float(sum(pumped_storage[pumped_storage[
                'state_short'].isin([region.name])]['capacity_mwh'])),
            cap_min=0,
            in_max=[power],
            out_max=[power],
            eta_in=eta_elec[typ + '_in'],
            eta_out=eta_elec[typ + '_out'],
            c_rate_in=c_rate[typ + '_in'],
            c_rate_out=c_rate[typ + '_out'],
            opex_var=opex_var[typ],
            capex=capex[typ],
            cap_initial=cap_initial,
            regions=[region])

    # run of river
    typ = 'run_of_river'
    energy = sum(ror_cap[ror_cap['state_short'].isin(
        [region.name])]['energy_mwh'])
    capacity[typ] = sum(ror_cap[ror_cap[
       'state_short'].isin([region.name])]['capacity_mw'])
    if energy > 0:
        source.FixedSource(
            uid=('FixedSrc', region.name, 'hydro'),
            # speist auf strombus ein
            outputs=[obj for obj in region.entities if obj.uid == (
               'bus', region.name, 'elec')],
            val=scale_profile_to_sum_of_energy(
                filename=kwargs.get('filename_hydro'),
                energy=energy,
                capacity = capacity[typ]),
            out_max=[capacity[typ]],  # inst. Leistung!
            regions=[region])
            
def get_out_max_chp(capacity_chp_el, chp_faktor_flex, 
                    eta_th_chp, eta_el_chp):
    out_max_el = float(capacity_chp_el) * (1-chp_faktor_flex)
    out_max_th = out_max_el * eta_th_chp / eta_el_chp
    out = [out_max_el, out_max_th]
    return out
    
def get_out_max_chp_flex(capacity_chp_el, chp_faktor_flex, sigma_chp):
    out_max_el = float(capacity_chp_el) * (chp_faktor_flex)
    out_max_th = out_max_el / sigma_chp
    out = [out_max_el, out_max_th]
    return out


def scale_profile_to_capacity(filename, capacity):
    profile = pd.read_csv(filename, sep=",")
    generation_profile = (profile.values / np.amax(profile.values) *
        float(capacity))
    return generation_profile


def scale_profile_to_sum_of_energy(filename, energy, capacity):
    profile = pd.read_csv(filename, sep=",")
    generation_profile = profile.values * float(energy) / (
                        sum(profile.values) * float(capacity))
    return generation_profile


def call_el_demandlib(demand, method, year, **kwargs):
    '''
    Calls the demandlib and creates an object which includes the demand
    timeseries.

    Required Parameters
    -------------------
    demand :
    method : Method which is to be applied for the demand calculation
    '''
    demand.val = dm.electrical_demand(method,
                         year=year,
                         annual_elec_demand=kwargs.get(
                         'annual_elec_demand'),
                         ann_el_demand_per_sector=kwargs.get(
                         'ann_el_demand_per_sector'),
                         path=kwargs.get('path'),
                         filename=kwargs.get('filename'),
                         ann_el_demand_per_person=kwargs.get(
                         'ann_el_demand_per_person'),
                         household_structure=kwargs.get(
                         'household_structure'),
                         household_members_all=kwargs.get(
                         'household_members_all'),
                         population=kwargs.get(
                         'population'),
                         comm_ann_el_demand_state=kwargs.get(
                         'comm_ann_el_demand_state'),
                         comm_number_of_employees_state=kwargs.get(
                         'comm_number_of_employees_state'),
                         comm_number_of_employees_region=kwargs.get(
                         'comm_number_of_employees_region')).elec_demand
    return demand


def call_heat_demandlib(region, year, **kwargs):
    '''
    Calls the demandlib and creates an object which includes the demand
    timeseries.

    Required Parameters
    -------------------
    demand : Sink object
    region : Region object
    '''
    holidays = helpers.get_german_holidays(year, ['Germany', region.name])
    load_profile = eb.Building().hourly_heat_demand(
                        fun=bdew_heat.create_bdew_profile,
                        datapath="../../oemof/oemof/demandlib/bdew_data",
                        year=year, holidays=holidays,
                        temperature=region.temp,
                        shlp_type=kwargs.get('shlp_type', None),
                        building_class=(region.building_class
                            if region.building_class is not None else 0),
                        wind_class=region.wind_class,
                        ww_incl=kwargs.get('ww_incl', True),
                        annual_heat_demand=kwargs.get(
                            'annual_heat_demand', None))
    return load_profile


def ind_profile_parameters():
    am = settime(7, 0, 0)
    pm = settime(20, 00, 0)
    profile_factors = {'week': {'day': 0.8, 'night': 0.6},
                       'weekend': {'day': 0.9, 'night': 0.7}}
    return am, pm, profile_factors


def call_ind_profile(year, annual_demand, **kwargs):
    '''
    Creates an industrial load profile as step load profile.
    '''
    ilp = eb.IndustrialLoadProfile(
        method='simple_industrial_profile', year=year,
        annual_demand=annual_demand,
        am=kwargs.get('am', None), pm=kwargs.get('pm', None),
        profile_factors=kwargs.get('profile_factors', None))
    return ilp.slp
