# -*- coding: utf-8 -*-
"""
@author: Elisa
"""
import logging
import pandas as pd
import numpy as np

from oemof.core.network.entities.components import transformers as transformer
from oemof.core.network.entities.components import sources as source
from oemof.demandlib import demand as dm


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
    co2_fix['oil_dec'] = 0.1
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

    eta_th = {}
    eta_th['lignite'] = 0.35
    eta_th['hard_coal'] = 0.39
    eta_th['natural_gas'] = 0.45
    eta_th['oil'] = 0.40
    eta_th['waste'] = 0.40
    eta_th['biomass'] = 0.40
    eta_th['pumped_storage'] = 0.40

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

    return(co2_emissions, co2_fix, eta_elec, eta_th, opex_var, capex, price)


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


def get_demand(conn):
    sql = """
        SELECT sector, region, demand
        FROM oemof.demand as pp
        """
    df = pd.DataFrame(
        conn.execute(sql).fetchall(), columns=['sector', 'fstate', 'demands'])
    return df


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


def get_small_runofriver_pps(conn):
    sql = """
        SELECT state_short, capacity, energy_average
        FROM oemof_test.runofriver_under10mw as ror
        """
    df = pd.DataFrame(
        conn.execute(sql).fetchall(), columns=[
        'state_short', 'capacity', 'energy'])
    return df


def get_pumped_storage_pps(conn):
    sql = """
        SELECT state_short, power_mw, capacity_mwh
        FROM oemof_test.pumped_storage_germany as pspp
        """
    df = pd.DataFrame(
        conn.execute(sql).fetchall(), columns=['state_short', 'power_mw',
            'capacity_mwh'])
    return df


def entity_exists(esystem, uid):
    return len([obj for obj in esystem.entities if obj.uid == uid]) > 0


def create_opsd_entity_objects(esystem, region, pp, bclass, **kwargs):
    'creates simple, CHP or storage transformer for pp from db'

    (co2_emissions, co2_fix, eta_elec, eta_th, opex_var, capex,
        price) = get_parameters()

    # weist existierendem Bus die location region zu
    if entity_exists(esystem, ('bus', region.name, pp[1].type)):
        logging.debug('Bus {0} exists. Nothing done.'.format(
            ('bus', region.name, pp[1].type)))
        location = region.name
    # weist existierendem Bus die location global zu
    elif entity_exists(esystem, ('bus', 'global', pp[1].type)):
        logging.debug('Bus {0} exists. Nothing done.'.format(
            ('bus', 'global', pp[1].type)))
        location = 'global'
    # erstellt Bus für Kraftwerkstyp und weist location global zu
    else:
        logging.debug('Creating Bus {0}.'.format(
            ('bus', region.name, pp[1].type)))
        bclass(uid=('bus', 'global', pp[1].type), type=pp[1].type,
            price=price[pp[1].type], regions=esystem.regions, excess=False)
        location = 'global'
        # erstellt source für Kraftwerkstyp
        source.Commodity(
            uid=pp[1].type,
            outputs=[obj for obj in esystem.entities if obj.uid == (
                'bus', location, pp[1].type)])
        print(('bus und source' + location + pp[1].type + 'erstellt'))

    # TODO: getBnEtzA ändern: ich brauche chp und wärmeleistung
    if pp[1].chp == 'yes':
        if pp[1].cap_th_uba is None:
            transformer.CHP(
                uid=('transformer', region.name, pp[1].type, 'chp'),
                # nimmt von ressourcenbus
                inputs=[obj for obj in esystem.entities if obj.uid == (
                    'bus', location, pp[1].type)],
                # speist auf strombus und fernwärmebus
                outputs=[[obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'elec')][0],
                    [obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'dh')][0]],
                in_max=[None],
                out_max=[[float(pp[1].cap_el)], [float(pp[1].cap_el) * 0.2]],
                eta=[eta_elec[pp[1].type], eta_th[pp[1].type]],
                opex_var=opex_var[pp[1].type],
                regions=[region])
        else:
            transformer.CHP(
                uid=('transformer', region.name, pp[1].type, 'chp'),
                # nimmt von ressourcenbus
                inputs=[obj for obj in esystem.entities if obj.uid == (
                    'bus', location, pp[1].type)],
                # speist auf strombus und fernwärmebus
                outputs=[[obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'elec')][0],
                    [obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'dh')][0]],
                in_max=[None],
                out_max=[[float(pp[1].cap_el_uba)], [float(pp[1].cap_th_uba)]],
                eta=[eta_elec[pp[1].type], eta_th[pp[1].type]],
                opex_var=opex_var[pp[1].type],
                regions=[region])

    # TODO: parameter überprüfen!!!
    elif pp[1].type == 'pumped_storage':
        transformer.storage(
            uid=('Storage', region.name, pp[1].type),
            # nimmt von strombus
            inputs=[obj for obj in esystem.entities if obj.uid == (
                'bus', location, 'elec')],
            # speist auf strombus ein
            outputs=[obj for obj in region.entities if obj.uid == (
            'bus', region.name, 'elec')],
            cap_max=[float(pp[1].cap_el)],
            out_max=[float(pp[1].cap_el)],  # inst. Leistung!
            eta_in=[eta_elec['pumped_storage_in']],  # TODO: anlegen!!
            eta_out=[eta_elec['pumped_storage_out']],  # TODO: anlegen!!!
            opex_var=opex_var[pp[1].type],
            regions=[region])

    # TODO: scale hydropower profile (csv) with capacity from db as fixed source
    elif pp[1].type == 'hydro_power':
        source.FixedSource(
            uid=('FixedSrc', region.name, 'hydro'),
            outputs=[obj for obj in region.entities if obj.uid == (
                'bus', region.name, 'elec')],
            val=scale_profile_to_capacity(path=kwargs.get('path_hydro'),
                filename=kwargs.get('filename_hydro'),
                capacity=pp[1].cap_el),
            out_max=[float(pp[1].cap_el)],
            regions=[region])
        print ('FixedSource')

    else:
        transformer.Simple(
            uid=('transformer', region.name, pp[1].type),
            # nimmt von ressourcenbus
            inputs=[obj for obj in esystem.entities if obj.uid == (
                'bus', location, pp[1].type)],
            # speist auf strombus ein
            outputs=[obj for obj in region.entities if obj.uid == (
                'bus', region.name, 'elec')],
            in_max=[None],
            out_max=[float(pp[1].cap_el)],  # inst. Leistung!
            eta=[eta_elec[pp[1].type]],
            opex_var=opex_var[pp[1].type],
            regions=[region])
    print ('hier')
    print (pp[1].cap_el)
    print (type(pp[1].cap_el))


def create_opsd_summed_objects(esystem, region, pp, bclass, chp_faktor,
    **kwargs):  # bclass = Bus

    'Creates entities for each type generation'

    (co2_emissions, co2_fix, eta_elec, eta_th, opex_var, capex,
        price) = get_parameters()

    typeofgen = kwargs.get('typeofgen')
    ror_cap = kwargs.get('ror_cap')
    pumped_storage = kwargs.get('pumped_storage')
    print('Anfang Funktion')

    # replace NaN with 0
    mask = pd.isnull(pp)
    pp = pp.where(~mask, other=0)

    capacity = {}
    capacity_chp_el = {}
    capacity_chp_th = {}
    efficiency = {}
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
        # th capacity for chp transformer (cap_th_uba + cap_el*Faktor
           # (where cap_th_uba=0 and chp=yes))
        capacity_chp_th[typ] = float(sum(pp[pp['type'].isin([typ])][pp[
            'status'].isin(['operating'])][pp['chp'].isin(['yes'])][
            'cap_th_uba'])) + float(sum(pp[pp['type'].isin([typ])][pp[
            'status'].isin(['operating'])][pp['chp'].isin(['yes'])][pp[
            'cap_th_uba'].isin([0])]['cap_el']) * chp_faktor)

        # efficiency only for simple transformer
        efficiency[typ] = np.mean(pp[pp['type'].isin([typ])][pp[
        'status'].isin(['operating'])][pp['chp'].isin(['no'])]['efficiency'])

        if capacity_chp_el[typ] > 0:
            transformer.CHP(
                uid=('transformer', region.name, typ, 'chp'),
                # nimmt von ressourcenbus
                inputs=[obj for obj in esystem.entities if obj.uid == (
                    'bus', 'global', typ)],
                # speist auf strombus und fernwärmebus ein
                outputs=[[obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'elec')][0],
                    [obj for obj in region.entities if obj.uid == (
                    'bus', region.name, 'dh')][0]],
                in_max=[None],
                out_max=[float(capacity_chp_el[typ]),
                    float(capacity_chp_th[typ])],
                eta=[eta_elec[typ], eta_th[typ]],
                opex_var=opex_var[typ],
                regions=[region])

        if capacity[typ] > 0:
            transformer.Simple(
                uid=('transformer', region.name, typ),
                # nimmt von ressourcenbus
                inputs=[obj for obj in esystem.entities if obj.uid == (
                    'bus', 'global', typ)],
                outputs=[[obj for obj in region.entities if obj.uid == (
                'bus', region.name, 'elec')][0]],
                in_max=[None],
                out_max=[float(capacity[typ])],
                eta=efficiency[typ],
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
            out_max=[power],
            in_max=[power],
            eta_in=eta_elec['pumped_storage_in'],  # TODO: anlegen!!
            eta_out=eta_elec['pumped_storage_out'],  # TODO: anlegen!!
            opex_var=opex_var[typ],
            regions=[region])

    # run of river
    typ = 'run_of_river'
    energy = sum(ror_cap[ror_cap['state_short'].isin([region.name])]['energy'])
    capacity[typ] = sum(pp[pp['type'].isin([typ])][
       pp['status'].isin(['operating'])]['cap_el']) + sum(ror_cap[ror_cap[
       'state_short'].isin([region.name])]['capacity'])
    if energy > 0:
        source.FixedSource(
            uid=('FixedSrc', region.name, 'hydro'),
            # speist auf strombus ein
            outputs=[obj for obj in region.entities if obj.uid == (
               'bus', region.name, 'elec')],
            val=scale_profile_to_sum_of_energy(
                filename=kwargs.get('filename_hydro'),
                energy=energy),
            out_max=[capacity[typ]],  # inst. Leistung!
            regions=[region])


def scale_profile_to_capacity(filename, capacity):
    profile = pd.read_csv(filename, sep=",")
    generation_profile = (profile.values / np.amax(profile.values) *
        float(capacity))
    return generation_profile


def scale_profile_to_sum_of_energy(filename, energy):
    profile = pd.read_csv(filename, sep=",")
    generation_profile = profile.values * float(energy) / sum(profile.values)
    return generation_profile