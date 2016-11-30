# -*- coding: utf-8 -*-
"""
Created on Thu Nov 10 07:54:31 2016

@author: Elisa.Gaudchau
"""

import pandas as pd
from oemof.core.network.entities.components import transformers as transformer
from oemof.core.network.entities.components import sources as source
import feedin_nominal
import helper_SmEnOs as hls
import helper_dh as hldh
from shapely.wkt import loads as wkt_loads
import numpy as np


def get_polygon_gvm(conn):
    nuts = ['xx', 'yy']
    sql = '''
        SELECT st_astext(ST_Transform(st_union(geom), 4326))
        FROM deutschland.alle_gebiete
        WHERE gen = 'Grevesmühlen' and des = 'Stadt';
    '''.format(tuple(nuts))
    return wkt_loads(conn.execute(sql).fetchone()[0])


def get_transformer_db(conn, schema, table, scenario, typ):
    'transformers in BBB regions'

    sql = """
        SELECT plant, fuel, power, eta_el, eta_th, opex
        FROM """ + str(schema) + """.""" + str(table) + """ AS d
        WHERE scenario = '""" + str(scenario) + """' and
        typ = '""" + str(typ) + """'"""
    read_parameter = pd.DataFrame(
        conn.execute(sql).fetchall(),
        columns=['plant', 'fuel', 'power', 'eta_el', 'eta_th', 'opex'])

    return(read_parameter)


def create_powerplants(esystem, region, conn, scenario_base, scenario_heat, 
                       schema, table, year, peakshaving=False, ps=0.6):

    dh_bus = [obj for obj in region.entities if obj.uid ==
              "('bus', '"+region.name+"', 'dh')"][0]
    if peakshaving:
        el_bus = [obj for obj in region.entities if obj.uid ==
              "('bus', '"+region.name+"', 'el_excess')"][0]
    else:
        el_bus = [obj for obj in region.entities if obj.uid ==
              "('bus', '"+region.name+"', 'elec')"][0]
    # create bhkws ###########################################
    pp = get_transformer_db(conn, schema, table, scenario_base, 'bhkw')
    print('pp_bhkw')
    print(pp)
    if pp.empty:
        print('keine BHKW')
    else:
        for index, row in pp.iterrows():
            p_el_bhkw = float(row['power'])  # from kW to MW
            p_th_bhkw = p_el_bhkw * float(row['eta_th']) / float(row['eta_el'])
            transformer.CHP(
                    uid=('transformer', region.name, row['plant'], 'bhkw',
                         row['fuel']),
                    inputs=[obj for obj in esystem.entities if obj.uid ==
                            "('bus', 'global', '"+row['fuel']+"')"],
                    outputs=[[obj for obj in region.entities if obj.uid ==
                              "('bus', '"+region.name+"', 'elec')"][0],
                             [obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'dh')"][0]],
                    in_max=[None],
                    out_max=[p_el_bhkw, p_th_bhkw],
                    eta=[float(row['eta_el']), float(row['eta_th'])],
                    opex_var=float(row['opex']/(row['eta_el']+row['eta_th'])),                    
                    #input_costs=row['opex'],
                    regions=[region])

    # create electric transformer ##############################
    pp = get_transformer_db(conn, schema, table, scenario_base,
                                'transformer_el')
    print('pp_el')
    print(pp)
    if pp.empty:
        print('keine transformer_el')
    else:
        for index, row in pp.iterrows():
            transformer.Simple(
                    uid=('transformer', region.name, row['plant'], 't_el',
                         row['fuel']),
                    inputs=[obj for obj in esystem.entities if obj.uid ==
                            "('bus', 'global', '"+row['fuel']+"')"],
                    outputs=[[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'elec')"][0]],
                    in_max=[None],
                    out_max=[float(row['power'])],
                    eta=[float(row['eta_el'])],
                    opex_var=float(row['opex']/row['eta_el']),
                    regions=[region])

    # create heat transformer #################################
    pp = get_transformer_db(conn, schema, table, scenario_base,
                            'transformer_th')
    print('pp_th')
    print(pp)
    if pp.empty:
        print('keine transformer_th')
    else:
        for index, row in pp.iterrows():
            transformer.Simple(
                    uid=('transformer', region.name, row['plant'], 't_th',
                         row['fuel']),
                    inputs=[obj for obj in esystem.entities if obj.uid ==
                             "('bus', 'global', '"+row['fuel']+"')"],
                    outputs=[[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'dh')"][0]],
                    in_max=[None],
                    out_max=[float(row['power'])],
                    eta=[float(row['eta_th'])],
                    opex_var=float(row['opex']/row['eta_th']),
                    regions=[region])

    # create powertoheat transformer #################################
    pp = get_transformer_db(conn, schema, table, scenario_heat,
                            'powertoheat')
    print('pp_pth')
    print(pp)
    if pp.empty:
        print('kein powertoheat')
    else:
        for index, row in pp.iterrows():
            if row['power'] > 0:
                if peakshaving:
                    pthbus = [obj for obj in esystem.entities if obj.uid ==
                                "('bus', '"+region.name+"', 'el_excess')"]
                else:
                    pthbus = [obj for obj in esystem.entities if obj.uid ==
                                "('bus', '"+region.name+"', 'elec')"]
                transformer.Simple(
                        uid=('transformer', region.name, row['plant'], 't_pth'),
                        inputs=pthbus,
                        outputs=[[obj for obj in region.entities if obj.uid ==
                                 "('bus', '"+region.name+"', 'dh')"][0]],
                        in_max=[None],
                        out_max=[float(row['power'])],
                        eta=[float(row['eta_th'])],
                        opex_var=float(row['opex']/row['eta_th']),
                        regions=[region])

    # create dh heat pump
    pp = get_transformer_db(conn, schema, table, scenario_heat,
                            'heatpump')
    if pp.empty:
        print('keine heatpump')
    else:
        for index, row in pp.iterrows():
            if row['power'] > 0:
                cap = float(row['power'])  # sollte Grundlast decken können (outputleistung)
                max_supply_temp_hp = 70  # sollte so gewählt werden, dass Grundlast gedeckt wird, nicht mehr al 80
                type_hp = 'brine'  # muss noch angepasst werden
                T_supply_max = 95  #Wert laut Datenanfrage
                T_supply_min = 65  #Wert laut Datenanfrage
                T_amb_min = -12  #Wert laut Datenanfrage
                hldh.create_hp_entity(('transformer', region.name, 'dh', 'heat_pump'),
                              cap, dh_bus, el_bus, region,
                              max_supply_temp_hp, type_hp,
                              T_supply_max=T_supply_max,
                              T_supply_min=T_supply_min, T_amb_min=T_amb_min,
                              heat_source_temp=region.temp)


    #create dh heat storage
    pp = get_transformer_db(conn, schema, table, scenario_heat,
                            'heat_storage')
    if pp.empty:
        print('kein heat_storage')
    else:
        for index, row in pp.iterrows():
            if row['power'] > 0:
                cap = float(row['power'])
                out_max = cap / 4
                in_max = cap / 4
                eta_in = 0.95  # TODO!
                eta_out = 0.95
                cap_loss = 0.01  # Wert aus Wittenberg Simulation
                hldh.create_heat_storage_entity(
                    ('storage', region.name, 'dh'), cap, dh_bus, region,
                     out_max=out_max, in_max=in_max, eta_in=eta_in, eta_out=eta_out,
                     cap_loss=cap_loss)


    # create renewable power plants ##########################
    # renewable parameters
    site = hls.get_res_parameters()
    feedin_df, cap = feedin_nominal.Feedin().aggregate_cap_val(
             conn, region=region, year=year, bustype='elec', **site)

    feedin_df['wind>ps'] = (np.where(feedin_df['wind_pwr'] >= ps, 1, 0))*(feedin_df['wind_pwr']-ps)
    feedin_df['wind<ps'] = feedin_df['wind_pwr'] - feedin_df['wind>ps']
    feedin_df['pv>ps'] = (np.where(feedin_df['pv_pwr'] >= ps, 1, 0))*(feedin_df['pv_pwr']-ps)
    feedin_df['pv<ps'] = feedin_df['pv_pwr'] - feedin_df['pv>ps']
    # feedin_df.to_csv('feedin_gvm.csv')

    pp = get_transformer_db(conn, schema, table, scenario_base, 'pv')
    if pp.empty:
        print('keine pv')
    else:
        for index, row in pp.iterrows():
            if peakshaving:
                source.FixedSource(
                    uid=('FixedSrc', region.name, row['plant'], 'pv>05'),
                    outputs=[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'el_excess')"],
                    val=feedin_df['pv>ps'],
                    out_max=[float(row['power'])])
                source.FixedSource(
                    uid=('FixedSrc', region.name, row['plant'], 'pv<05'),
                    outputs=[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'elec')"],
                    val=feedin_df['pv<ps'],
                    out_max=[float(row['power'])])
            else:
                source.FixedSource(
                    uid=('FixedSrc', region.name, row['plant'], 'pv'),
                    outputs=[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'elec')"],
                    val=feedin_df['pv_pwr'],
                    out_max=[float(row['power'])])

    pp = get_transformer_db(conn, schema, table, scenario_base, 'wind')
    if pp.empty:
        print('keine windkraft')
    else:
        for index, row in pp.iterrows():
            if peakshaving:
                source.FixedSource(
                    uid=('FixedSrc', region.name, row['plant'], 'wind>05'),
                    outputs=[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'el_excess')"],
                    val=feedin_df['wind>ps'],
                    out_max=[float(row['power'])])
                source.FixedSource(
                    uid=('FixedSrc', region.name, row['plant'], 'wind<05'),
                    outputs=[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'elec')"],
                    val=feedin_df['wind<ps'],
                    out_max=[float(row['power'])])
            else:                
                source.FixedSource(
                    uid=('FixedSrc', region.name, row['plant'], 'wind'),
                    outputs=[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'elec')"],
                    val=feedin_df['wind_pwr'],
                    out_max=[float(row['power'])])

    