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
from shapely.wkt import loads as wkt_loads


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


def create_powerplants(esystem, region, conn, scenario, schema, table, year):

    # create bhkws ###########################################
    pp = get_transformer_db(conn, schema, table, scenario, 'bhkw')
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
                    eta=[row['eta_el'], row['eta_th']],
                    opex_var=row['opex']/(row['eta_el']+row['eta_th']),                    
                    #input_costs=row['opex'],
                    regions=[region])

    # create electric transformer ##############################
    pp = get_transformer_db(conn, schema, table, scenario,
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
                             "('bus', '"+region.name+"', 'el')"][0]],
                    in_max=[None],
                    out_max=[float(row['power'])],
                    eta=[row['eta_el']],
                    opex_var=row['opex']/row['eta_el'],
                    regions=[region])

    # create heat transformer #################################
    pp = get_transformer_db(conn, schema, table, scenario,
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
                    eta=[row['eta_th']],
                    opex_var=row['opex']/row['eta_th'],
                    regions=[region])

    # create powertoheat transformer #################################
    pp = get_transformer_db(conn, schema, table, scenario,
                            'powertoheat')
    print('pp_pth')
    print(pp)
    if pp.empty:
        print('kein powertoheat')
    else:
        for index, row in pp.iterrows():
            transformer.Simple(
                    uid=('transformer', region.name, row['plant'], 't_pth'),
                    inputs=[obj for obj in esystem.entities if obj.uid ==
                            "('bus', '"+region.name+"', 'el')"],
                    outputs=[[obj for obj in region.entities if obj.uid ==
                             "('bus', '"+region.name+"', 'dh')"][0]],
                    in_max=[None],
                    out_max=[float(row['power'])],
                    eta=[row['eta_th']],
                    opex_var=row['opex']/row['eta_th'],
                    regions=[region])

    # create renewable power plants ##########################
    # renewable parameters
    site = hls.get_res_parameters()
    feedin_df, cap = feedin_nominal.Feedin().aggregate_cap_val(
             conn, region=region, year=year, bustype='elec', **site)
    print('wind:')
    print(feedin_df['wind_pwr'])

    pp = get_transformer_db(conn, schema, table, scenario, 'pv')
    if pp.empty:
        print('keine pv')
    else:
        for index, row in pp.iterrows():
            source.FixedSource(
                uid=('FixedSrc', region.name, row['plant'], 'pv'),
                outputs=[obj for obj in region.entities if obj.uid ==
                         "('bus', '"+region.name+"', 'elec')"],
                val=feedin_df['pv_pwr'],
                out_max=[float(row['power'])])

    pp = get_transformer_db(conn, schema, table, scenario, 'wind')
    if pp.empty:
        print('keine windkraft')
    else:
        for index, row in pp.iterrows():
            source.FixedSource(
                uid=('FixedSrc', region.name, row['plant'], 'wind'),
                outputs=[obj for obj in region.entities if obj.uid ==
                         "('bus', '"+region.name+"', 'elec')"],
                val=feedin_df['wind_pwr'],
                out_max=[float(row['power'])])

    