# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 14:35:28 2016

@author: uwe
"""
import pandas as pd
from oemof import db
from Open_eQuarterPy.stat_util import energy_demand as ed
from Open_eQuarterPy.stat_util import building_evaluation as be

conn = db.connection()



sql = '''
select
    st_area(st_transform(geom, 3068)),
    st_perimeter(st_transform(geom, 3068))
from berlin.hausumringe
where gid=360186;
'''
results = (conn.execute(sql))
columns = results.keys()
result1_df = pd.DataFrame(results.fetchall(), columns=columns)
print(result1_df)
print()

sql = '''
SELECT ag.* FROM berlin.alkis_gebaeude as ag, berlin.hausumringe as haus
WHERE ST_contains(ag.geom, st_centroid(haus.geom)) AND haus.gid=360186;
'''
results = (conn.execute(sql))
columns = results.keys()
result2_df = pd.DataFrame(results.fetchall(), columns=columns)
print("Anzahl der Obergeschosse:", result2_df.anzahldero[0])
print("Adresse:", result2_df.strassen_n[0], result2_df.hausnummer[0])
exit(0)
import pprint as pp
pp.pprint(ed.evaluate_building(
    population_density=52,
    area=166.7,
    floors=3.5,
    year_of_construction=2004,
    ))

pp.pprint(be.evaluate_building(
    population_density=52,
    area=166.7,
    perimeter=61.166,
    length=23.485,
    floors=3.5,
    year_of_construction=2004,
    ))

print(be.evaluate_building(15000, 10000, year_of_construction=1970))
