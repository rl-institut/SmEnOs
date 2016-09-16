# -*- coding: utf-8

import pandas as pd
import matplotlib.pyplot as plt
from oemof.core import energy_system as es
from oemof.solph.predefined_objectives import minimize_cost
from oemof.outputlib import to_pandas as tpd


def create_es(solver, timesteps, year):
    simulation = es.Simulation(solver=solver, timesteps=timesteps,
                               stream_solver_output=True,
                               debug=False, duals=True,
                               objective_options={"function": minimize_cost})

    # Adding a time index to the energy system
    time_index = pd.date_range('1/1/' + year,
                               periods=len(timesteps),
                               freq='H')
    energysystem = es.EnergySystem(time_idx=time_index,
                                   simulation=simulation)
    return energysystem


def color_dict(reg):
    cdict = {
             # renewables
             "('FixedSrc', '"+reg+"', 'wind_pwr')": 'lightblue',
             "('FixedSrc', '"+reg+"', 'pv_pwr')": 'yellow',

             "('transformer', '"+reg+"', 'oil')": 'black',
             "('transformer', '"+reg+"', 'oil', 'chp')": 'black',
             "('transformer', '"+reg+"', 'oil', 'SEchp')": 'black',

             "('transformer', '"+reg+"', 'natural_gas')": 'lightgrey',
             "('transformer', '"+reg+"', 'natural_gas', 'chp')": 'lightgrey',
             "('transformer', '"+reg+"', 'natural_gas', 'SEchp')": 'lightgrey',

             "('transformer', '"+reg+"', 'natural_gas_cc')": 'darkgrey',
             "('transformer', '"+reg+"', 'natural_gas_cc', 'chp')": 'darkgrey',
             "('transformer', '"+reg+"', 'natural_gas_cc', 'SEchp')": 'darkgrey',

             "('transformer', '"+reg+"', 'HH', 'bhkw_gas')": 'grey',
             "('transformer', '"+reg+"', 'GHD', 'bhkw_gas')": 'grey',

             "('transformer', '"+reg+"', 'biomass')": 'lightgreen',
             "('transformer', '"+reg+"', 'biomass', 'chp')": 'lightgreen',
             "('transformer', '"+reg+"', 'biomass', 'SEchp')": 'lightgreen',

             "('transformer', '"+reg+"', 'HH', 'bhkw_bio')": 'green',
             "('transformer', '"+reg+"', 'GHD', 'bhkw_bio')": 'green',

             "('transformer', '"+reg+"', 'lignite_jw', 'SEchp')": 'brown',
             "('transformer', '"+reg+"', 'lignite_sp', 'SEchp')": 'orange',
             "('demand', '"+reg+"', 'elec')": 'red',
             "('demand', '"+reg+"', 'elec', 'mob')": 'red',
             "('bus', '"+reg+"', 'elec')_excess": 'purple',
             "('transformer', '"+reg+"', 'hp', 'brine', 'ww')": 'blue',
             "('transformer', '"+reg+"', 'hp', 'brine', 'heating')": 'blue',
             "('transformer', '"+reg+"', 'hp', 'air', 'ww')": 'blue',
             "('transformer', '"+reg+"', 'hp', 'air', 'heating')": 'blue',
             "('transformer', '"+reg+"', 'hp', 'air', 'ww', 'rod')": 'blue',
             "('transformer', '"+reg+"', 'hp', 'air', 'heating', 'rod')": 'blue'}
    return cdict

def color_dict_dh(reg):
    cdict = {
             "('transformer', '"+reg+"', 'oil', 'chp')": 'black',
             "('transformer', '"+reg+"', 'oil', 'SEchp')": 'black',

             "('transformer', '"+reg+"', 'natural_gas', 'chp')": 'lightgrey',
             "('transformer', '"+reg+"', 'natural_gas', 'SEchp')": 'lightgrey',

             "('transformer', '"+reg+"', 'natural_gas_cc', 'chp')": 'darkgrey',
             "('transformer', '"+reg+"', 'natural_gas_cc', 'SEchp')": 'darkgrey',

             "('transformer', '"+reg+"', 'biomass', 'chp')": 'lightgreen',
             "('transformer', '"+reg+"', 'biomass', 'SEchp')": 'lightgreen',

             "('transformer', '"+reg+"', 'lignite_jw', 'SEchp')": 'brown',
             "('transformer', '"+reg+"', 'lignite_sp', 'SEchp')": 'orange',

             "('demand', '"+reg+"', 'dh')": 'red',

             "('bus', '"+reg+"', 'dh')_excess": 'purple',
             "('bus', '"+reg+"', 'dh')_shortage": 'blue'}
    return cdict

def stack_plot_gas(energysystem):
    # Plotting a combined stacked plot
    myplot = tpd.DataFramePlot(energy_system=energysystem)

    cdict = color_dict()

    ## Plotting the input flows of the electricity bus
    myplot.slice_unstacked(bus_uid="gas", type="input",
                           date_from="2050-01-01 00:00:00",
                           date_to="2050-01-14 00:00:00")
    myplot.color_from_dict(cdict)

    fig = plt.figure(figsize=(24, 14))
    plt.rc('legend', **{'fontsize': 19})
    plt.rcParams.update({'font.size': 19})
    plt.style.use('grayscale')

    handles, labels = myplot.io_plot(
        bus_uid="gas", cdict=cdict, line_kwa={'linewidth': 4},
        ax=fig.add_subplot(1, 1, 1),
        date_from="2050-01-01 00:00:00",
        date_to="2050-01-8 00:00:00",
        )
    myplot.ax.set_ylabel('Power in MW')
    myplot.ax.set_xlabel('Date')
    myplot.ax.set_title("Electricity bus")
    myplot.set_datetime_ticks(tick_distance=24, date_format='%d-%m-%Y')
    myplot.outside_legend(handles=handles, labels=labels)

    plt.show()
    return


def stack_plot(energysystem, reg, bus, date_from, date_to):
    # Plotting a combined stacked plot
    myplot = tpd.DataFramePlot(energy_system=energysystem)

    if bus == 'elec':
        cdict = color_dict(reg)
    elif bus == 'dh':
        cdict = color_dict_dh(reg)

    ## Plotting the input flows of the electricity bus
    myplot.slice_unstacked(bus_uid="('bus', '"+reg+"', '"+bus+"')", type="input",
                           date_from=date_from,
                           date_to=date_to)
    myplot.color_from_dict(cdict)

    fig = plt.figure(figsize=(24, 14))
    plt.rc('legend', **{'fontsize': 19})
    plt.rcParams.update({'font.size': 19})
    plt.style.use('grayscale')

    handles, labels = myplot.io_plot(
        bus_uid="('bus', '"+reg+"', '"+bus+"')", cdict=cdict, line_kwa={'linewidth': 4},
        ax=fig.add_subplot(1, 1, 1),
        date_from=date_from,
        date_to=date_to,
        )
    myplot.ax.set_ylabel('Power in MW')
    myplot.ax.set_xlabel('Date')
    myplot.ax.set_title(bus+" bus")
    myplot.set_datetime_ticks(tick_distance=24, date_format='%d-%m-%Y')
    myplot.outside_legend(handles=handles, labels=labels)

    plt.show()
    return


def sum_max_output_of_component(energysystem, from_uid, to_uid):
    results_bus = energysystem.results[[obj for obj in energysystem.entities
        if obj.uid == (from_uid)][0]]
    results_bus_component = results_bus[[obj for obj in energysystem.entities
        if obj.uid == (to_uid)][0]]
    return sum(results_bus_component), max(results_bus_component)

def timeseries_of_component(energysystem, from_uid, to_uid):
    results_bus = energysystem.results[[obj for obj in energysystem.entities
        if obj.uid == (from_uid)][0]]
    results_bus_component = results_bus[[obj for obj in energysystem.entities
        if obj.uid == (to_uid)][0]]
    return results_bus_component


def res_share(energysystem):
    # conventional
    from_uid = ['commodity_hard_coal', 'commodity_gas', 'commodity_waste',
        'commodity_uranium', 'commodity_lignite']
    to_uid = ['hard_coal', 'gas', 'waste', 'uranium', 'lignite']

    summe_conv = 0
    for i in range(len(from_uid)):
        summe_plant, maximum = sum_max_output_of_component(
            energysystem, from_uid[i], to_uid[i])
        summe_conv += summe_plant

    # renewables
    from_uid = ['biomass_st', 'wind_onshore', 'wind_offshore',
        'pv', 'geothermal', 'runofriver']
    summe_res = 0
    for i in range(len(from_uid)):
        summe_plant, maximum = sum_max_output_of_component(
            energysystem, from_uid[i], 'electricity')
        summe_res += summe_plant

    # shortage
    summe_shortage, maximum = sum_max_output_of_component(
            energysystem, 'electricity_shortage', 'electricity')

    return ((summe_res - summe_shortage) /
        (summe_conv + summe_res - summe_shortage))


def print_validation_outputs(energysystem, reg):

    # capacities of pp
    pp = [
        "('FixedSrc', '"+reg+"', 'wind_pwr')",
        "('FixedSrc', '"+reg+"', 'pv_pwr')",

        "('transformer', '"+reg+"', 'oil')",
        "('transformer', '"+reg+"', 'oil', 'chp')",
        "('transformer', '"+reg+"', 'oil', 'SEchp')",

        "('transformer', '"+reg+"', 'natural_gas')",
        "('transformer', '"+reg+"', 'natural_gas', 'chp')",
        "('transformer', '"+reg+"', 'natural_gas', 'SEchp')",

        "('transformer', '"+reg+"', 'natural_gas_cc')",
        "('transformer', '"+reg+"', 'natural_gas_cc', 'chp')",
        "('transformer', '"+reg+"', 'natural_gas_cc', 'SEchp')",

        "('transformer', '"+reg+"', 'HH', 'bhkw_gas')",
        "('transformer', '"+reg+"', 'GHD', 'bhkw_gas')",

        "('transformer', '"+reg+"', 'biomass')",
        "('transformer', '"+reg+"', 'biomass', 'chp')",
        "('transformer', '"+reg+"', 'biomass', 'SEchp')",

        "('transformer', '"+reg+"', 'HH', 'bhkw_bio')",
        "('transformer', '"+reg+"', 'GHD', 'bhkw_bio')",

        "('transformer', '"+reg+"', 'lignite_jw', 'SEchp')",
        "('transformer', '"+reg+"', 'lignite_sp', 'SEchp')"]

    ebus = "('bus', '"+reg+"', 'elec')"
    short = "('bus', '"+reg+"', 'elec')_shortage"
    excess = "('bus', '"+reg+"', 'elec')_excess"
    summe_plant_dict = {}
    for p in pp:
        print(p)
        try:
            summe_plant_dict[p], maximum = sum_max_output_of_component(
            energysystem, p, ebus)
            print(('sum:' + str(summe_plant_dict[p])))
            print(('max:' + str(maximum)))
        except:
            print('nicht vorhanden')
        try:
            print(('vls:' + str(summe_plant_dict[p] / maximum)))
        except:
            pass
        print('\n')

    # shortage
    summe_plant, maximum = sum_max_output_of_component(
        energysystem, short, ebus)
    print(('el_shortage_sum:' + str(summe_plant)))
    print(('el_shortage_max:' + str(maximum)))
    print('\n')

    # excess
    summe_plant, maximum = sum_max_output_of_component(
        energysystem, ebus, excess)
    print(('el_excess_sum:' + str(summe_plant)))
    print(('el_excess_max:' + str(maximum)))

    sum_fee = (summe_plant_dict["('FixedSrc', '"+reg+"', 'wind_pwr')"] +
               summe_plant_dict["('FixedSrc', '"+reg+"', 'pv_pwr')"])
    print(('share excess:' + str((summe_plant / sum_fee) * 100)))
    return


def print_exports(energysystem):

    export_from = ["('bus', 'UB', 'elec')",
                   "('bus', 'UB', 'elec')",
                   "('bus', 'PO', 'elec')",
                   "('bus', 'PO', 'elec')",
                   "('bus', 'HF', 'elec')",
                   "('bus', 'LS', 'elec')",
                   "('bus', 'HF', 'elec')",
                   "('bus', 'OS', 'elec')"]
    export_to = ["transport_('bus', 'UB', 'elec')('bus', 'KJ', 'elec')",
                 "transport_('bus', 'UB', 'elec')('bus', 'MV', 'elec')",
                 "transport_('bus', 'PO', 'elec')('bus', 'MV', 'elec')",
                 "transport_('bus', 'PO', 'elec')('bus', 'ST', 'elec')",
                 "transport_('bus', 'HF', 'elec')('bus', 'ST', 'elec')",
                 "transport_('bus', 'LS', 'elec')('bus', 'SN', 'elec')",
                 "transport_('bus', 'BE', 'elec')('bus', 'HF', 'elec')",
                 "transport_('bus', 'BE', 'elec')('bus', 'OS', 'elec')"]

    export_all = 0
    for i in range(len(export_from)):
        print(export_to[i])
#        time = timeseries_of_component(
#                energysystem, export_from[i], export_to[i])
#        print(time)
        summe_ex, maximum = sum_max_output_of_component(
            energysystem, export_from[i], export_to[i])
        export_all += summe_ex
        print('export:')
        print(summe_ex)
        print('max:')
        print(maximum)
    print('export_gesamt:')
    print(export_all)

# load dumped energy system
year = 2050
energysystem = create_es(
    'cbc', [t for t in range(8760)], str(year))
energysystem.restore()

reg = 'OS'
bus = 'elec'
date_from = "2010-01-01 00:00:00"
date_to = "2010-01-04 00:00:00"
#
stack_plot(energysystem, reg, bus, date_from, date_to)

## anteil ee
#print(res_share(energysystem))

## capacities
print_validation_outputs(energysystem, reg)
ebus = "('bus', '"+reg+"', 'elec')"
pv_time = timeseries_of_component(
            energysystem, "('FixedSrc', '"+reg+"', 'pv_pwr')", ebus)
wind_time = timeseries_of_component(
            energysystem, "('FixedSrc', '"+reg+"', 'wind_pwr')", ebus)
demand_time = timeseries_of_component(
            energysystem, ebus, "('demand', '"+reg+"', 'elec')")
            
res = pd.DataFrame(index=range(len(demand_time)), columns=['ee', 'pv', 'wind'])
for i in range(len(demand_time)):
    fee = demand_time[i] - pv_time[i] - wind_time[i]
    if fee < 0:
        res['ee'][i] = demand_time[i]
        res['pv'][i] = demand_time[i] * pv_time[i] / (
                        pv_time[i] + wind_time[i])
        res['wind'][i] = demand_time[i] * wind_time[i] / (
                        pv_time[i] + wind_time[i])
    else:
        res['ee'][i] = pv_time[i] + wind_time[i]
        res['pv'][i] = pv_time[i]
        res['wind'][i] = wind_time[i]

ee_share = sum(res['ee']) / sum(demand_time)
pv_share = sum(res['pv']) / sum(demand_time)
wind_share = sum(res['wind']) / sum(demand_time)
print('ee share:')
print(ee_share)
print('pv share:')
print(pv_share)
print('wind share:')
print(wind_share)    

print_exports(energysystem)

# stack h2
# stack gas
#stack_plot(energysystem)

## print all buses
#for entity in energysystem.entities:
    #try:
        #print(entity.uid)
        #print(entity.crf)
    #except:
        #pass