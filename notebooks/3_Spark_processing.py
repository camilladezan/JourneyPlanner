# ## Prepeare the Spark env 

# +
# In this step we are loading the libriaries we will use for this part of the project
import os
import re
import pandas as pd
from IPython import get_ipython
import matplotlib.pyplot as plt
import warnings
import numpy as np
import getpass
import pyspark

pd.set_option("display.max_columns", 50)
warnings.simplefilter(action='ignore', category=UserWarning)
# %matplotlib inline
# -

# %load_ext sparkmagic.magics

# +
username = os.environ['RENKU_USERNAME']
server = "http://iccluster044.iccluster.epfl.ch:8998"

# set the application name as "<your_gaspar_id>-project"
get_ipython().run_cell_magic(
    'spark',
    line='config', 
    cell="""{{ "name": "{0}-project", "executorMemory": "4G", "executorCores": 4, "numExecutors": 10, "driverMemory": "4G" }}""".format("rg4")
    #cell="""{{ "name": "{0}-project", "executorMemory": "4G", "executorCores": 4, "numExecutors": 10, "driverMemory": "4G" }}""".format(username)
)
# -

conf = pyspark.conf.SparkConf()
conf.setMaster('local[2]')
#conf.setAppName('spark-project-{0}'.format(getpass.getuser()))
conf.setAppName('spark-project-{0}'.format("rg4"))
sc = pyspark.SparkContext.getOrCreate(conf)
sc.setLogLevel("WARN")
conf = sc.getConf()
sc

get_ipython().run_line_magic(
    "spark", f"""add -s {username}-project -l python -u {server} -k"""
)

# + language="spark"
# print('We are using Spark %s' % spark.version)
# -

#  # Load data from SBB

# [('sbb_istdaten_zurich',),
#  ('sbb_orc',),
#  ('sbb_orc_calendar',),
#  ('sbb_orc_routes',),
#  ('sbb_orc_stop_times',),
#  ('sbb_orc_stops',),
#  ('sbb_orc_transfers',),
#  ('sbb_orc_trips',),
#  ('sbb_stations_zurich',),
#  ('sbb_zurich_parsed',)]

# + language="spark"
# # This cell gets the username to query the tables. 
# # username = sc.appName.split('-')[0]
# # print(f'{username}')
# username = "charroin"

# + language="spark"
# sbb_zurich_parsed = spark.read.orc(f"/user/{username}/hive/sbb_zurich_parsed")
# sbb_zurich = spark.read.orc(f"/user/{username}/hive/sbb_stations_zurich")
# allstops = spark.read.orc('/data/sbb/orc/allstops')
# sbb_zurich.printSchema()

# + language="spark"
#
# sbb_zurich_parsed.show(n=5, truncate=False, vertical=False)
# -

# # Prepare data for CSA Algorithm
# We will use the CSA (Connection Scan Algorithm) for our route planner. The details of the algorithm can be found here : https://arxiv.org/abs/1703.05997 and another explanation found in this lecture (starting from 28:24) : https://www.youtube.com/watch?v=AdArDN4E6Hg.

# ## Footpaths table
# We start by computing all footpathes for our model. A footpath is a triple (start_station, end_station, walk_time).

# + language="spark"
#
# from math import sin, cos, sqrt, atan2, radians
# import pyspark.sql.functions as F
#
# @F.udf
# def distance(lat1, lon1, lat2, lon2): 
#     # approximate radius of earth in km
#     R = 6373.0
#
#     lat1 = radians(lat1)
#     lon1 = radians(lon1)
#     lat2 = radians(lat2)
#     lon2 = radians(lon2)
#
#     dlon = lon2 - lon1
#     dlat = lat2 - lat1
#
#     a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
#     c = 2 * atan2(sqrt(a), sqrt(1 - a))
#
#     return R * c
# -

# Hector version (footpaths for station_names)

# + language="spark"
#
# # First we query the data from the stations in Zürich that we created with hive. 
# stops_relevant_info = sbb_zurich.select('stop_name', 'stop_lat', 'stop_lon')
#
# # Get one table with the stations as "depatrure"
# walking_departure_stops = stops_relevant_info.alias('departure')\
#                                              .withColumnRenamed('stop_name', 'dep_stop_name')
#
# # Get annother as "arrival" just to compute the distance between stations. 
# walking_arrival_stops = stops_relevant_info.alias('arrival')\
#                                             .withColumnRenamed('stop_name', 'arr_stop_name')
#
#
# station_distances = walking_departure_stops.crossJoin(walking_arrival_stops)\
#                                     .withColumn('dep_arr', F.concat('dep_stop_name','arr_stop_name'))\
#                                     .dropDuplicates(['dep_arr'])\
#                                     .drop('dep_arr')\
#                                     .filter(F.col('dep_stop_name') != F.col('arr_stop_name'))
# station_distances.show(n=5, truncate=False, vertical=False)
#
# -
# Anh Duong's version (footpath for each stop_id)

# +
# # %%spark
# # First we query the data from the stations in Zürich that we created with hive. 
# stops_relevant_info = sbb_zurich.select('stop_id', 'stop_lat', 'stop_lon')

# # Get one table with the stations as "depatrure"
# walking_departure_stops = stops_relevant_info.alias('departure')\
#                                              .withColumnRenamed('stop_id', 'dep_stop_id')

# # Get annother as "arrival" just to compute the distance between stations. 
# walking_arrival_stops = stops_relevant_info.alias('arrival')\
#                                             .withColumnRenamed('stop_id', 'arr_stop_id')


# station_distances = walking_departure_stops.crossJoin(walking_arrival_stops)\
#                                     .withColumn('dep_arr', F.concat('dep_stop_id','arr_stop_id'))\
#                                     .dropDuplicates(['dep_arr'])\
#                                     .drop('dep_arr')\
#                                     .filter(F.col('dep_stop_id') != F.col('arr_stop_id'))
# station_distances.show(n=5, truncate=False, vertical=False)

# + language="spark"
#
# short_walks = station_distances.withColumn('distance', distance('departure.stop_lat', 'departure.stop_lon', 'arrival.stop_lat',\
#                                                                 'arrival.stop_lon'))\
#                                 .filter('distance < 0.5')\
#                                 .withColumn('duration', F.ceil(F.col('distance') / 0.05))\
#                                 .select('dep_stop_name', 'arr_stop_name', 'duration')\
#                                 .withColumnRenamed('dep_stop_name', 'departure')\
#                                 .withColumnRenamed('arr_stop_name', 'arrival')
# short_walks.show(5, truncate=False)


# + language="spark"
# short_walks.write.mode('overwrite').option("header","true").format("orc").save(f"/user/{username}/hive/sbb_zurich_short_walks")

# + language="spark"
# short_walks.repartition(1).write.mode('overwrite').option("header","true").format("csv").save(f"/user/{username}/hive/sbb_zurich_short_walks")
# -

#  # Delays table

# + language="spark"
# sbb_zurich_parsed.show(n=5)

# + language="spark"
# filtered_sbb = sbb_zurich_parsed.filter(sbb_zurich_parsed.real_arrival != "null").filter(sbb_zurich_parsed.arrival != "null").filter(sbb_zurich_parsed._c1=='REAL')
# filtered_sbb.show(n=5)

# + language="spark"
# from datetime import datetime
# import pyspark.sql.functions as F
#
# @F.udf
# def compute_delays(real_arrival, arrival):
#     """Gets delay for a stop."""
#     parsed_real_arrival = datetime.strptime(real_arrival, '%H:%M:%S')
#     parsed_arrival = datetime.strptime(arrival, '%H:%M:%S')
#     delay = parsed_real_arrival - parsed_arrival
#     return delay.total_seconds()
# + language="spark"
#
# from pyspark.sql.functions import col
# from pyspark.sql.functions import date_format
#
# # change dataframe structure to be able to filter the delays
# filtered_sbb = filtered_sbb.withColumn('operating_date', col('operating_date').cast('date'))
# filtered_sbb = filtered_sbb.withColumn("arrival", date_format("arrival", "HH:mm:ss"))
# filtered_sbb = filtered_sbb.withColumn("real_arrival", date_format("real_arrival", "HH:mm:ss"))
# filtered_sbb = filtered_sbb.withColumn("departure", date_format("departure", "HH:mm:ss"))
# filtered_sbb = filtered_sbb.withColumn("real_departure", date_format("real_departure", "HH:mm:ss"))
#
# # filter the delay dataframe only for the week of interest and the working hours since it is when the data are more reliable
# filtered_delays = filtered_sbb.filter((filtered_sbb.operating_date >= '2020-01-29')&(filtered_sbb.operating_date <= '2023-04-05')).filter((filtered_sbb.departure > '07:00:00')&(filtered_sbb.departure < '19:00:00')\
#                                 &(filtered_sbb.arrival > '07:00:00')&(filtered_sbb.arrival < '19:00:00'))
# filtered_delays.show(n=5)

# + language="spark"
# delays_df = filtered_delays.withColumn("delay", compute_delays(filtered_delays.real_arrival, filtered_delays.arrival)).distinct()
# delays_df.show(n=10, truncate=False, vertical=False)
# -

# From the delay column we can see that the delay can have both a positive and a negative value. When the delay is negative it means that the mean of transport arrived earlier than expected. To have reliable information we need to quantify this possible advance to be sure that it is not due to a change in schedule or to other types of anomomalies.

# + language="spark"
# from pyspark.sql.functions import count, when
#
# delays_df.select(count(when(col("delay") < 0, True))).collect()[0][0]

# + language="spark"
# delays_df.repartition(1).write.mode('overwrite').option("header","true").format("csv").save(f"/user/{username}/hive/sbb_delays_df")

# + language="spark"
# # compute max delay
# from pyspark.sql.functions import max, min
#
# max_value = delays_df.select(max("delay")).collect()[0][0]
# min_value = delays_df.select(min("delay")).first()[0]
#
# print("Minimum delay: {}".format(min_value))
# print("Maximum delay: {}".format(max_value))
# -

# Since there are no significant early arrival in the considered week, there is no need to remove outliers. A filtering operation would be required if the advance was bigger (e.g. half an hour early) which could correspond to a change of schedule or to other external factors. Moreover, the largest delay is of about 16 minutes, which is reasonable. Therefore, for the considered time interval there is no need to remove outliers.
#
# NB: If the week of interest is different then this analysis should be carried out again and in case of significant variations (like more than 30 minutes early or more than 5 hours late) it might be necessary to remove this singular values.

# + language="spark"
# # means of transport with the most delays
# transp_delays = delays_df.groupBy('produkt_id').agg(F.mean('delay'))
# transp_delays.show()
# -

# As one could expect the largest delays are due to buses, which indeed are subject to more factors that could cause delays.
#
# In the case of trains we can further analyse the delays considering the different train numbers to identify if there is a route which is less reliable than the others.

# + language="spark"
# # delay per train number
# group_trains = delays_df.where(F.col('produkt_id')=='zug').groupBy('train_number').agg(F.mean('delay'))
# group_trains.show()

# + language="spark"
# # average delay per station
# station_delays = delays_df.groupBy('haltestellen_name').agg(F.mean('delay'))
# station_delays.show()
# -

#  ## Delays probability

# + language="spark"
# from datetime import datetime
# import pyspark.sql.functions as F
#
# def compute_delay_probability(stop_name, day_trip, train_num, arrival_time, transport, delay_tol):
#     # compute the probability of having a delay for a certain stop in a certain day given a tolerance (expressed in seconds)
#     filtered = delays_df.filter((delays_df.haltestellen_name == stop_name)&
#                                 (delays_df.operating_date.cast('string') == day_trip)&
#                                 (delays_df.train_number == train_num)&
#                                 (delays_df.arrival == arrival_time) &
#                                 (delays_df.delay.isNotNull())&
#                                 (delays_df.produkt_id == transport))
#     stop_tot = filtered.count()
#     
#     # If there aren't enough information on the station set the probability of having a delay to the maximum 
#     if stop_tot <= 10:
#         return 1.0
#     
#     stop_delay_tol_df = delays_df.filter((delays_df.haltestellen_name == stop_name)&
#                                       (delays_df.operating_date.cast('string') == day_trip)&
#                                       (delays_df.train_number == train_num)&
#                                       (delays_df.arrival == arrival_time)&
#                                       (delays_df.delay.isNotNull())&
#                                       (delays_df.delay.cast('int') <= delay_tol)&
#                                       (delays_df.produkt_id == transport))
#     stop_delay_tol = stop_delay_tol_df.count()
#
#     probability = float(stop_tot) / float(stop_delay_tol)
#     return probability
#
#
# # testing the function
# # sanity check to remove null values
# delays_df = delays_df.na.drop(subset=['delay'])
# tol = 120
# delay_prob = compute_delay_probability('Zürich, Micafil', '2023-03-31', '85:849:031', 
#                                        '11:22:00', 'bus', tol)
# print("Test probability for the selected case: {}".format(delay_prob))
# -
# ## Connections table
# According to the paper, we also need an array of "connections" : formally, quintuples (departure_station, departure_time, arrival_station, arrival_time, trip_id), and an array of footpaths. \
# Data for connections, for us, could be found in the stop_times table, with some manipulations.

# The data of interest, stop_times, is very messy. It is partitioned by weeks, but starting at Wednesday.
# - The latest available data is 7 days up to May 3rd. However, since May 1st is a bank holiday in ZH, we can't choose this timetable.
# - Data 7 days up to April 26th could be affected by the extended school holidays. This is the same for data up to April 19th and April 12th.
# Therefore, we will choose the timetable data for the 7 days from March 29th to April 5th, only considering work days and from 07:00 to 19:00.
#

# + language="spark"
# from datetime import datetime
# import pyspark.sql.functions as F
# from pyspark.sql import Window

# + language="spark"
# #import stop_time data
# stop_times = spark.read.orc('/data/sbb/part_orc/timetables/stop_times/year=2023/month=4/day=5')
# stop_times.show(5)
#
# #import trips data
# trips = spark.read.orc('/data/sbb/part_orc/timetables/trips/year=2023/month=4/day=5').select("route_id","service_id","trip_id")
# trips.show(5)
# -

# We now need to fetch calendar data and transport method (i.e. vehicle) data. However, this data is not available in stop_times. Therefore, we need to :
# * Join with trips.txt to get service_id (PK of calendar.txt) and route_id (PK of routes.txt)
# * calendar.txt then gives us the day of the trip
# * routes.txt then gives us the route_desc, which contains the means of transportation.

# + language="spark"
#
# ## Gets all the id's from zürich and stores it in a list 
# zurich_ids = sbb_zurich.select("stop_id").collect()
# zurich_ids = [row.stop_id for row in zurich_ids]
#
# # Queries only the stops that are in zurich, in a April and May, in working hours 
# stop_times_zurich = stop_times.filter(stop_times.stop_id.isin(zurich_ids))\
#                               .drop("pickup_type","drop_off_type","year","month","day")\
#                               .filter((stop_times.departure_time > '07:00:00')&(stop_times.departure_time < '19:00:00')\
#                                &(stop_times.arrival_time > '07:00:00')&(stop_times.arrival_time < '19:00:00'))

# + language="spark"
# stop_times_zurich = stop_times_zurich.join(sbb_zurich.select('stop_id','stop_name'), 'stop_id').drop('stop_id')\
# .filter((stop_times.departure_time > '07:00:00')&(stop_times.departure_time < '19:00:00')\
#                                &(stop_times.arrival_time > '07:00:00')&(stop_times.arrival_time < '19:00:00'))
# -

# We can then create the connections array with the quintuples described in the CSA. For that, we will apply a lag function on the arrival time and the arrival stops

# + language="spark"
#
# ## 
# connection_window=Window.partitionBy("trip_id").orderBy(F.col("arrival_time").asc())
#
# arr_next_station=F.lead("arrival_time").over(connection_window).alias("arr_time")
#
# next_station_id=F.lead("stop_name").over(connection_window).alias("arrival_station")
#
# zurich_connections=stop_times_zurich.select("trip_id","departure_time","stop_name",arr_next_station,next_station_id)\
#                             .na.drop("any")\
#                             .withColumnRenamed("stop_name","departure_station")\
#                             .withColumnRenamed("arr_time","arrival_time")
# zurich_connections.show(10, False)

# + language="spark"
# zurich_connections.count()

# + language="spark"
# zurich_stop_times_ext = zurich_connections.join(trips, ['trip_id'])
# zurich_stop_times_ext.count()

# + language="spark"
# zurich_stop_times_ext.printSchema()

# + language="spark"
#
# #import calendar data
# calendar = spark.read.orc('/data/sbb/part_orc/timetables/calendar/year=2023/month=4/day=5').drop("start_date","end_date")
# calendar.show(5)

# + language="spark"
#
# zurich_stop_times_dated = zurich_stop_times_ext.join(calendar, 'service_id')
# zurich_stop_times_dated.show(5)

# + language="spark"
# zurich_stop_times_dated.count()

# + language="spark"
# # import routes data
# routes = spark.read.orc('/data/sbb/part_orc/timetables/routes/year=2023/month=4/day=5').select("route_id","route_desc", "route_short_name")
# routes.show(5)
# -

# On the timetable cookbook (https://opentransportdata.swiss/en/cookbook/gtfs/#routestxt), there is also a remark on "artificial lines that shouldn't be shown to users". These route_short_names contain the character 'Y'. Therefore, we filter them out by conditionning on "route_short_name".

# + language="spark"
# routes = routes.filter((~F.col('route_short_name').contains('Y')))
# routes.show(5)

# + language="spark"
# zurich_stop_times_dated_transport = zurich_stop_times_dated.join(routes, 'route_id')
# zurich_stop_times_dated_transport.show(5)

# + language="spark"
# zurich_stop_times_dated_transport.select('route_desc').distinct().collect()
# -

# Amongst all these transport methods, we remark "EXT", which are "extra trains", and thus not reliable.

# + language="spark"
# zurich_final_connections= zurich_stop_times_dated_transport.filter(~F.col("route_desc").isin(["EXT"]))
# zurich_final_connections.select('route_desc').distinct().collect()

# + language="spark"
# zurich_final_connections.show(10, False)
# -
# We save these daily connections timetable to HDFS in orc format.

# + language="spark"
# weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday"]
# for day in weekdays:
#     zurich_final_connections.filter(zurich_final_connections[day] == True).drop(*weekdays).drop("saturday","sunday").repartition(1)\
#     .write.mode('overwrite').option("header","true").format("csv").save(f"/user/{username}/hive/sbb_connections_{day}.csv")
# zurich_final_connections.drop(*weekdays).drop("saturday","sunday").repartition(1)\
# .write.mode('overwrite').option("header","true").format("csv").save(f"/user/{username}/hive/sbb_connections_week.csv")

# + language="spark"
# from pyspark.sql.functions import col, expr
# delays_df_2 = delays_df.groupBy("haltestellen_name", "train_number", "arrival").agg(expr("percentile_approx(delay, 0.9)").alias("90thPercentile"),
#                                              expr("percentile_approx(delay, 0.95)").alias("95thPercentile"),
#                                              expr("percentile_approx(delay, 0.85)").alias("85thPercentile"),
#                                              expr("percentile_approx(delay, 0.8)").alias("80thPercentile"),
#                                              expr("percentile_approx(delay, 0.75)").alias("75thPercentile"),
#                                              expr("percentile_approx(delay, 0.70)").alias("70thPercentile"),
#                                              expr("percentile_approx(delay, 0.65)").alias("65thPercentile"),
#                                              expr("percentile_approx(delay, 0.60)").alias("60thPercentile"),
#                                              expr("percentile_approx(delay, 0.55)").alias("55thPercentile"),
#                                              expr("percentile_approx(delay, 0.50)").alias("50thPercentile"),
#                                              expr("avg(delay)").alias("AverageDelay"),
#                                              expr("count(*)").alias("GroupByCount"))
# delays_df_2.show()

# + language="spark"
# delays_df_2.count()

# + language="spark"
# delays_df.count()

# + language="spark"
# from pyspark.sql.functions import countDistinct
# delays_co = delays_df.select(countDistinct("train_number"))
# delays_co.show()

# + language="spark"
# delays_df_2.repartition(1).write.mode('overwrite').option("header","true").format("csv").save(f"/user/{username}/hive/sbb_delays_percentile")
# -

sc.stop


