# # STEP 0: Set up the environment 

# ## Prepare Hive env: Get data from HDFS with Hive
# + Why use hive. We can querry the data that we will use without using much resources from spark, It will allow us to simplify the querries and get a clean dataset.
# + This will allow is to make an EDA using just normal python libraries 

# +
# In this step we are loading the libriaries we will use for this part of the project
import os
import re
import pandas as pd
from IPython import get_ipython
import matplotlib.pyplot as plt
import warnings
import numpy as np
from pyhive import hive

pd.set_option("display.max_columns", 50)
warnings.simplefilter(action='ignore', category=UserWarning)
# %matplotlib inline


# +
# Set python variables from environment variables to use in the whole notebook 
username = os.environ['USERNAME']
hive_host = os.environ['HIVE_SERVER2'].split(':')[0]
hive_port = os.environ['HIVE_SERVER2'].split(':')[1]

# create connection to hive based on past assignments 
conn = hive.connect(
    host=hive_host,
    port=hive_port,
    # auth="KERBEROS",
    # kerberos_service_name = "hive"
)

# create cursor
cur = conn.cursor()

print(f"Username {username}")
print(f"Connected to {hive_host}:{hive_port}")
# -

# ### First check if the tables are already in the database

# This step allows us to see which tables we have in our hive database, if they are in memory we can query them directly and skip these steps to create tables

# Using the database we created in the env-preparation 
query = f"""USE {username}""" 
cur.execute(query)
query = f"""
        SHOW TABLES IN {username}
"""
cur.execute(query)
cur.fetchall()

# ## Run this only if you don't have the tables already

# <div class="alert alert-warning">
# <strong>Stop and check:</strong> 
#    If you already have the tables in hadoop, there is no need to re run these cells, as they are just to create the external tables and it may take some time, in last step you can see which tables you have in your database already.
# </div>

# The description of the data can be seen in the README file. To know it is important when creating the tables as it is neded to specify the name and datatype to use.
# * Create the EXTERNAL table for the **_istdaten_** dataset, stored as ORC.
# * Table name: `sbb_orc`

# +
query = f"""
            DROP TABLE IF EXISTS {username}.sbb_orc
            """
cur.execute(query)

query = f"""
CREATE EXTERNAL TABLE {username}.sbb_orc(
        betriebstag STRING,
        fahrt_bezichner STRING,
        betreiber_id STRING,
        betreiber_abk STRING,
        betreiber_name STRING,
        produkt_id STRING,
        linien_id STRING,
        linien_TEXT STRING,
        umlauf_id STRING,
        verkehrsmittel_text STRING,
        zusatzfahrt_tf STRING,
        faellt_aus_tf STRING,
        bpuic STRING,
        haltestellen_name STRING,
        ankunftszeit STRING,
        an_prognose STRING,
        an_prognose_status STRING,
        abfahrtszeit STRING,
        ab_prognose STRING,
        ab_prognose_status STRING,
        durchfahrt_tf STRING
    )
    
    PARTITIONED BY (year INTEGER, month INTEGER)
    STORED AS ORC
    LOCATION '/data/sbb/part_orc/istdaten' """
  
cur.execute(query)

query = f""" 
    MSCK REPAIR TABLE sbb_orc
"""
cur.execute(query)

# +
query = f"""
    SELECT * FROM {username}.sbb_orc LIMIT 5
"""

pd.read_sql(query, conn).head(10)
# -

# * Create the calendar EXTERNAL table as `sbb_orc_calendar`

# +
query = f"""
            DROP TABLE IF EXISTS {username}.sbb_orc_calendar
            """
cur.execute(query)

query = f"""
CREATE EXTERNAL TABLE {username}.sbb_orc_calendar(
        service_id STRING,
        monday STRING,
        tuesday STRING,
        wednesday STRING,
        thursday STRING,
        friday STRING,
        saturday STRING,
        sunday STRING,
        start_date STRING,
        end_date STRING,
        year INTEGER, 
        month INTEGER, 
        day INTEGER
    )
    
---    PARTITIONED BY (year INTEGER, month INTEGER, day INTEGER)
    STORED AS ORC
    LOCATION '/data/sbb/part_orc/timetables/calendar'
    """
  
cur.execute(query)

query = f""" 
    MSCK REPAIR TABLE sbb_orc_calendar
"""
cur.execute(query)

query = f"""
    SELECT * FROM {username}.sbb_orc_calendar LIMIT 10
"""

pd.read_sql(query, conn).head(10)
# -

# * Create the route EXTERNAL table as `sbb_orc_routes`

# +
query = f"""
            DROP TABLE IF EXISTS {username}.sbb_orc_routes
            """
cur.execute(query)

query = f"""
CREATE EXTERNAL TABLE {username}.sbb_orc_routes(
        route_id STRING,
        agency_id STRING,
        route_short_name STRING,
        route_long_name STRING,
        route_desc STRING,
        route_type STRING
    )
    
    PARTITIONED BY (year INTEGER, month INTEGER, day INTEGER)
    STORED AS ORC
    LOCATION '/data/sbb/part_orc/timetables/routes'
  """
cur.execute(query)

query = f""" 
    MSCK REPAIR TABLE sbb_orc_routes
"""
cur.execute(query)

query = f"""
    SELECT * FROM {username}.sbb_orc_routes LIMIT 10
"""

pd.read_sql(query, conn).head(10)
# -

# * Create the stop times EXTERNAL table as `sbb_orc_stop_times`

# +
query = f"""
            DROP TABLE IF EXISTS {username}.sbb_orc_stop_times
            """
cur.execute(query)

query = f"""
            CREATE EXTERNAL TABLE {username}.sbb_orc_stop_times(
                    trip_id STRING,
                    arrival_time STRING,
                    departure_time STRING,
                    stop_sequence STRING,
                    pickup_type STRING,
                    drop_off_type STRING
                )
                PARTITIONED BY (year INTEGER, month INTEGER, day INTEGER)
                STORED AS ORC
                LOCATION '/data/sbb/part_orc/timetables/stop_times'
              """
cur.execute(query)

query = f""" 
    MSCK REPAIR TABLE sbb_orc_stop_times
"""
cur.execute(query)

query = f"""
    SELECT * FROM {username}.sbb_orc_stop_times LIMIT 5
"""

pd.read_sql(query, conn).head(10)
# -

# * Create the stops EXTERNAL table as `sbb_orc_stops`

# +
query = f"""
            DROP TABLE IF EXISTS {username}.sbb_orc_stops
            """
cur.execute(query)

query = f"""
            CREATE EXTERNAL TABLE {username}.sbb_orc_stops(
                    stop_id STRING,
                    stop_name STRING,
                    stop_lat FLOAT ,
                    stop_lon FLOAT ,
                    location_type STRING,
                    parent_station STRING
                )
---                PARTITIONED BY (year INTEGER, month INTEGER, day INTEGER)
                STORED AS ORC
                LOCATION '/data/sbb/orc/allstops'
              """

cur.execute(query)

query = f""" 
    MSCK REPAIR TABLE sbb_orc_stops
"""
cur.execute(query)

query = f"""
    SELECT * FROM {username}.sbb_orc_stops LIMIT 10
"""

pd.read_sql(query, conn).head(10)
# -
# * Create the transfers EXTERNAL table as `sbb_orc_transfers`

# +
query = f"""
            DROP TABLE IF EXISTS {username}.sbb_orc_transfers
            """
cur.execute(query)

query = f"""
            CREATE EXTERNAL TABLE {username}.sbb_orc_transfers(
                from_stop_id STRING,
                to_stop_id STRING,
                transfer_type STRING,
                min_transfer_time STRING
            )

            PARTITIONED BY (year INTEGER, month INTEGER, day INTEGER)
            STORED AS ORC
            LOCATION '/data/sbb/part_orc/timetables/transfers'
            """
cur.execute(query)

query = f""" 
    MSCK REPAIR TABLE sbb_orc_transfers
"""
cur.execute(query)

query = f"""
    SELECT * FROM {username}.sbb_orc_transfers LIMIT 10
"""

pd.read_sql(query, conn).head(10)
# -
# * Create the trips EXTERNAL table as `sbb_orc_trips`

# +
query = f"""
            DROP TABLE IF EXISTS {username}.sbb_orc_trips
            """
cur.execute(query)

query = f"""
            CREATE EXTERNAL TABLE {username}.sbb_orc_trips(
                route_id STRING,
                service_id STRING,
                trip_id STRING,
                trip_headsign STRING,
                trip_short_name STRING,
                direction_id STRING
            )
            PARTITIONED BY (year INTEGER, month INTEGER, day INTEGER)
            STORED AS ORC
            LOCATION '/data/sbb/part_orc/timetables/trips'
            """
cur.execute(query)

query = f""" 
    MSCK REPAIR TABLE sbb_orc_trips
"""
cur.execute(query)

query = f"""
    SELECT * FROM {username}.sbb_orc_trips LIMIT 10
"""

pd.read_sql(query, conn).head(10)
# -
# ## Get the coordinates from ZürichHB zone and visualization. 

# * This part is important as it will help us to filter just the stations that are within a **15 km** from Zürich HB. We use stations within a 15 km from Zürich HB, since even if we only look at trips in a 10 km radius, connections outside of the 10 km radius are still possible.
# * First we create an intermediate table that will compute the distance between each station and Zürich HB 
# * In ordet to simplify the tasks we filtered the whole table within a certain area, which is larger than the 15km but much smaller than the whole data stations. 

# +
# This is to get the coordinates from Zürich HB Create intermediate tables to get radius 

# Drops the intermediate table if its still in memory 
query = f"""
    DROP TABLE IF EXISTS {username}.sbb_station_radius
    """
cur.execute(query)

# create intermidiate table to compute radius 
lat = 47.378178 
lon = 8.540212
R = 6378
max_distance = 20

# This table helps to get the radius from one coordinate to the other (a), 
# in order to not go through all the elements of the table, this is bounded by the lon and lat close to zurich 
# This is created and ereased at the end. 
#                POW(SIN((RADIANS({lat})-RADIANS(stop_lat))*0.5),2) + COS(RADIANS({lat})) * COS(RADIANS(stop_lat)) * POW(SIN((RADIANS({lon})-RADIANS(stop_lon))*0.5),2) AS a

query = f"""
    CREATE EXTERNAL TABLE {username}.sbb_station_radius
    STORED AS ORC 
    AS 
        SELECT stop_id, 
               stop_name,
               stop_lat,
               stop_lon,
               location_type,
               parent_station
    FROM {username}.sbb_orc_stops
    WHERE stop_lat > 47.05 
      AND stop_lat < 47.75
      AND stop_lon > 8
      AND stop_lon < 9
"""
cur.execute(query)

# +
# This is to get the coordinates from Zürich HB Create intermediate tables to get radius 

# Drops the intermediate table if its still in memory 
query = f"""
    DROP TABLE IF EXISTS {username}.sbb_station_radius_sin_cos
    """
cur.execute(query)

# create intermidiate table to compute radius 
lat = 47.378178 
lon = 8.540212
R = 6378
max_distance = 20

# This table helps to get the radius from one coordinate to the other (a), 
# in order to not go through all the elements of the table, this is bounded by the lon and lat close to zurich 
# This is created and ereased at the end.       

query = f"""
    CREATE EXTERNAL TABLE {username}.sbb_station_radius_sin_cos
    STORED AS ORC 
    AS 
        SELECT stop_id, 
               stop_name,
               stop_lat,
               stop_lon,
               location_type,
               parent_station,
               POW(SIN((RADIANS({lat})-RADIANS(stop_lat))*0.5),2) + COS(RADIANS({lat})) * COS(RADIANS(stop_lat)) * POW(SIN((RADIANS({lon})-RADIANS(stop_lon))*0.5),2) AS a
    FROM {username}.sbb_station_radius

"""
cur.execute(query)

# +
# Drops the table with all stations from zurich if is in memory, so it can be corrected if necessary. 
query = f""" DROP TABLE IF EXISTS {username}.sbb_stations_zurich """
cur.execute(query)

# Creates the table with all stations from zurich
# following this formula https://www.analyzemath.com/Geometry_calculators/distance-between-two-points-on-earth.html
# This takes in consideration the radius previously computed and transforms it to distance betwee stations. 
query = f"""
        CREATE EXTERNAL TABLE {username}.sbb_stations_zurich
        STORED AS ORC 
        AS 
            SELECT stop_id, 
                   stop_name,
                   stop_lat,
                   stop_lon,
                   location_type,
                   parent_station,
                   {R}*2*atan(sqrt(a)/sqrt(1-a)) AS RADIUS
            FROM {username}.sbb_station_radius_sin_cos
            WHERE {R}*2*atan(sqrt(a)/sqrt(1-a)) <= {max_distance}
            SORT BY RADIUS DESC
"""
cur.execute(query)

query = f"""
    DROP TABLE IF EXISTS {username}.sbb_station_radius
"""
cur.execute(query)

#See that the table was correctly created, it should just have the wanted stations and the distance from Zürich HB

query = f"SELECT * FROM {username}.sbb_stations_zurich"
pd.read_sql(query, conn).head(5)
# -

# ### Visualization of the stations
# This step is to verify that the past steps where correctly executed, in the map you should see the stations in the desired radius from ZürichHB as it's center. 
# You should see something like this: 
#
# <div>
# <img src="../figs/Map_Zurich.png"/>
# </div>
#

# +
import plotly.express as px

lat = 47.378178 
lon = 8.540212
R = 6378
max_distance = 25

query = f"""
        SELECT stop_id,
               stop_name,
               stop_lat,
               stop_lon,
               radius
        FROM {username}.sbb_stations_zurich
        WHERE RADIUS < {max_distance}
        ORDER BY stop_name
        """
df_stations = pd.read_sql(query, conn)


fig = px.density_mapbox(df_stations, lat='stop_lat', lon='stop_lon', radius=4,hover_name='stop_name', 
                        center=dict(lat=lat, lon=lon), zoom=10,
                        mapbox_style="stamen-terrain", title= 'Zurich stations within maximum distance' )
fig.update_layout(
    width=1000,  # Specify the width in pixels
    height=600  # Specify the height in pixels
)
fig.show()

pd.read_sql(query, conn).head(20)
# -


query = f"""Select stop_id,
                   stop_name,
                   COUNT(stop_name)                    
            FROM {username}.sbb_stations_zurich
            GROUP BY stop_id, stop_name
            HAVING COUNT(stop_name) >1
        """
pd.read_sql(query, conn).head(10)

# +
###### These next queries are just to see the structure of the tables, they dont really represent any process yet. 
# -

query = f"""SELECT * FROM {username}.sbb_orc_stop_times
            WHERE trip_id LIKE '%1.TA.96-182-3-j22-1.1.H%'
            LIMIT 10"""
pd.read_sql(query, conn).head(5)

query = f"""SELECT * FROM {username}.sbb_orc_trips
            WHERE trip_id LIKE '%1.TA.96-182-3-j22-1.1.H%'
            LIMIT 10"""
pd.read_sql(query, conn).head(5)

query = f"""SELECT * FROM {username}.sbb_orc
            LIMIT 10"""
pd.read_sql(query, conn).head(5)
## Use the 

# <div class="alert alert-warning">
# <strong>Stop here before going on:</strong> 
#    Just run this query if you don't have the table yet, as it may take some time.
# </div>
#
# This step is to prepare the data only for the selected radius. First we compute the delays between stations. 

# +
### Run this if you dont have the table 
# Drops the table with all stations from zurich 
query = f""" DROP TABLE IF EXISTS {username}.sbb_istdaten_zurich """
cur.execute(query)

# Creates the table with all stations from zurich
# Using the Istdaten table to get the data only for zürich to reduce processing time and memory in spark later on, 
# instead of using the whole `istdaten` dataset we use a reduced data partition. 
query = f"""
        CREATE EXTERNAL TABLE {username}.sbb_istdaten_zurich
        STORED AS ORC 
        AS 
            SELECT * FROM {username}.sbb_orc
            INNER JOIN {username}.sbb_stations_zurich 
                ON sbb_orc.haltestellen_name = sbb_stations_zurich.stop_name
"""
cur.execute(query)
# -
query = f"""Select * FROM {username}.sbb_istdaten_zurich
            LIMIT 10
        """
pd.read_sql(query, conn).head(5)

# This is just to visualize the type of transportation in this table 

# +
query = f"""SELECT LOWER(produkt_id) AS produkt,
                   COUNT(LOWER(produkt_id)) AS count
            FROM {username}.sbb_istdaten_zurich
            GROUP BY produkt_id
            LIMIT 10
        """
transport_type_df = pd.read_sql(query, conn)

plt.bar(transport_type_df['produkt'], transport_type_df['count'])
# -

# ## Delay tables. 
# * We parse the data to have a correct format of time and date to work with and compute the delays per station.  

query=f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {username}.sbb_zurich_parsed
    STORED AS ORC
    AS
    SELECT 
        haltestellen_name,
        UPPER(an_prognose_status),
        FROM_UNIXTIME(UNIX_TIMESTAMP(ankunftszeit, 'dd.MM.yyyy HH:mm'), 'yyyy-MM-dd HH:mm') AS arrival,
        FROM_UNIXTIME(UNIX_TIMESTAMP(an_prognose, 'dd.MM.yyyy HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') AS real_arrival,
        FROM_UNIXTIME(UNIX_TIMESTAMP(abfahrtszeit, 'dd.MM.yyyy HH:mm'), 'yyyy-MM-dd HH:mm') AS departure,
        FROM_UNIXTIME(UNIX_TIMESTAMP(ab_prognose, 'dd.MM.yyyy HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') AS real_departure,
        FROM_UNIXTIME(UNIX_TIMESTAMP(betriebstag, 'dd.MM.yyyy'), 'yyyy-MM-dd') AS operating_date,
        linien_id as train_number,
        LOWER(produkt_id) AS produkt_id
    FROM {username}.sbb_istdaten_zurich
"""
cur.execute(query)


query = f"""Select * FROM {username}.sbb_zurich_parsed
            LIMIT 10
        """
pd.read_sql(query, conn)



# Now the delay table is ready the next steps can be done in spark as it has more complex calculations between stations, the processing is more complex.  

### Maybe do this in spark. It takes too long in hive. The processing becomes more complex, we already have the tables, the next steps are:
# TODO: Check walkable paths (starteted something in spark based in past repos)
# TODO: Clean data for planner 
# TODO: Create routes 
query = f"""Select * FROM {username}.sbb_orc_transfers
            ORDER BY min_transfer_time 
        """
pd.read_sql(query, conn)


