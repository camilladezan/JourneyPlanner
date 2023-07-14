from collections import defaultdict
from datetime import time, datetime, date
import pandas as pd
import datetime as dt

class RobustPlanner:
    def __init__(self):
        """
        Initiates the algorithm : Loads all daily connections tables, footpaths and trips list.
        """
        self.monday = pd.read_csv(f"../data/sbb_connections_monday_perc.csv").sort_values(by=['arrival_time'], ascending=False).drop(["Unnamed: 0"], axis = 1)
        self.tuesday = pd.read_csv(f"../data/sbb_connections_tuesday_perc.csv").sort_values(by=['arrival_time'], ascending=False).drop(["Unnamed: 0"], axis = 1)
        self.wednesday = pd.read_csv(f"../data/sbb_connections_wednesday_perc.csv").sort_values(by=['arrival_time'], ascending=False).drop(["Unnamed: 0"], axis = 1)
        self.thursday = pd.read_csv(f"../data/sbb_connections_thursday_perc.csv").sort_values(by=['arrival_time'], ascending=False).drop(["Unnamed: 0"], axis = 1)
        self.friday = pd.read_csv(f"../data/sbb_connections_friday_perc.csv").sort_values(by=['arrival_time'], ascending=False).drop(["Unnamed: 0"], axis = 1)
        self.footpaths = pd.read_csv(f"../data/sbb_zurich_short_walks.csv")
        self.stops = pd.read_csv(f"../data/sbb_stations_zurich.csv")
    
    def add_time(self, timestamp : str, duration : int) -> str : 
        """
        Adds duration minutes to timestamp
        returns : result of addition between a time (HH:MM:SS) and a duration in minutes

        timestamp : a time of day (HH:MM:SS)
        duration  : minutes
        """
        delta = dt.timedelta(minutes=duration)
        dummy_date = dt.date.today()
        return (dt.datetime.combine(dummy_date, dt.datetime.strptime(timestamp, '%H:%M:%S').time()) + delta).time().strftime("%H:%M:%S")
    
    def get_coords(self, station : str) -> list:
        """
        Gives coordinates of a station
        returns : list of ["lat","lon"]
        
        station : station name
        """
        return self.stops.loc[self.stops['stop_name'] == station][["stop_lat","stop_lon"]].values.tolist()[0]

    def recover_path(self, S : pd.DataFrame, starting_stop : str, end_stop : str, log) -> list:
        """
        Recovers the fastest path found by CSA algorithm.
        Algorithmically, basically iteration along a (kind of) linked list determined by starting stop and end stop.

        Returns       :  list comprised of the legs of the journey

        S             : table comprised of the latest departure time for each stop, with the associated connection
        starting_stop : start of the journey of the user
        end_stop      : end of the journey of the user
        """
        path = []
        current_leg = S[starting_stop]
        prev_trip_id = current_leg["trip_id"]
        if prev_trip_id != "Walking":
            transport_name, transport_line = current_leg["route_desc"],current_leg["route_short_name"]
        else:
            transport_name, transport_line = "Walking", ""
        prev_dept_station = current_leg["departure_station"]
        prev_dept_time = current_leg["departure_time"]
        prev_arr_time = current_leg["arrival_time"]
        prev_arr_station = current_leg["arrival_station"]
        if log:
            print(current_leg)
        while current_leg["trip_id"] is not None : #while we haven't arrived yet
            current_trip_id = current_leg["trip_id"]
            if current_trip_id != prev_trip_id:
                path.append([prev_trip_id, transport_name, transport_line, prev_dept_station, \
                             *self.get_coords(prev_dept_station), prev_dept_time, \
                             prev_arr_station, *self.get_coords(prev_arr_station), prev_arr_time]) 
                prev_trip_id = current_trip_id
                prev_dept_station = current_leg["departure_station"]
                prev_dept_time = current_leg["departure_time"]
                prev_arr_time = current_leg["arrival_time"]
                if prev_trip_id != "Walking":
                    transport_name, transport_line = current_leg["route_desc"],current_leg["route_short_name"]
                else:
                    transport_name, transport_line = "Walking", ""
            prev_arr_station = current_leg["arrival_station"]
            prev_arr_time = current_leg["arrival_time"]
            current_leg = S[current_leg["arrival_station"]] #we go to the connection of the next leg. This is like jumping to next linked list item
            if log:
                print(current_leg)

        path.append([prev_trip_id,transport_name, transport_line, prev_dept_station, \
                             *self.get_coords(prev_dept_station), prev_dept_time, \
                             prev_arr_station, *self.get_coords(prev_arr_station), prev_arr_time]) 
        return path
    
    def CSA(self, starting_stop : str, arrival_stop : str, arrival_time: str, day : str, log : bool, prob : int) -> list : 
        '''
        Connections Scan Algorithm to maximize the starting time

        Return: List of connections which are each quintuples :  
                [means of transport (trip ID or 'Walking'), starting time (datetime), 
                starting_stop (name), arriving_time (datetime), arriving stop (name)]
                according to the paper

        starting_stop : Starting stop for algorithm
        arrival_stop  : End stop for algorithm
        arrival_time  : Demanded max arrival_time by the user
        day           : Day of week
        prob          : Confidence percentile (100 = delays not taken into account)

        '''   
        delta = 1 #minimum time to transfer in station
        percentile = {95 : "95_perc_arrival", 90 : "90_perc_arrival",85 : "85_perc_arrival", 80 : "80_perc_arrival",75 : "75_perc_arrival", 70 : "70_perc_arrival",\
                      65 : "65_perc_arrival", 60 : "60_perc_arrival",55 : "55_perc_arrival", 50 : "50_perc_arrival"}
        print(prob)
        if prob == 100:
            arrival_index = "arrival_time"
        else:
            arrival_index = percentile[prob]
        daily_connections = {"monday" : self.monday, "tuesday" : self.tuesday, 
                             "wednesday" : self.wednesday, "thursday" : self.thursday, "friday" : self.friday}
        weekday_table = daily_connections[day]
        # Set all S[x] to -infty and T[x] to false
        # S : { stop : [trip_id, tentative_start_time, start_stop, arrive_time, arrive_stop]}
        S = defaultdict(lambda: pd.Series(data = [None, time.min.strftime("%H:%M:%S"), None, time.max.strftime("%H:%M:%S"), None],
                                          index = ['trip_id', 'departure_time', 'departure_station', 'arrival_time', 'arrival_station']))
        T = defaultdict(lambda : False)
        S[arrival_stop] = pd.Series(data = [None, arrival_time, None, time.max.strftime("%H:%M:%S"), None], 
                                    index = ['trip_id', 'departure_time', 'departure_station', 'arrival_time', 'arrival_station'])

        # For all footpaths arriving from the arrival stop, set time as per algorithm
        for i, footpath in self.footpaths[self.footpaths["arrival"] == arrival_stop].iterrows():
            S[footpath["departure"]] = pd.Series(data = ['Walking', self.add_time(arrival_time,-footpath['duration']),footpath['departure'], arrival_time, arrival_stop],
                                  index = ['trip_id', 'departure_time', 'departure_station', 'arrival_time', 'arrival_station'])
        
        # Take all connections via means of transport which arrive before wanted arrival time

        weekday_table = weekday_table[weekday_table['arrival_time']<arrival_time].\
        sort_values(by=['arrival_time', 'route_short_name', 'trip_id'], ascending=[False,False,False]).\
        drop_duplicates(subset = ['route_id','departure_time','arrival_time','route_desc','route_short_name','departure_station','arrival_station'])
        # Core of the algorithm
        for i, connection in weekday_table.iterrows():
            # End conditition according to algorithm
            # if S[starting_stop]["departure_time"] >= connection["arrival_time"]:
            if S[starting_stop]["departure_time"] >= connection["departure_time"]:
                break 
            #If the connection is feasible, i.e. either we are on the trip that has the connection
            # or we depart after departure time + delta (min time of transfer)
            if T[connection["trip_id"]] or \
            (S[connection["arrival_station"]]["departure_time"] >= self.add_time(connection[arrival_index],delta)):
                # Check if the connection that allows the latest departure time is the current one
                if S[connection["departure_station"]]["departure_time"] < connection["departure_time"]:
                    T[connection["trip_id"]] = True
                    S[connection["departure_station"]] = connection
                    # We add footpath information for nearby stations
                    for _, footpath in self.footpaths[self.footpaths["arrival"] == connection["departure_station"]].iterrows():
                        if S[footpath["departure"]]["departure_time"]< self.add_time(connection["departure_time"],-footpath["duration"]):
                            S[footpath["departure"]] = pd.Series(data =['Walking', self.add_time(connection["departure_time"],-footpath["duration"]),\
                                                                        footpath['departure'], connection["departure_time"], footpath["arrival"]],
                                      index = ['trip_id', 'departure_time', 'departure_station', 'arrival_time', 'arrival_station'])
        if S[starting_stop]["trip_id"] is not None:
            return self.recover_path(S, starting_stop, arrival_stop, log)
        else:
            return []
        
    def run(self, start_station : str, end_station : str, arrival_time : str, day : str, prob = 100, log = False) -> list:
        """
        Returns the shortest path in the form of a list of lists(transport, line_name, departure_station, departure_longitude, departure_latitude,
        departure_time, arrival_station, arrival_longitude, arrival_latitude, arrival_time)
        
        start_station : starting station of journey
        end_station   : ending station of journey
        arrival_time  : maximum arrival time
        day           : day of the week
        prob          : confidence interval (100 = delays not taken into account)
        log           : print logs of shortest path         
        """
        return self.CSA(start_station, end_station, arrival_time, day, log, prob)


app = RobustPlanner()
app.run("Zürich HB", "Zürich, Zoo", "10:00:00", "tuesday")


