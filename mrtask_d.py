# MapReduce Tasks:
# Task 4. Write MapReduce codes to perform the tasks using the files you’ve downloaded on your EMR Instance:
# d. What is the average trip time for different pickup locations?

from mrjob.job import MRJob # importing mrjob library
from datetime import datetime # importing datetime library

# extending the MRJob class
class Avg_Trip_Time(MRJob): 

    def parse_datetime(self, datetime_str): # parse datetime function
        formats = ['%d-%m-%Y %H:%M:%S', '%d-%m-%Y %H:%M', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']
        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                pass
        raise ValueError('no valid date format found')
    
    # mapper function
    # Our mapper function takes a each record of file as an input 
    # Yielding the pickup_location and (trip_time,1) as keys,value pair
    def mapper(self, _, line): # mapper function
        if not line.startswith('VendorID'): # Skip the header line
            fields = line.split(',')
            pickup_location = fields[7]
            pickup_datetime = self.parse_datetime(fields[1])
            dropoff_datetime = self.parse_datetime(fields[2])
            trip_time = (dropoff_datetime - pickup_datetime).total_seconds() / 60.0 # Calculating the trip time
            yield pickup_location, (trip_time, 1)

    # combiner function
    # produces pickup_location, (total_trip_time, total_count) per pickup_location as key value pair
    def combiner(self, pickup_location, trip_times): 
        total_trip_time = 0
        total_count = 0
        for trip_time, count in trip_times:
            total_trip_time += trip_time
            total_count += count
        yield pickup_location, (total_trip_time, total_count)

    # reducer function
    # produces pickup_location and average_trip_time i.e. total_trip_time per number of trips per pickup_location as key value pair
    def reducer(self, pickup_location, trip_times): 
        total_trip_time = 0
        total_count = 0
        for trip_time, count in trip_times:
            total_trip_time += trip_time
            total_count += count
        average_trip_time = total_trip_time / total_count
        yield pickup_location, average_trip_time

# main function
if __name__ == '__main__': 
    Avg_Trip_Time.run() # calling the run function