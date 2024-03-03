# MapReduce Tasks:
# Task 4. Write MapReduce codes to perform the tasks using the files you’ve downloaded on your EMR Instance:
# b. Which pickup location generates the most revenue? 

from mrjob.job import MRJob # Importing mrjob library
from mrjob.step import MRStep  # Importing library to define multistep job

# Extending the MRJob class 
# This includes definition of map and reduce functions
class MostRevenue_PickupLocation(MRJob): 

    # Defining two step jobs
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]
    
    # mapper function
    # Our mapper function takes a each record of file as an input 
    # Yielding the pickup_location and revenue as keys,value pair
    def mapper(self, _, line):
        if not line.startswith('VendorID'): # skipping the header row
            fields = line.split(',')
            pickup_location = fields[7]
            revenue = float(fields[16])
            yield pickup_location, revenue

    # reducer function
    # Produces value as sum of revenue for each unique pickup_location. Key is set as none for every pair.
    def reducer(self, pickup_location, revenues):
        yield None, (sum(revenues), pickup_location)

    # Final reducer function
    # Produces value as record having maximum value among list of total revenues per pickup_location. 
    # Key is the pickup_location who is having maximum total revenue.
    def final_reducer(self, _, max_revenues):
        max_revenue, pickup_location = max(max_revenues)
        yield pickup_location, max_revenue

# main function
if __name__ == '__main__': 
    MostRevenue_PickupLocation.run() # calling the run function