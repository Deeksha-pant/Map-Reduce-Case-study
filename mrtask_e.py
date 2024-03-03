# MapReduce Tasks:
# Task 4. Write MapReduce codes to perform the tasks using the files you’ve downloaded on your EMR Instance:
# e. Calculate the average tips to revenue ratio of the drivers for different pickup locations in sorted format.

from mrjob.job import MRJob # importing mrjob library

# extending the MRJob class
class AverageTips_RevenueRatio(MRJob): 

    # mapper function
    # Our mapper function takes a each record of file as an input 
    # Yielding the pickup_location and (tips,total_revenue) per pickup_location as keys,value pair
    def mapper(self, _, line): 
        if not line.startswith('VendorID'): # Skip the header line
            fields = line.split(',')
            pickup_location = fields[7]
            total_revenue = float(fields[16])
            tips = float(fields[13])
            yield pickup_location, (tips, total_revenue)

    # combiner function
    # Yielding the pickup_location and (total_tips,total_revenue) per pickup_location as keys,value pair
    def combiner(self, pickup_location, tips_revenues): 
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        yield pickup_location, (total_tips, total_revenue)

    # reducer function
    # Yielding the pickup_location and average_tips_to_revenue_ratio (i.e. ratio of total_tips and total_revenue) per pickup_location as keys,value pair
    def reducer(self, pickup_location, tips_revenues): 
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        average_tips_to_revenue_ratio = total_tips / total_revenue
        yield pickup_location, average_tips_to_revenue_ratio

# main function
if __name__ == '__main__': 
    AverageTips_RevenueRatio.run() # calling the run function