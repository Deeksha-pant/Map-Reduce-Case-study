# MapReduce Tasks:
# Task 4. Write MapReduce codes to perform the tasks using the files you’ve downloaded on your EMR Instance:
# c. What are the different payment types used by customers and their count? 
#    The final results should be in a sorted format.

from mrjob.job import MRJob # Importing mrjob library
from mrjob.step import MRStep  # Importing library to define multistep job

# Extending the MRJob class 
# This includes definition of map and reduce functions
class Payment_Types(MRJob): 
    # Defining step jobs
    def steps(self): 
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            # MRStep(reducer=self.reducer_sort_results),
            MRStep(reducer=self.reducer_output_result)
        ]
    
    # mapper function
    # Our mapper function takes a each record of file as an input 
    # Yielding the payment_type and 1 as keys, value pair
    def mapper(self, _, line): # mapper function
        # Skip header line
        if not line.startswith('VendorID'): # skipping the header row
            fields = line.split(',')
            payment_type = fields[9]
            yield payment_type, 1

    # reducer function
    # Produces value as sum of counts for each unique payment_type.
    def reducer(self, payment_type, counts):
        yield None, (sum(counts), payment_type)

    # Final reducer function produces output in sorted manner
    def reducer_output_result(self, _, sorted_results):
        for count, payment_type in sorted(sorted_results, reverse=True):
            yield payment_type, count

# main function
if __name__ == '__main__': 
    Payment_Types.run() # calling the run function