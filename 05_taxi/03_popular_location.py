from mrjob.job import MRJob
from mrjob.step import MRStep


class MRTaxi(MRJob):


    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper_keys,
                   reducer=self.reducer_sorted)
        ]

    def mapper(self, _, line):
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude,
         pickup_latitude, RatecodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
         fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount) = line.split(',')
        pickup_latitude = pickup_latitude[:8]
        pickup_longitude = pickup_longitude[:9]
        yield (float(pickup_latitude), float(pickup_longitude)), 1


    def reducer(self, key, values):
        yield key, sum(values)


    def mapper_keys(self, key, value):
        yield None, (value, key)

    def reducer_sorted(self, key, values):
        self.results = []
        for i in values:
            self.results.append((key, i))
        yield None, sorted(self.results, reverse=True)[:10]

if __name__ == '__main__':
    MRTaxi.run()