from mrjob.job import MRJob
from mrjob.step import MRStep


class MRFlight(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (year, month, day, day_of_week, airline, flight_nr, tail_nr, origin_airport,
         destination_airport, scheduled_departure, departure_time, departure_delay, taxi_out,
         wheels_off, scheduled_time, elapsed_time, air_time, distance, wheels_on, taxi_in,
         scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled, cancellation_reason,
         air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay) = line.split(',')
        yield airline, int(cancelled)

    def reducer(self, key, values):
        total = 0
        num_rows = 0
        for i in values:
            total += i
            num_rows += 1
        yield key, total / num_rows

if __name__ == '__main__':
    MRFlight.run()