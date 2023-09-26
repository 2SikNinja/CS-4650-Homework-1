import re
from mrjob.job import MRJob

QUALITY_RE = re.compile(r"[01459]")

class WindDirectionTemperature(MRJob):

    def mapper(self, _, line):
        val = line.strip()
        
        # Extract temperature, quality of temperature, wind direction, and quality of wind direction
        temp, temp_q, wind_dir, wind_dir_q = val[87:92], val[92:93], val[60:63], val[63:64]

        # If temperature and wind direction are valid, yield wind direction, temperature
        if (temp != "+9999" and re.match(QUALITY_RE, temp_q) and wind_dir != "999" and re.match(QUALITY_RE, wind_dir_q)):
            yield wind_dir, int(temp)

    def reducer(self, key, values):
        temps = list(values)
        yield key, {
            "low": min(temps),
            "high": max(temps),
            "count": len(temps)
        }

if __name__ == '__main__':
    WindDirectionTemperature.run()
