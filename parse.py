import os
import shutil



source = "/net/20/kun/source/HOUREMIS/HOUREMIS_"
source_inp = "/net/20/kun/source/inps_old/Linerun_"
target_inp = "/net/20/kun/source/inps/"


NO2 = {
'0103' : '   O3VALUES HROFDY 5.34 5.88    6.41    6.95    7.48    7.48    6.95    5.88    6.41    7.48    9.62    11.8    13.4    14.4    14.4    13.9    11.8    8.55    6.41    5.88    5.34    5.34    5.34    5.34',
'0403' : '   O3VALUES HROFDY 15.8    13.9    12.8    11.4    10.6    10.6    10.6    10.6    13.9    18.3    25.3    31.5    36.3    40.3    42.1    42.9    41.4    39.6    34.1    28.2    23.1    19.4    16.5    16.1',
'0703' : '   O3VALUES HROFDY 13.7  11  11.4    10  10.4    8.7 8.03    9.71    14.7    23.4    35.5    47.5    58.9    66.6    70.3    70  69.3    60.6    53.6    46.2    35.5    23.4    18.1    14.4',
'1003' : '   O3VALUES HROFDY 6.23    6.23    6.23    6.23    6.79    6.79    6.23    5.66    6.23    9.06    14.2    20.4    27.2    32.8    35.7    35.7    31.7    24.3    18.1    11.9    9.62    8.49    7.92    6.79'
}


def parse_inp():
    pollutants = ["BC", "CO", "NO2", "PM"]
    dates = ["0103", "0403", "0703", "1003"]
    hours = range(1, 25)
    for p in pollutants:
        for date in dates:
            for hour in hours:
                hour = '0'+str(hour) if hour < 10 else str(hour)
                in_file = "%s%s_%s/aermod_%s.inp" % (source_inp, p, date, hour)
                out_file = "%saermod_%s_%s_%s.inp" % (target_inp, p, date, hour)
                with open(in_file, 'r') as inp:
                    out = open(out_file, 'w')
                    for line in inp:
                        if line.startswith('AVERTIME', 3):
                            out.write("   AVERTIME  1\n")
                        elif p == 'NO2' and line.startswith('OZONEVAL', 3):
                            out.write(NO2[date]+'\n')
                        elif line.startswith('OU PLOTFILE'):
                            pass
                        else:
                            out.write(line)
                    out.close()

def parse_houremic():
    def houremic(pollutant, date):
        in_file = source + pollutant
        out_file = source+ pollutant +"_" + date
        date = date[:2]+" "+date[2:]
        out = open(out_file, 'w')
        with open(in_file, 'r') as file:
            for line in file:
                # print line[:15] + "04 03" + line[20:]
                # exit(0)
                out.write(line[:15] + date + line[20:])
        out.close()

    pollutants = ["BC", "CO", "NOX", "PM"]
    dates = ["0403", "0703", "1003", "0103"]

    for p in pollutants:
        for date in dates:
            # print source+ p +"_" + date
            if date == "0103":
                shutil.move(source+p, source+p+"_" + date)
            else:
                houremic(p, date)

parse_inp()
# parse_houremic()
