# -*- coding: utf-8 -*-

import os
import shutil
import numpy as np


source_pollutant = "/net/20/kun/source/HOUREMIS/HOUREMIS_"
source_inp = "/net/20/kun/source/inps_old/Linerun_"
target_inp = "/net/20/kun/source/inps"


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
                out_file = "%s/%s_%s_%s.inp" % (target_inp, p, date, hour)
                with open(in_file, 'r') as inp:
                    out = open(out_file, 'w')
                    for line in inp:
                        if line.startswith('SO HOUREMIS'):
                            pass
                        elif line.startswith('AVERTIME', 3):
                            out.write("   AVERTIME  1\n")
                        elif p == 'NO2' and line.startswith('OZONEVAL', 3):
                            out.write(NO2[date]+'\n')
                        elif line.startswith('OU PLOTFILE'):
                            pass
                        else:
                            out.write(line)
                    out.close()

def parse_houremis():
    def houremis(pollutant, date):
        in_file = source_pollutant + pollutant
        out_file = source_pollutant+ pollutant +"_" + date
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
            # print source_pollutant+ p +"_" + date
            if date == "0103":
                shutil.move(source_pollutant+p, source_pollutant+p+"_" + date)
            else:
                houremis(p, date)


def parse_source_from_excel():
    pollutants = ['BC', 'PM'] #+  ['CO', 'NO']
    src_file = '/net/20/kun/source/PM.csv'
    template = '/net/20/kun/source/source'
    STK_nums = 169908
    params_weekday = np.zeros((STK_nums, 24))
    params_weekend = np.zeros((STK_nums, 24))
    params_jam = np.zeros((STK_nums, 24))
    params_apec = np.zeros((STK_nums, 24))


    with open(template) as a:
        source_lines = a.readlines()

    def parse_source_p(p):
        print 'parsing ' + p + ' ...'
        with open('/net/20/kun/source/'+p+'.csv') as infile:
            i = 0
            infile.readline()
            infile.readline()
            for line in infile:
                line = line.split(',')
                jam = line[7:31]
                weekday = line[31:55]
                weekend = line[55:79]
                apec = line[79:103]
                params_weekday[i, :] = weekday
                params_weekend[i, :] = weekend
                params_jam[i, :] = jam
                params_apec[i, :] = apec
                i += 1
                # if i > 3:
                #     break
        # print "%.4E" % params_weekday[1,0]
        # exit()
        for situation, params in [('wkd', params_weekday), ('wke', params_weekend), ('jam', params_jam), ('apec', params_apec)]:
            for h in range(1, 25):
                hh = '0'+str(h) if h < 10 else str(h)
                with open('/net/20/kun/source/sources/'+situation+'/source_'+p+'_'+hh, 'w') as o:
                # with open('sources/source_'+p+'_'+hh, 'w') as o:
                    i = 0
                    for line in source_lines:
                        if i%2==0:
                            o.write(line)
                        else:
                            ol = line.split()
                            ol[3] = "%.4E" % params[i/2, h-1]
                            o.write(' '.join(ol)+'\n')
                        i+= 1

    for p in pollutants:
        parse_source_p(p)

def parse_source():
    '''
    污染物为PM时，需要把HOUREMIS里面SRC参数写入对应hour的source文件
    '''
    src_file = '/net/20/kun/source/source'
    hem_file = '/net/20/kun/source/HOUREMIS/HOUREMIS_'
    pollutants = ['BC', 'CO', 'NOX', 'PM']
    STK_nums = 169908
    params = [0] * STK_nums * 24

    with open(src_file) as a:
        source_lines = a.readlines()

    for p in pollutants:
        hem = open(hem_file + p + '_0103')
        i = 0
        for line in hem:
            params[i] = line.split()[7]
            i += 1
        i = 0
        j = 0
        for h in range(1, 25):
            hh = '0'+str(h) if h < 10 else str(h)
            with open('/net/20/kun/source/sources/source_'+p+'_'+hh, 'w') as o:
                i = 0
                for line in source_lines:
                    if i%2==0:
                        o.write(line)
                    else:
                        ol = line.split()
                        ol[3] = params[j]
                        o.write(' '.join(ol)+'\n')
                        j += 1
                    i+= 1


#parse_inp()
# parse_houremis()
# parse_source()
parse_source_from_excel()
