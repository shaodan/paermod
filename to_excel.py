# coding: utf-8

import xlsxwriter
import os.path


pollutants = ["BC", "CO", "PM", "NO"]
dates = ["0103", "0403", "0703", "1003"]
hours = range(1, 25)
output_dir ='../output/'
start_str = '       X-COORD (M)   Y-COORD (M)        CONC                       X-COORD (M)   Y-COORD (M)        CONC'
end_str = ' *** AERMOD - VERSION'

receptor_number = sum(1 for line in open('../source/receptor', 'r'))

pd = {
    "BC": ["0103", "0403", "0703", "1003"],
    "CO": ["0103", "0403", "1003"],
    "PM": ["0103", "0403", "0703", "1003"]
}
pd = {
    "PM": ["0403"]
}

def parse_pd(p, d):
    if os.path.exists('../excel/%s_%s.xlsx' % (p, d)):
        print '%s_%s.xlsx already exist' % (p, d)
        return
    day_data = [None] * 24
    for hour in hours:
        h = '0'+str(hour) if hour < 10 else str(hour)
        task = "%s_%s_%s" % (p, d, h)
        if not os.path.exists(output_dir+task+'.out'):
            print 'outfile not found for task: ' + task
            return 
    for hour in hours:
        h = '0'+str(hour) if hour < 10 else str(hour)
        task = "%s_%s_%s" % (p, d, h)

        hour_data = [0] * receptor_number
        day_data[hour-1] = hour_data
        receptor_i = 0
        print 'parsing '+task +'...'
        with open(output_dir+task+'.out', 'r') as outfile:
            lines = outfile.readlines()
            length = len(lines)
            i = 0
            while i < length:
                if lines[i].startswith(start_str):
                    i+=2
                    while not lines[i].startswith(end_str):
                        data = lines[i].split()
                        hour_data[receptor_i] = float(data[2])
                        receptor_i += 1
                        if len(data)==6:
                            hour_data[receptor_i] = float(data[5])
                            receptor_i += 1
                        i+=1
                i+=1

    print "save to %s_%s.xlsx" % (p, d)
    workbook = xlsxwriter.Workbook('../excel/%s_%s.xlsx' % (p, d))
    worksheet = workbook.add_worksheet()
    worksheet.write(0, 0, 0)
    for col in hours:
        worksheet.write(0, col, 0)
    for row in range(1, receptor_number+1):
        worksheet.write(row, 0, row)
        for col in hours:
            worksheet.write(row, col, day_data[col-1][row-1])
    workbook.close()

#for p, dates in pd.items():
#    for d in dates:
for p in pollutants[:-1]:
    for d in dates:
        parse_pd(p, d)


