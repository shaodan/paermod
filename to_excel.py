# coding: utf-8

import xlsxwriter
import os.path


pollutants = ["BC", "CO", "PM", "NO"]
dates = ["0103", "0403", "0703", "1003"]
situations = ['wkd', 'wke', 'jam', 'apec']
hours = range(1, 25)
output_dir = '../output/'
export_dir = '../excel/'
start_str = '       X-COORD (M)   Y-COORD (M)        CONC                       X-COORD (M)   Y-COORD (M)        CONC'
end_str = ' *** AERMOD - VERSION'

receptor_number = 45792 #sum(1 for line in open('../source/receptor', 'r'))
skip_receptors = [(1,51),(213,257),(425,447),(637,641),(849,850),(1061,1061)]
sss = []
for s in skip_receptors:
    sss += range(s[0], s[1]+1)
skip_receptors = set(sss)
report_number = receptor_number-len(skip_receptors)
print 'Total %d Receptors, Skip %d of them' % (receptor_number, len(skip_receptors))

pd = {
    "BC": ["0103", "0403", "0703", "1003"],
    "CO": ["0103", "0403", "0703", "1003"],
    "PM": ["0103", "0403", "0703", "1003"]
}
pd2 = {
    "PM": ["0403"]
}


def parse_psd(p, s, d):
    psd = '%s_%s_%s.xlsx' % (p, s, d)
    if os.path.exists(export_dir + psd):
        print psd + ' already exist'
        # return
    day_data = [None] * 24
    # for hour in hours:
    #     h = '0'+str(hour) if hour < 10 else str(hour)
    #     task = "%s_%s_%s_%s" % (p, s, d, h)
    #     if not os.path.exists(output_dir+task+'.out'):
    #         print 'outfile not found for task: ' + task
    #         return 
    for hour in hours:
        h = '0'+str(hour) if hour < 10 else str(hour)
        task = "%s_%s_%s_%s" % (p, s, d, h)

        hour_data = [0] * receptor_number
        day_data[hour-1] = hour_data
        
        if not os.path.exists(output_dir+task+'.out'):
            continue
        
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
    
    receptor_data = [None] * report_number
    receptor_i = 0
    for i in range(receptor_number):
        if (i+1) in skip_receptors:
            continue
        receptor_data[receptor_i] = [0]*25
        receptor_data[receptor_i][0] = i+1 #receptor_i + 1
        for h in xrange(1, 25):
            receptor_data[receptor_i][h] = day_data[h-1][i]
        receptor_i += 1
        
    # print receptor_i, report_number
    # return
        
    print "save to " + psd
    workbook = xlsxwriter.Workbook(export_dir + psd)
    worksheet = workbook.add_worksheet()
    worksheet.write_row(0, 0, [0]*25)
    for row in range(1, report_number+1):
        worksheet.write_row(row, 0, receptor_data[row-1])
        # worksheet.write(row, 0, row)
        # for col in hours:
        #     worksheet.write(row, col, day_data[col-1][row-1])
    workbook.close()



parse_psd('PM', 'wkd', '0103')
# parse_psd('PM', 'wkd', '0403')
# parse_psd('PM', 'wkd', '0703')
# parse_psd('PM', 'wkd', '1003')



