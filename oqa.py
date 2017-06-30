#coding: utf-8

import numpy as np

base = '/net/20/kun/source/oqa/'

def parse_oqa(date):
    date = base + date
    with open(date+'.txt') as infile:
        infile.readline()
        n = 0
        sums = np.zeros((24, 5))
        count = np.zeros((24, 5))
        for line in infile:
            if line.isspace():
                continue
            line = line.split()
            DATE = line[2]
            if DATE[-2:] != '00':
                continue
            h = int(DATE[-4:-2])
            DIR = line[3]
            SPD = line[4]
            TEMP = line[21]
            STP = line[25]
            SLP = line[23]
            data = STP, TEMP, DIR, SPD, SLP
            # data = DIR, SPD, TEMP, SLP

            for i in range(5):
                if data[i][0] != '*':
                    sums[h][i] += float(data[i])
                    count[h][i] += 1
            # n += 1
            # if n>3:
            #     break
        with np.errstate(divide='ignore', invalid='ignore'):
            avg = np.true_divide(sums, count)
            avg[avg == np.inf] = 0
            avg = np.nan_to_num(avg)
        # print avg
        avg[:, 0] *= 10
        avg[:, 1] -= 32
        avg[:, 1] *= (10/1.8)
        avg[:, 2] /= 10
        avg[:, 3] *= 4.47
        avg[:, 4] *= 10

        out = open(date+'_oqa.csv', 'w')
        for h in range(24):
            line = avg[h, :]
            out.write(','.join([str(l) for l in line])+'\n')
        out.close()


def parse_deepspace(month):
    '''stropy import error'''
    # from astropy.io import ascii
    # data = ascii.read('deepspace_'+str(month)+'.txt', format='fixed_width')

    '''
    1 列不确定
    参考excel导入文本的固定宽度模式(fixed width columns)
    -----------------------------------------------------------------------------
       PRES   HGHT   TEMP   DWPT   RELH   MIXR   DRCT   SKNT   THTA   THTE   THTV
        hPa     m      C      C      %    g/kg    deg   knot     K      K      K
    -----------------------------------------------------------------------------
     1017.0     55   -3.9  -20.9     25   0.72      0      6  268.0  270.1  268.1
     1016.0     61   -3.9  -20.9     25   0.72      0      6  268.0  270.1  268.1
       42.8  21056  -60.5                         262     83  523.2         523.2

    可以看到数据是右对齐的，可以考虑按照固定宽度分割然后处理

    2 高度不确定
    1000、925、850、700、500、400、300、250、200、150、100、50 这几个标准等压面必须要有数
    所以其他的都可能每天不一样，这几个每天的一定都会有

    第一列PRES、第三列TEMP、第四列DWPT均值完之后乘以十，第二列HGHT第七列DRCT均值完后不动，第八列SKNT均值完后乘以5.144
    相当于和地面不一样的地方在于，今天地面每个小时月均值就是返回一个值，这边最后一天不是要每个小时的，
    只要早七点晚七点，但是比如早点它要返回的是一个列表，列表里不同高度下的各个参数需要是月均值
    '''
    hh = [1000, 925, 850, 700, 500, 400, 300, 250, 200, 150, 100, 50]
    height = {h:i for i,h in enumerate(hh)}
    colums = '   PRES   HGHT   TEMP   DWPT   RELH   MIXR   DRCT   SKNT   THTA   THTE   THTV'
    # right_align = []
    # for i in xrange(1, len(colums)):
    #     if colums[i]==' ' and colums[i-1]!=' ':
    #         right_align.append(i)
    # print right_align
    right_align = [7, 14, 21, 28, 35, 42, 49, 56, 63, 70]
    # PRES(0), HGHT(1), TEMP(2), DWPT(3), DRCT(6), SKNT(7)
    extract = (1, 2, 3, 6, 7)
    def line_to_row(line):
        strs = [line[right_align[k-1]:right_align[k]] for k in extract]
        return map(lambda s: (0, 0) if s.isspace() else (float(s), 1), strs)

    with open(base+'deepspace_'+str(month)+'.txt') as infile:
        lines = infile.readlines()
        n = len(lines)
        i = 0
        d = 0
        datas = np.zeros((2, len(hh), 5, 2))
        while i < n:
            if lines[i].startswith('------'):
                i += 4
                # h = 0
                while not lines[i].startswith('Station'):
                    line = lines[i]
                    PRES = float(line[0:right_align[0]])
                    if PRES in height:
                        # print PRES
                        data = line_to_row(line)
                        h = height[PRES]
                        for j in range(5):
                            datas[d%2][h][j][:] += data[j]
                        # h += 1
                    i += 1
                # if h < len(hh)-1:
                #     print "Error", i
                #     print h
                #     exit(0)
                d += 1
            else:
                i += 1
        # print datas
        avg = datas[:, :, :, 0] / datas[:, :, :, 1]
        avg[:, :, 1] *= 10
        avg[:, :, 2] *= 10
        avg[:, :, 4] *= 5.144
        # print avg

        # with open(base+'2013_'+str(month)+'_ds_count.csv', 'w') as out:
        with open(base+'2013_'+str(month)+'_ds.csv', 'w') as out:
            out.write('PRES, HGHT, TEMP, DWPT, DRCT, SKNT\n')
            timestr = ['00Z\n', '12Z\n']
            for t in range(2):
                out.write(timestr[t])
                for h in xrange(len(height)):
                    line = [hh[h]]+list(avg[t, h, :])
                    # line = [hh[h]]+list(datas[t, h, :, 1])
                    # print line
                    out.write(','.join([str(l) for l in line])+'\n')

def fuck_how_to_end_this_1():
    u'气象/EX01_SF.OQA'
    book = xlrd.open_workbook(u'气象/地面气象汇总.xlsx')
    # print book.sheet_names()
    sample = ' 13010300    -9 99999 10030   300 00000 09999 00300 00300 00300 00300\n         00300 09999 00000 00099 00999 99999   -22   999   999   999    30    36  N'
    for sheet in book.sheets():
        month = sheet.name
        oqa = open('Beijing_SF_13' + month + '.OQA', 'w')
        # print sheet.row(1)
        r = -2
        for row in sheet.get_rows():
            r += 1
            if r<0:
                continue
            line = [' 13%s03%02d' % (month, r)]
            # print row
            for cell in row[1:]:
                if cell.ctype == 1:  # text
                    cell_s = cell.value
                else:               # number
                    cell_s = '%5.0f' % cell.value
                line.append(cell_s)
            line.append(' N')
            # print sample
            # print ' '.join(line[:11]) + '\n         ' + ' '.join(line[11:])
            oqa.write(' '.join(line[:11]) + '\n         ' + ' '.join(line[11:]) + '\n')
        oqa.close()

def fuck_how_to_end_this_2():
    book = xlrd.open_workbook(u'气象/探空插值.xlsx')
    sheet1, sheet2 =  book.sheets()
    sample = ' 13010407   28\n  10134      0   -160   -182    170     10'
    # print sample
    months = [('01','07'), ('01','19'), ('04','07'), ('04','19'), ('10','07'), ('10','19'), ('07','07'), ('07','19')]
    slices = [(0, 6),  (7, 13),  (14, 20),  (21, 27),  (28, 34),  (35, 41), (0, 6),  (7, 13)]
    lines = []
    for month, s in zip(months, slices):
        start_line = 2 if month[0] == '07' else 3
        sheet = sheet2 if month[0] == '07' else sheet1
        for r in range(start_line, sheet.nrows):
            row = sheet.row_slice(r, s[0], s[1])
            line = []
            for cell in row:
                line.append('%7.0f' % cell.value)
            lines.append(''.join(line))
        if month[1] == '19':
            oqa = open('Beijing_UA_13' + month[0] + '.OQA', 'w')
            for i in range(2, 5):
                line_num = len(lines) / 2
                oqa.write(' 13%s%02d%s   %d\n' % (month[0], i, '07', line_num))
                for j in range(line_num):
                    oqa.write(lines[j]+'\n')
                oqa.write(' 13%s%02d%s   %d\n' % (month[0], i, '19', line_num))
                for j in range(line_num, len(lines)):
                    oqa.write(lines[j]+'\n')
            oqa.close()
            lines = []


fuck_how_to_end_this_1()
fuck_how_to_end_this_2()


# dates = ['2013.01', '2013.04', '2013.07', '2013.10']
# for date in dates:
#     print 'parsing ' + date
#     parse_oqa(date)

# months = [1, 4, 7, 10]
# for month in months:
#     print 'parsing ' + str(month)
#     parse_deepspace(month)
