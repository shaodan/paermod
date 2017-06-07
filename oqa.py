import numpy as np

def parse_oqa(date):
    with open(date+'.txt') as infile:
        infile.readline()
        n = 0
        sums = np.zeros((24, 4))
        count = np.zeros((24, 4))
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
            data = STP, TEMP, DIR, SPD
            # data = DIR, SPD, TEMP, STP
            for i in range(4):
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
        print avg
        avg[:, 0] *= 10
        avg[:, 1] -= 32
        avg[:, 1] *= (10/1.8)
        avg[:, 2] /= 10
        avg[:, 3] *= 4.47

        out = open(date+'_oqa.csv', 'w')
        for h in range(24):
            line = avg[h, :]
            out.write(','.join([str(l) for l in line])+'\n')
        out.close()


# parse_oqa('2013.01')
parse_oqa('2013.04')
parse_oqa('2013.07')
parse_oqa('2013.10')
