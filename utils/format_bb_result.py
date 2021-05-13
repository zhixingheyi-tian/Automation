import sys
import os
import xlwt
import xlrd


def readfile(filepath,savepath,times):

    print('reading file:'+filepath)
    f = open(filepath,"r")
    filename = f.name
    lines = f.readlines()
    f.close()

    #create a workbook
    workbook = xlwt.Workbook()
    datasheet = workbook.add_sheet("data")
    analyzesheet = workbook.add_sheet("analyze")
    # add a sheet for comparison
    comparesheet = workbook.add_sheet("compare")

    pattern = xlwt.Pattern()
    pattern.pattern = xlwt.Pattern.SOLID_PATTERN
    pattern.pattern_back_colour = 2
    pattern.pattern_fore_colour = 2
    redstype = xlwt.XFStyle()
    redstype.pattern = pattern;
    row = 0
    column = 0
    start_date = [0]*lines.__len__()
    bench_phase = [0]*lines.__len__()
    query_number = [0]*lines.__len__()
    duration = [0]*lines.__len__()
    successful = [0]*lines.__len__()
    for line in lines:
        column = 0
        startpos = 0
        a = line.find('|',startpos+1)
        while(a>0):
            if (startpos==0):
                startpos=-1
            value = line[startpos+1:a]
            datasheet.write(row,column,value)

            #start_date
            if (column == 3):
                start_date[row] = value
                analyzesheet.write(row, 0, value)
            #benchmark_pahse
            if (column == 8):
                bench_phase[row] = value
                analyzesheet.write(row, 1, value)
            #query_number
            if (column == 6):
                query_number[row] = value
                analyzesheet.write(row, 2, value)
            #duration
            if (column == 2):
                if(row != 0):
                    value = round(float(value)/60000,3)

                duration[row] = value
                analyzesheet.write(row, 3, value)
            #SUCCESSFUL
            if (column == 10):
                successful[row] = value
                if(value=='FAILED'):
                    analyzesheet.write(row, 4, value,redstype)
                else:
                    analyzesheet.write(row, 4, value)
            startpos = a
            a = line.find('|', startpos + 1)
            column=column+1

        if(row!=0):
            analyzesheet.write(row, (times + 1) * 6 + 0, xlwt.Formula("G"+str(row+1)))
            analyzesheet.write(row, (times + 1) * 6 + 1, xlwt.Formula("H"+str(row+1)))
            analyzesheet.write(row, (times + 1) * 6 + 2, xlwt.Formula("I"+str(row+1)))

            #GET AVERAGE for serveral times
            midvalueFormula = "AVERAGE("
            for i in range(1,times+1,1):
                midvalueFormula += numToExcelChar(i*6+3)+str(row+1)
                if(i!=times):
                    midvalueFormula+=","
                else:
                    midvalueFormula+=")"
            analyzesheet.write(row, (times + 1) * 6 + 3, xlwt.Formula(midvalueFormula))
        row = row+1

    #add title
    for i in range(1, times + 2, 1):
        analyzesheet.write(0, 6 * i + 0, 'STARTDATE')
        analyzesheet.write(0, 6 * i + 1, 'BENCHMARK_PHASE')
        analyzesheet.write(0, 6 * i + 2, 'QUERY')
        analyzesheet.write(0, 6 * i + 3, 'DURATION')
        analyzesheet.write(0, 6 * i + 4, 'SUCCESSFUL')

    #add Formula of compare
    comparesheet.write(0,0,filename)
    comparesheet.write(0,1,'A')
    comparesheet.write(0,5,'B')
    comparesheet.write(1, 0, 'BENCHMARK_PHASE')
    comparesheet.write(1, 1, 'QUERY')
    comparesheet.write(1, 2, 'DURATION')
    comparesheet.write(1, 4, 'BENCHMARK_PHASE')
    comparesheet.write(1, 5, 'QUERY')
    comparesheet.write(1, 6, 'DURATION')
    comparesheet.write(1, 8, 'BENCHMARK_PHASE')
    comparesheet.write(1, 9, 'QUERY')
    comparesheet.write(1, 10, '(B-A)/B*100%')

    for i in range(2,row+2,1):
        comparesheet.write(i, 8, xlwt.Formula("A"+str(i+1)))
        comparesheet.write(i, 9, xlwt.Formula("B"+str(i+1)))
        #caculate the percentages of comparison
        comparesheet.write(i, 10, xlwt.Formula("100*(G"+str(i+1)+"-C"+str(i+1)+")/C"+str(i+1)))


    workbook.save(savepath);
    return (row,column)

def inputValidate(args):
    if (args.__len__() != 4):
	usage()
        exit(1)
    if (not os.path.exists(args[1])):
        print 'No such file: ' + args[1]
        exit(1)
    if (os.path.exists(args[2])):
        print 'the result file existed,please choose another path: ' + args[2]
        exit(1)

    if (int(args[3]) < 1 or int(args[3])>8):
        print 'times should in the range of 2-8 ,your value is :' + args[3]
        exit(1)

def numToExcelChar(number):
    if (number>72):
        print 'number shoule be less than 72'
        exit(1)
    firstNum = number/26
    secondNum = number%26
    if(number>25):
        reschar = chr(firstNum+64)+chr(secondNum+65)
    else:
        reschar = chr(secondNum+65)
    return reschar

def usage():
    print("Usage: utils/result_format.py [inputpath] [outputpath] [times]")
    print("   inputpath: inputpath should be the path of your times.csv ")
    print("   outputpath: outputpath should named as an excel file. For example:/tmp/result.xls ")
    print("   times: means the times that you have run in the inputpath ")

    exit(1)

if __name__ == '__main__':

    args = sys.argv;

    inputValidate(args)

    sourcepath = args[1]
    resultpath = args[2]
    times = int(args[3])

    rowcols = readfile(sourcepath,resultpath,times)






