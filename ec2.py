import csv
import yaml
from dateutil.relativedelta import *
from datetime import datetime, date,timedelta

import authorize
import gspread
from apiclient import discovery
from httplib2 import Http

import io
##

# copy the initial data series from input to output file
def copy_input_data_to_ouput_file(InputDataFile, OutputFile):

    output = open(OutputFile, 'w')
   # clean output file
    output.truncate()
    writer = csv.writer(output)
    # write first line with column names in csv file
    writer.writerow(['input','date', 'value','source','file_id'])
    inputs = csv.reader(open(InputDataFile, 'rb'))

    for input, datestr, price in inputs:
        writer.writerow([input,datestr, price,'seriesForecast','None'])

def copy_column_from_excel_to_list(sheet,column):
    result = []
    i = 1
    index = column + '1'
    cellInput = sheet.acell(index).value
    while cellInput != '':
        result.append(cellInput)
        i = i + 1
        index = column + str(i)
        cellInput = sheet.acell(index).value


    return result

def get_metadata_from_excel_file_gspread(wb):
    data = dict()
    sheetModelName = wb.worksheet("Name")
    modelName = sheetModelName.acell('A1').value
    sheetOutput = wb.worksheet("Output")
    sheetInput = wb.worksheet("Input")
    data['name'] = modelName
    data['input'] = copy_column_from_excel_to_list(sheetInput,'A')
    data['output'] = copy_column_from_excel_to_list(sheetOutput,'A')
    # check next line
    data['calculator'] = modelName
    data['model_id'] =wb.id
    return data



def keyRow(key,sheet):
    row=1
    string = 'A' +str(row)
    name =  sheet.acell(string).value
    while name!= key:
        row +=1
        string = 'A' + str(row)
        name = sheet.acell(string).value
    return row

def get_output(wb):

    sheetOutput = wb.worksheet('Output')
    outputs = dict()
    row = 1
    cellOutput = sheetOutput.acell('A1').value
    while cellOutput !='' :
      string = 'B'+str(row)
      outputs[cellOutput] = sheetOutput.acell(string).value
      row +=1
      string2='A'+str(row)
      cellOutput = sheetOutput.acell(string2).value
    return (outputs)

def recordInputData(inputDictionary,wb):
    sheetInput = wb.worksheet('Input')
    for key, value in inputDictionary.items():
        n = keyRow(key, sheetInput)
        label = 'B' + str(n)
        sheetInput.update_acell(label,  value)


# write output data to output.csv
def publishForecast(date,inputName,forecast,modelName,source,outputStream):
    datestr = date.strftime('%m/%d/%Y')
    data = [inputName,datestr,forecast,modelName,source]
    writer = csv.writer(outputStream)
    writer.writerow(data)


# convert csv file to Python dictionary
def readFromCsv(outputFile):
    output = open(outputFile, 'r')
    reader = csv.reader(output)
    outputDict = dict()
    next(reader)
    for input,datestr,price,source,file_id in reader:
        date = datetime.strptime(datestr , '%m/%d/%Y').date()
        key = (input,date)
        outputDict[key] = price
    return outputDict



# we get files in the folder. Initially we get folder ID. Then we get all files from the folder
def get_files_in_folder(service,folder):
    qString = " mimeType ='application/vnd.google-apps.folder' and name = '" + folder + "'"
    response = service.files().list(
        q=qString,
        spaces='drive',
        fields='nextPageToken, files(id, name)'
    ).execute()

    folder_id = response['files'][0]['id']
    qString2 = "'" + str(folder_id) + "'" + " in parents"
    children = DRIVE.files().list(
        q=qString2,
        fields='nextPageToken, files(id, name)'
    ).execute()

    file_list = children['files']
    return file_list

#register models
def register_models(files,meta_model_file,gc):
  allData= []
  for file in files:
        name = file['name']
        wb = gc.open(name)
        data = get_metadata_from_excel_file_gspread(wb)
        allData.append(data)

  stream = open(meta_model_file, 'a')
  yaml.dump(allData, stream)

# main cylce..Calculate data for all dates for all models
def calculate_forecast(firstDate,numberOfDays,metaModelsFile,outputFile,gs):
    # get all metadata
    stream = io.open(metaModelsFile,'r')
    models = list(yaml.load_all(stream))[0]
    ## we get the initial data series forecast from output file
    outputData = readFromCsv(outputFile)

    # we loop through several days -> daterange
    daterange =[(firstDate + relativedelta(days = x)) for x in range(0,numberOfDays)]

    for date in daterange:
      for m in  models:
          # get some metadata
          modelName = m['name']
          inputList = m['input']
          calculatorFileId = m['model_id']
          # we prepare all the input data for the model in the inputValuesDictionary
          inputValuesDictionary = dict()
          for input in inputList:
              prevDate = date - timedelta(1)
              value = outputData[(input, prevDate)]
              inputValuesDictionary[input] = value
          # we record input data into the Google Drive model file
          wb = gs.open_by_key(calculatorFileId)
          recordInputData(inputValuesDictionary,wb)
          # get dictionary of outputs
          forecast = get_output(wb)
          stream2 = open(outputFile, 'a')
          for key,value in forecast.items():
            publishForecast(date, key, value,modelName, calculatorFileId,stream2)
            outputData[(key,date)]  = value


### main procedure

# copy input data to output data
input_file = '/Users/macbook/PycharmProjects/test/input.csv'
output_file = '/Users/macbook/PycharmProjects/test/output.csv'
copy_input_data_to_ouput_file(input_file,output_file)
# clean yaml model local file
output = open('/Users/macbook/PycharmProjects/test/MetaModel.yaml', 'w')
output.truncate()

# authorization Google Drive
# we use our own authorize module

credentials = authorize.get_credentials()

# create Google Drive Service

DRIVE = discovery.build('drive', 'v3', http = credentials.authorize(Http()))

# authorithation gspread
gc = gspread.authorize(credentials)

# get Model lists. They are located in Google Drive folder 'models'
file_list = get_files_in_folder(DRIVE,'models')

#register models
register_models(file_list ,'/Users/macbook/PycharmProjects/test/MetaModel.yaml',gc)

startDay= date(2018, 1, 2)

# main cylce. Calculate all results and record them to the output.csv
calculate_forecast(startDay,3,'/Users/macbook/PycharmProjects/test/MetaModel.yaml','/Users/macbook/PycharmProjects/test/output.csv',gc)







