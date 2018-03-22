import csv
import yaml
from dateutil.relativedelta import *
from datetime import datetime, date, timedelta

import authorize
import gspread
from apiclient import discovery
from httplib2 import Http

import io


##

# copy the initial data series from input to output file
def copy_input_data_to_output_file(input_data_file, output_file2):
    output2 = open(output_file2, 'w')
    # clean output file
    output2.truncate()
    writer = csv.writer(output2)
    # write first line with column names in csv file
    writer.writerow(['input', 'date', 'value', 'source', 'file_id'])
    inputs = csv.reader(open(input_data_file, 'rb'))

    for series, datestr, price in inputs:
        writer.writerow([series, datestr, price, 'seriesForecast', 'None'])


def copy_column_from_excel_to_list(sheet, column):
    result = []
    i = 1
    index = column + '1'
    cell_input = sheet.acell(index).value
    while cell_input != '':
        result.append(cell_input)
        i = i + 1
        index = column + str(i)
        cell_input = sheet.acell(index).value

    return result


def get_metadata_from_excel_file_gspread(wb):
    data = dict()
    sheet_model_name = wb.worksheet("Name")
    model_name = sheet_model_name.acell('A1').value
    sheet_output = wb.worksheet("Output")
    sheet_input = wb.worksheet("Input")
    data['name'] = model_name
    data['input'] = copy_column_from_excel_to_list(sheet_input, 'A')
    data['output'] = copy_column_from_excel_to_list(sheet_output, 'A')
    # check next line
    data['calculator'] = model_name
    data['model_id'] = wb.id
    return data


def key_row(key, sheet):
    row = 1
    string = 'A' + str(row)
    name = sheet.acell(string).value
    while name != key:
        row += 1
        string = 'A' + str(row)
        name = sheet.acell(string).value
    return row


def get_output(wb):
    sheet_output = wb.worksheet('Output')
    outputs = dict()
    row = 1
    cell_output = sheet_output.acell('A1').value
    while cell_output != '':
        string = 'B' + str(row)
        outputs[cell_output] = sheet_output.acell(string).value
        row += 1
        string2 = 'A' + str(row)
        cell_output = sheet_output.acell(string2).value
    return outputs


def record_input_data(input_dictionary, wb):
    sheet_input = wb.worksheet('Input')
    for key, value in input_dictionary.items():
        n = key_row(key, sheet_input)
        label = 'B' + str(n)
        sheet_input.update_acell(label, value)


# write output data to output.csv
def publish_forecast(date2, input_name, forecast, model_name, source, output_stream):
    datestr = date2.strftime('%m/%d/%Y')
    data = [input_name, datestr, forecast, model_name, source]
    writer = csv.writer(output_stream)
    writer.writerow(data)


# convert csv file to Python dictionary
def read_from_csv(output_file2):
    output2 = open(output_file2, 'r')
    reader = csv.reader(output2)
    output_dict = dict()
    next(reader)
    for series, datestr, price, source, file_id in reader:
        date2 = datetime.strptime(datestr, '%m/%d/%Y').date()
        key = (series, date2)
        output_dict[key] = price
    return output_dict


# we get files in the folder. Initially we get folder ID. Then we get all files from the folder
def get_files_in_folder(service, folder):
    q_string = " mimeType ='application/vnd.google-apps.folder' and name = '" + folder + "'"
    response = service.files().list(
        q=q_string,
        spaces='drive',
        fields='nextPageToken, files(id, name)'
    ).execute()

    folder_id = response['files'][0]['id']
    q_string2 = "'" + str(folder_id) + "'" + " in parents"
    children = DRIVE.files().list(
        q=q_string2,
        fields='nextPageToken, files(id, name)'
    ).execute()

    file_list = children['files']
    return file_list


# register models
def register_models(files, meta_model_file, gc2):
    all_data = []
    for model in files:
        name = model['name']
        wb = gc2.open(name)
        data = get_metadata_from_excel_file_gspread(wb)
        all_data.append(data)

    stream = open(meta_model_file, 'a')
    yaml.dump(all_data, stream)


# main cylce..Calculate data for all dates for all models
def calculate_forecast(first_date, number_of_days, meta_models_file, output_file2, gs):
    # get all metadata
    stream = io.open(meta_models_file, 'r')
    ttt = yaml.load_all(stream)
    aaa = list(ttt)
    models = aaa[0]
    # we get the initial data series forecast from output file
    output_data = read_from_csv(output_file)

    # we loop through several days -> daterange
    daterange = [(first_date + relativedelta(days=x)) for x in range(0, number_of_days)]

    for date2 in daterange:
        for m in models:
            # get some metadata
            model_name = m['name']
            input_list = m['input']
            calculator_file_id = m['model_id']
            # we prepare all the input data for the model in the inputValuesDictionary
            input_values_dictionary = dict()
            for series in input_list:
                prev_date = date2 - timedelta(1)
                value = output_data[(series, prev_date)]
                input_values_dictionary[series] = value
            # we record input data into the Google Drive model file
            wb = gs.open_by_key(calculator_file_id)
            record_input_data(input_values_dictionary, wb)
            # get dictionary of outputs
            forecast = get_output(wb)
            stream2 = open(output_file2, 'a')
            for key, value in forecast.items():
                publish_forecast(date2, key, value, model_name, calculator_file_id, stream2)
                output_data[(key, date2)] = value


# main procedure

# copy input data to output data
input_file = '/Users/macbook/PycharmProjects/test/input.csv'
output_file = '/Users/macbook/PycharmProjects/test/output.csv'
copy_input_data_to_output_file(input_file, output_file)
# clean yaml model local file
meta_model_file = '/Users/macbook/PycharmProjects/test/MetaModel.yaml'
output = open(meta_model_file, 'w')
output.truncate()

# authorization Google Drive
# we use our own authorize module

credentials = authorize.get_credentials()

# create Google Drive Service

DRIVE = discovery.build('drive', 'v3', http=credentials.authorize(Http()))

# authorization gspread
gc = gspread.authorize(credentials)

# get Model lists. They are located in Google Drive folder 'models'
file_list = get_files_in_folder(DRIVE, 'models_common')

# register models

register_models(file_list, meta_model_file, gc)

startDay = date(2018, 1, 2)

# main cylce. Calculate all results and record them to the output.csv
calculate_forecast(startDay, 3, meta_model_file, output_file, gc)
