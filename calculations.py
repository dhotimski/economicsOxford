import yaml
from datetime import  date, timedelta
import pandas as pd
import help_functions
import authorize
import gspread
from apiclient import discovery
import io
import httplib2


def calculate_forecast(serviceSheets, serviceDrive, first_date, number_of_days, meta_models_file, gs):
    # end_date = first_date + timedelta(number_of_days)
    date_range = pd.date_range(first_date, periods=number_of_days)
    df = pd.DataFrame(index=date_range)
    put_inputs_into_dataframe(serviceSheets, serviceDrive, df, date_range, number_of_days)

    print(df)
    print('----------- after all inputs in --------------------------------------')
    dates_all = date_range[1:]
    #   model_files_ids = help_functions.get_files_ids_in_folder(serviceDrive, 'models_exe')
    model_files = help_functions.get_files_in_folder(serviceDrive, 'models_exe')
    # models = map(lambda x: gs.open_by_key(x) , model_files_ids)
    stream = io.open(meta_models_file, 'r')
    all_meta1 = list(yaml.load_all(stream))
    all_meta = all_meta1[0]
    models = map(
        lambda x: (x, gs.open_by_key(x['id']), model_inputs(x['name'], all_meta), model_outputs(x['name'], all_meta)),
        model_files)
    for date_s in dates_all:
        for model in models:
            insert_all_inputs_get_all_output2(serviceSheets, model, date_s, df)

    return df


def model_inputs(model_name, meta_data):
    model_meta = filter(lambda x: x['name'] == model_name, meta_data)
    input_list = model_meta[0]['input']
    return input_list


def model_outputs(model_name, meta_data):
    model_meta = filter(lambda x: x['name'] == model_name, meta_data)
    output_list = model_meta[0]['output']
    return output_list


def put_inputs_into_dataframe(serviceSheets, service_drive, df, date_range, number_of_days):
    input_files = help_functions.get_files_in_folder(service_drive, 'input_exe')
    range_ = 'Data!B1:B' + str(len(date_range))
    for file_ in input_files:
        result = serviceSheets.spreadsheets().values().get(
            spreadsheetId=file_['id'], range=range_).execute()
        series_value = result['values']
        #  df[first_day,file_['name']] = series_value
        for i in range(number_of_days):
            series_name = file_['name'][:-4]
            df.set_value(date_range[i], series_name, float(series_value[i][0]))


def insert_all_inputs_get_all_output2(serviceSheets, model, date_s, df):
    wb = model[1]
    spreadsheet_id = model[0]['id']
    inputs = model[2]
    outputs = model[3]
    total_outputs = len(outputs)

    for item in inputs:
        order = item['order']
        series_name = item['current_name']
        prev_date = date_s - timedelta(1)
        value_ = (df.at[prev_date, series_name])
        range_name = 'Input!E' + str(3 + order) + ':E' + str(3 + order)
        values = [
            [value_]
        ]
        body = {
            'values': values
        }
        value_input_option = 'RAW'
        result = serviceSheets.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, valueInputOption=value_input_option, range=range_name,
            body=body).execute()

    sheet_out = wb.worksheet('Output')

    output_values = sheet_out.range(2, 2, 2 - 1 + total_outputs, 2)

    for i in range(total_outputs):
        output_item = filter(lambda x: x['order'] == i, outputs)
        output_name = output_item[0]['current_name']
        out_value = float(output_values[i].value)
        df.set_value(date_s, output_name, out_value)
        print(df)
        print('--------------' + str(date_s) + '____' + output_name + '___' + str(out_value) + '____')


# insert all inputs
# read all outputs


def main():
    # we leave it just for testing
    config_file = '/Users/macbook/PycharmProjects/test/config_exe.yaml'
    output_file = '/Users/macbook/PycharmProjects/test/output.csv'
    meta_model_common = '/Users/macbook/PycharmProjects/test/meta_model_common.yaml'
    meta_model_user = '/Users/macbook/PycharmProjects/test/meta_model_user.yaml'
    meta_model_exe = '/Users/macbook/PycharmProjects/test/meta_model_exe.yaml'
    meta_inputs_exe = '/Users/macbook/PycharmProjects/test/meta_inputs_exe.yaml'
    common_input_folder = 'input_common'
    user_input_folder = 'input_user'
    simulation_input_folder = 'input_simulation'

    # clean_file(meta_model_common)
    # clean_file(meta_model_user)
    # clean_file(meta_model_exe)
    # clean_file(meta_inputs_exe)

    stream = io.open(config_file, 'r')
    config_exe_list = list(yaml.load_all(stream))[0]

    # authorization Google Drive
    # we use our own authorize module
    credentials = authorize.get_credentials()
    http = credentials.authorize(httplib2.Http())
    # create Google Drive Service
    # DRIVE = discovery.build('drive', 'v3', http=credentials.authorize(Http()))
    DRIVE = discovery.build('drive', 'v3', http=http)
    # create Google Sheets Service

    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    serviceSheets = discovery.build('sheets', 'v4', http=http,
                                    discoveryServiceUrl=discoveryUrl)
    # authorization gspread
    gs = gspread.authorize(credentials)
    df = calculate_forecast(serviceSheets, DRIVE, date(2018, 1, 1), 3, meta_model_exe, gs)
    print(df)


if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    main()
