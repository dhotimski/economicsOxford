import authorize
import help_functions
import calculations
import gspread
from apiclient import discovery
import io
import yaml
import httplib2
import meta_file_creation

from datetime import date


def change_input_value_for_several_days(service, changes_list, gs):
    commands_change_input = find_change_input_for_several_days_commands(changes_list)
    for item in commands_change_input:
        name = item['input'] + '_exe'
        input_file_id = help_functions.find_file_id(service, name, 'input_exe')
        wb = gs.open_by_key(input_file_id)
        sheet = wb.worksheet('Data')
        start_cell = sheet.find(item['first_day'])
        r1 = start_cell.row
        shift = int(item['number_of_days'])
        r2 = start_cell.row + shift - 1
        range_str = "B" + str(r1) + ":B" + str(r2)
        cell_list = sheet.range(range_str)
        for cell in cell_list:
            cell.value = item['new_value']
        # Update in batch
        sheet.update_cells(cell_list)


# - command: change_input_value_several_days_add_delta
# input: gasoline
# initial_day: 01 / 01 / 2018
# number_of_days: 2
# delta: 5
# corrections_input_value_several_days_add_delta()
def change_input_value_for_several_days_add_delta(service, changes_list, gs):
    commands_change_input = find_change_input_for_several_days_add_delta_commands(changes_list)
    for item in commands_change_input:
        name = item['input'] + '_exe'
        input_file_id = help_functions.find_file_id(service, name, 'input_exe')
        wb = gs.open_by_key(input_file_id)
        sheet = wb.worksheet('Data')
        start_cell = sheet.find(item['first_day'])
        r1 = start_cell.row
        shift = int(item['number_of_days'])
        r2 = start_cell.row + shift - 1
        range_str = "B" + str(r1) + ":B" + str(r2)

        cell_list = sheet.range(range_str)
        increment = item['delta']
        for cell in cell_list:
            a = float(cell.value)
            cell.value = a + increment
        # Update in batch
        sheet.update_cells(cell_list)


def find_change_input_for_several_days_commands(changes_list):
    a = filter(lambda command: command['command'] == 'change_input_value_several_days', changes_list)
    return a


def find_change_input_for_several_days_add_delta_commands(changes_list):
    a = filter(lambda command: command['command'] == 'change_input_value_several_days_add_delta', changes_list)
    return a


def get_start_day(config_exe_list):
    a = filter(lambda command: command['command'] == 'start_day', config_exe_list)
    b = a[0]['start_day']
    return b


def get_days_of_forecast(config_exe_list):
    a = filter(lambda command: command['command'] == 'number_of_days', config_exe_list)
    b = a[0]['number_of_days']
    return b


def copy_necessary_input_to_exe_environment(drive_service,files_list):
    destination_folder_name = 'input_exe'
    destination_folder_id = help_functions.get_folder_id(drive_service, destination_folder_name)
    for item in files_list:
        new_name = item['input_name'] + '_exe'
        help_functions.copy_file(drive_service, item['file_id'], new_name, destination_folder_id)


def clean_file(file_name):
    mm = open(file_name, 'w')
    mm.truncate()


def add_current_input_columns(model, sheet, column):
    index = column + str(1)
    sheet.update_acell(index, 'Current input name')
    input_data = model['input']
    for item in input_data:
        cell_f = sheet.find(item['initial_name'])
        row = cell_f.row
        index = column + str(row)
        sheet.update_acell(index, item['current_name'])


def copy_models_with_added_current_input_to_exe_folder(drive_service, meta_model_exe, common_model_folder, user_model_folder,
                                                       exe_model_folder, gs):
    stream = io.open(meta_model_exe, 'r')
    all_meta = list(yaml.load_all(stream))[0]
    for model in all_meta:
        file_name = model['name']
        folder_name = common_model_folder if model['source'] == 'common' else user_model_folder
        file_id = help_functions.find_file_id(drive_service, file_name, folder_name)
        exe_model_folder_id = help_functions.get_folder_id(drive_service, exe_model_folder)
        new_file = help_functions.copy_file(drive_service, file_id, file_name, exe_model_folder_id)
        new_file_id = new_file['id']
        wb = gs.open_by_key(new_file_id)
        input_sheet = wb.worksheet('Input')
        add_current_input_columns(model, input_sheet, 'F')


def prepare_input_files_id_name_list(drive_service, meta_model_exe, common_input_folder, user_input_folder, simulation_input_folder):
    input_files_id_name_list = []
    stream = io.open(meta_model_exe, 'r')
    all_meta = list(yaml.load_all(stream))[0]
    seen = set()
    for model in all_meta:
        input_list = model['input']
        for item in input_list:
            folder_name = common_input_folder if item['source'] == 'common' else (
                simulation_input_folder if item['source'] == 'simulation' else user_input_folder)
            input_name = item['current_name']

            file_id = help_functions.find_file_id(drive_service, input_name, folder_name)
            if file_id not in seen:
                input_files_id_name_list.append({'file_id': file_id, 'input_name': input_name})
                seen.add(file_id)
    return input_files_id_name_list

def main():
    print('simulation start')
    config_file = '/Users/macbook/PycharmProjects/test/config_exe.yaml'
    meta_model_common = '/Users/macbook/PycharmProjects/test/meta_model_common.yaml'
    meta_model_user = '/Users/macbook/PycharmProjects/test/meta_model_user.yaml'
    meta_model_exe = '/Users/macbook/PycharmProjects/test/meta_model_exe.yaml'
    meta_inputs_exe = '/Users/macbook/PycharmProjects/test/meta_inputs_exe.yaml'
    common_input_folder = 'input_common'
    user_input_folder = 'input_user'
    simulation_input_folder = 'input_simulation'

    clean_file(meta_model_common)
    clean_file(meta_model_user)
    clean_file(meta_model_exe)
    clean_file(meta_inputs_exe)

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

    common_models_file_list = help_functions.get_files_in_folder(DRIVE, 'models_common')
    user_models_file_list = help_functions.get_files_in_folder(DRIVE, 'models_user')

    # the next command could be omitted later. Will use ready-made model
    meta_file_creation.create_meta_file(common_models_file_list, meta_model_common, serviceSheets)
    meta_file_creation.create_meta_file(user_models_file_list, meta_model_user, serviceSheets)
    # we create metafiles for models and all inputs
    meta_file_creation.create_meta_files_exe(meta_model_common, meta_model_user, meta_model_exe, config_exe_list)
    # prepare the exchange list of common models to user ones
    copy_models_with_added_current_input_to_exe_folder(DRIVE, meta_model_exe, 'models_common', 'models_user', 'models_exe', gs)
    input_files_id_list = prepare_input_files_id_name_list(DRIVE, meta_model_exe, common_input_folder, user_input_folder,
                                                           simulation_input_folder)
    copy_necessary_input_to_exe_environment(DRIVE, input_files_id_list)
    change_input_value_for_several_days(DRIVE, config_exe_list, gs)
    change_input_value_for_several_days_add_delta(DRIVE, config_exe_list, gs)
    start_day = get_start_day(config_exe_list)
    days_of_forecast = get_days_of_forecast(config_exe_list)
    df = calculations.calculate_forecast(serviceSheets, DRIVE, start_day, days_of_forecast, meta_model_exe, gs)
    print(df)


if __name__ == "__main__":
    main()
