import authorize
import help_functions
import gspread
#import ec2
from apiclient import discovery
from httplib2 import Http
import io
import yaml
from datetime import datetime, date, timedelta

config_file = '/Users/macbook/PycharmProjects/test/config_exe.yaml'
output_file = '/Users/macbook/PycharmProjects/test/output.csv'
meta_model_file = '/Users/macbook/PycharmProjects/test/MetaModel.yaml'
output = open(meta_model_file, 'w')
output.truncate()
stream = io.open(config_file, 'r')
config_exe_list = list(yaml.load_all(stream))[0]


# authorization Google Drive
# we use our own authorize module
credentials = authorize.get_credentials()

# create Google Drive Service
DRIVE = discovery.build('drive', 'v3', http=credentials.authorize(Http()))

# authorization gspread
gc = gspread.authorize(credentials)

# we prepare the file which will be used to execute all the models
# it will be in yaml
# it takes into account config file
# get Model lists. They are located in Google Drive folder 'models'

models_file_list = help_functions.get_files_in_folder(DRIVE, 'models_common')
models_user_file_list = help_functions.get_files_in_folder(DRIVE, 'models_user')



def prepare_metafile_exe(models_file_list2):

    # prepare list of all models.
    #  Point out where they are stored( "user" or "common" directory)
    # There are three cases. Case 0 - No any filters on models. Case 1- "All but" filter. Case 2- Include filter.
    # Determine Case number
    filter_models = model_filter_type() #  filter_models  = (model_filter_case,included_models,excluded_models)

    register_all_models(models_file_list2, meta_model_file, gc,filter_models)



    # for every model prepare the list of inputs.
    # Every input should provide both names ("name in the model calculator and name of correct series").
    # Point out where this series is stored ( "user" or "common" directory)
    # Prepare the list of all changes to time series.



def model_filter_type():
    a = filter(lambda command: command['command'] == 'filter_models', config_exe_list)
    if len(a) == 0: return (0,[],[])
    elif a[0]['filter_type'] == 'all_but': return (1,[],a[0]['exclude'])
    else: return (2,a[0]['include'],[])



# register models
def register_all_models(files, meta_model_file, gc,filter):
    (model_filter_case, included_models, excluded_models) = filter
    all_data = []
    for model in files:
        name = model['name']
        include_test = (model_filter_case == 0) \
                       or ((model_filter_case == 1) and (name not in excluded_models)) \
                       or((model_filter_case == 2) and (name in included_models))
        if include_test:
                wb = gc.open(name)
                data = get_metadata_from_excel_file_gspread(wb)
                all_data.append(data)
    stream = open(meta_model_file, 'a')
    yaml.dump(all_data, stream)





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


def get_start_day(config_exe_list):
    a= filter(lambda command: command['command'] == 'start_day', config_exe_list)
    b = a[0]['start_day']
    return  b

def get_days_of_forecast():
    a = filter(lambda command: command['command'] == 'number_of_days', config_exe_list)
    b = a[0]['number_of_days']
    return b



def copy_necessary_input_to_exe_environment():
    # copy
    # correct
    q=1

def copy_necessary_models_to_exe_environment():
    # copy
    q = 1

def copy_initial_data_to_output(output_file):
    #output_file.truncate()
    q=1

def calculate_forecast(first_date, number_of_days, meta_models_file, output_file, gs):
    copy_initial_data_to_output(output_file)

def change_common_models_for_user_models(models_file_list):
 #   - command: change_model
 #   initial_model: Goodyear
 #   final_model: Goodyear_user
    exchange_list = prepare_exchange_list()
    for item in exchange_list:
        new_model_file_list = delete_old_record(item[0],models_file_list)
        new_model_file_list2 = insert_new_record(item[1],new_model_file_list)
    return new_model_file_list2

def delete_old_record(name,models_file_list):
    a = filter(lambda model: model['name'] != name, models_file_list)
    return a


def insert_new_record(name,models_file_list2):
    a = filter(lambda model: model['name'] == name, models_user_file_list)
    b = a[0]
    c = models_file_list2[:]
    c.append(b)
    return c


def prepare_exchange_list():
    exchange_list = []
    a = filter(lambda command: command['command'] == 'change_model', config_exe_list)
    for item in a:
        exchange_list.append((item['initial_model'],item['final_model']))
    return exchange_list

# exchange some of models to user ones
models_file_list2 = change_common_models_for_user_models(models_file_list)
prepare_metafile_exe(models_file_list2)

copy_necessary_input_to_exe_environment()

copy_necessary_models_to_exe_environment()



startDay = get_start_day(config_exe_list)
days_of_forecast = get_days_of_forecast()



calculate_forecast(startDay, days_of_forecast, meta_model_file, output_file, gc)