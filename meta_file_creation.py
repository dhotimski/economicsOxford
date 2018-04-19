import yaml


def prepare_exchange_list(changes_list):
    exchange_list = []
    a = filter(lambda command: command['command'] == 'change_model', changes_list)
    for item in a:
        exchange_list.append((item['initial_model'], item['final_model']))
    return exchange_list


def create_meta_files_exe(meta_model_common, meta_model_user, meta_model_exe, changes_list):
    # prepare list of model changes( i.e. Exxon <--> Exxon_user)
    exchange_list = prepare_exchange_list(changes_list)
    new_meta_data = []
    filter_which_models_to_include = models_to_include_and_filter_type(changes_list)
    (model_filter_case, included_models, excluded_models) = filter_which_models_to_include
    stream_common = open(meta_model_common, 'r')
    stream_user = open(meta_model_user, 'r')
    common_meta = yaml.load(stream_common)
    user_meta = yaml.load(stream_user)

    for model in common_meta:
        name = model['name']
        include_test = (model_filter_case == 0) \
                       or ((model_filter_case == 1) and (name not in excluded_models)) \
                       or ((model_filter_case == 2) and (name in included_models))
        if include_test:
            # model could be included  But changed to another
            m = change_test(exchange_list, model)
            # if model is not changed for another
            if not m:
                new_meta_data.append(model)
            else:
                user_model = filter(lambda item: item['name'] == m[0], user_meta)
                new_meta_data.append(user_model[0])

    new_meta_data1 = change_input_for_some_models(new_meta_data[:], changes_list)
    new_meta_data2 = change_input_for_all_models(new_meta_data1[:], changes_list)
    # change inputs whenever necessary
    stream = open(meta_model_exe, 'a')
    yaml.Dumper.ignore_aliases = lambda *args: True
    yaml.safe_dump(new_meta_data2, stream)


def change_test(exchange_list, model):
    model = list(item[1] for item in exchange_list if item[0] == model['name'])
    return model


def change_input_for_some_models(all_data, changes_list):
    #   - command: change_input_series_one_model
    #   model_name: Goodyear
    #  input_initial: oil
    #  input_final: {'name':oil2,'source': 'user',input_id:0}

    a = filter(lambda command: command['command'] == 'change_input_series_one_model', changes_list)
    for item in a:
        model_name = item['model_name']
        initial_input_name = item['input_initial']
        new_input_data = item['input_final']
        all_data = change_input_name_in_one_model(model_name, initial_input_name, new_input_data, all_data)
    return all_data


def change_input_for_all_models(all_data, changes_list):
    # - command: change_input_series_all_models
    # input_initial: oil
    # input_final: {'name':oil2,'source': 'user',input_id:0}
    # input_source: common
    a = filter(lambda command: command['command'] == 'change_input_series_all_models', changes_list)
    for item in a:
        initial_input_name = item['input_initial']
        new_input_data = item['input_final']
        all_data = change_input_in_all_models(initial_input_name, new_input_data, all_data[:])
    return all_data


# Find necessary model. Change input in it
def change_input_name_in_one_model(model_name, initial_input_name, new_input_data, all_data):
    a = [x if x['name'] != model_name else metadata_with_adjusted_input(initial_input_name, new_input_data, x) for x in
         all_data]
    return a


def change_input_in_all_models(initial_input_name, new_input_data, all_data):
    a = [x if not (has_input(initial_input_name, x['input'])) else metadata_with_adjusted_input(initial_input_name,
                                                                                                new_input_data, x) for x
         in
         all_data]
    return a


def has_input(initial_input_name, input_list):
    a = filter(lambda item: item['initial_name'] == initial_input_name, input_list)
    check = len(a) == 1
    return check


def metadata_with_adjusted_input(initial_input_name, new_input_data, model_dictionary_item):
    b = model_dictionary_item.copy()
    c = model_dictionary_item['input']
    d = []
    #  order = c[0]['order']
    for item in c:
        if item['current_name'] != initial_input_name:
            d.append(item)
        else:
            k = new_input_data.copy()
            k['order'] = item['order']
            k['initial_name'] = initial_input_name
            d.append(k)
    b['input'] = d
    return b


def models_to_include_and_filter_type(changes_list):
    # return filter type for each model
    a = filter(lambda command: command['command'] == 'filter_models', changes_list)
    if len(a) == 0:
        return (0, [], [])
    elif a[0]['filter_type'] == 'all_but':
        return (1, [], a[0]['exclude'])
    else:
        return (2, a[0]['include'], [])


def create_meta_file(models_file_list, meta_model_file, serviceSheets):
    all_data = []
    for model in models_file_list:
        file_id = model['id']
        data = get_metadata_from_excel_file(serviceSheets, file_id)
        all_data.append(data)
    stream = open(meta_model_file, 'a')
    yaml.safe_dump(all_data, stream)


def get_metadata_from_excel_file(serviceSheets, file_id):
    data = dict()

    model_name = serviceSheets.spreadsheets().values().get(
        spreadsheetId=file_id, range='Name!B1').execute()
    data['name'] = model_name['values'][0][0]
    model_source = serviceSheets.spreadsheets().values().get(
        spreadsheetId=file_id, range='Name!B2').execute()
    data['source'] = model_source['values'][0][0]
    #   sheet_output = wb.worksheet("Output")
    total_outputs_result = serviceSheets.spreadsheets().values().get(
        spreadsheetId=file_id, range='Output!B1').execute()
    total_outputs = total_outputs_result['values'][0][0]
    a = int(total_outputs)
    range_output = 'Output!A2:A' + str(a + 1)
    outputs_list = serviceSheets.spreadsheets().values().get(
        spreadsheetId=file_id, range=range_output).execute()
    outputs = outputs_list['values']
    data['output'] = map(lambda (i, x): {'order': i, 'current_name': x[0]}, enumerate(outputs))

    total_inputs_result = serviceSheets.spreadsheets().values().get(
        spreadsheetId=file_id, range='Input!B1').execute()
    total_inputs = total_inputs_result['values'][0][0]
    a = int(total_inputs)
    range_inputs = 'Input!A3:D' + str(a + 2)
    inputs_list = serviceSheets.spreadsheets().values().get(
        spreadsheetId=file_id, range=range_inputs).execute()
    # data['input'] = map(lambda x: x[0], inputs_list['values'])
    inputs = inputs_list['values']
    data['input'] = map(
        lambda (i, (v, x, y, z)): {'order': i, 'current_name': v, 'source': x, 'series_id': y, 'initial_name': v,
                                   'comments': z}, enumerate(inputs))
    # map(lambda (i, x): {'name': x, 'rank': i}, enumerate(ranked_users))
    # user_details = [{'name': x, 'rank': i} for i, x in enumerate(ranked_users)]
    # data['input'] = input_combine_labeled
    # check next line
    data['calculator'] = file_id
    # model_id is not ready yet
    data['model_id'] = 1
    return data



