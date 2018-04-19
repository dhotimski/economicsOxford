# we get files in the folder. Initially we get folder ID. Then we get all files from the folder
from apiclient import errors


def get_files_in_folder(service, folder_name):
    folder_id = get_folder_id(service, folder_name)
    q_string2 = "'" + str(folder_id) + "'" + " in parents" + " and trashed = false"
    children = service.files().list(
        q=q_string2,
        fields='nextPageToken, files(id, name)'
    ).execute()

    file_list = children['files']
    return file_list


def print_full(pd, x):
    pd.set_option('display.max_rows', len(x))
    print(x)
    pd.reset_option('display.max_rows')


def copy_file(service, origin_file_id, new_file_name, destination_folder_id):
    newfile = {'name': new_file_name, 'parents': [destination_folder_id]}
    try:
        return service.files().copy(
            fileId=origin_file_id, body=newfile).execute()
    except errors.HttpError, error:
        print 'An error occurred: %s' % error
    return None


def get_folder_id(service, folder_name):
    q_string = " mimeType ='application/vnd.google-apps.folder' and name = '" + folder_name + "'"
    response = service.files().list(
        q=q_string,
        spaces='drive',
        fields='nextPageToken, files(id, name)'
    ).execute()

    folder_id = response['files'][0]['id']
    return folder_id


def find_file_id(service, file_name, folder_name):
    folder_id = get_folder_id(service, folder_name)
    q_string2 = "name = " + "'" + file_name + "'" + " and trashed = false and " + "'" + str(
        folder_id) + "'" + " in parents"
    #  q_string2 = "name = 'gasoline' and "  + "'" + str(folder_id) + "'"  + " in parents"
    res = service.files().list(
        q=q_string2,
        fields='nextPageToken, files(id,name)'
    ).execute()
    if res is None:
        return None
    else:
        a = res['files']
        b = a[0]['id']
    return b
