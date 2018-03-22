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
    children = service.files().list(
        q=q_string2,
        fields='nextPageToken, files(id, name)'
    ).execute()

    file_list = children['files']
    return file_list