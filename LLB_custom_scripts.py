# import statements
import pandas as pd
import numpy as np
import xlrd
import csv
import uuid
from sklearn.utils import shuffle

# Define function for converting excel xls sheets to csv
# adapted from 'https://stackoverflow.com/questions/20105
# 118/convert-xlsx-to-csv-correctly-using-python'.

# DOES NOT HANDLE BLANK ROWS
# need to manually delete prior to processing
def csv_from_xls(filename, output_path):
    wb = xlrd.open_workbook(filename)
    sh = wb.sheet_by_name('Sheet1')
    short_name = filename.split('/')[-1]
    shorter_name = short_name.split('.')[0]
    csv_file = open(output_path + f'{shorter_name}.csv', 'w')
    wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)

    for row_num in range(sh.nrows):
        wr.writerow(sh.row_values(row_num))

    csv_file.close()

def anonymize_module(csv_filepath, SPR_dict):
    # should take in spr path as well

    # put csv contents into dataframe
    module_df = pd.read_csv(csv_filepath, encoding = "ISO-8859-1")

    module_df['SPR Code'] = module_df['SPR Code'].astype(str)

    # get new column names for new dataframe with each row representing a single student
    columns_1 = module_df.columns.values.tolist()
    assignments_columns = module_df.iloc[0].values.tolist()


    # this is the main row with student number, etc
    row_inds_1 = module_df.index.values[module_df['Ocurr'] == 'AAA']

    # get number of assessments
    num_assessments = row_inds_1[1] - row_inds_1[0] - 1

    # loop through assessments

    # make dictionary
    column_titles = {}
    for assessment in range(1, num_assessments+1):
        column_titles[assessment] = [
        f'Assessment {assessment} ' + str(x) for x in assignments_columns]

    # make column titles
    new_columns = []
    new_columns = columns_1

    for assessment in column_titles:
        new_columns.extend(column_titles[assessment])

    # make parts of new dataframe
    # get indices of each type of row

    row_inds = {}

    for assessment in range(1, num_assessments+1):
        row_inds[assessment] = row_inds_1 + int(assessment)

    # get each type of dataframe as an array
    array_1 = module_df.iloc[row_inds_1].values

    # loop through assessments to make new arrays
    arrays = {}

    for assessment in range(1, num_assessments+1):
        arrays[assessment] = module_df.iloc[row_inds[assessment]].values

    # make new dataframe from horizontally stacked arrays
    new_array = array_1

    for assessment in range(1, num_assessments+1):
        new_array = np.hstack((new_array, arrays[assessment]))

    new_df = pd.DataFrame(data=new_array, columns=new_columns)
    new_df = new_df.dropna(axis='columns', how='all') # drop columns that have all NaN values

    # go through each row and replace SPR code with another number. put mapping in dictionary.

    # loop through SPR Code column and make replacement in order to create new dataframe
    # and also to populate dictionary
    for (index, value) in new_df['SPR Code'].iteritems():
        if value in SPR_dict:
            new_df.at[index, 'SPR Code'] = SPR_dict[value]
        else:
            new_value = str(uuid.uuid4())
            new_dict_pair = {value: new_value}
            SPR_dict.update(new_dict_pair)
            new_df.at[index, 'SPR Code'] = new_value

    # shuffle rows for increased anonymity
    new_df = shuffle(new_df)

    # clean up dataframe to get rid of unnecessary columns
    new_df.drop(labels=['Ocurr', 'Student name'], axis=1, inplace=True)
    new_df.set_index('SPR Code', inplace=True)

    # return the SPR_dict and DataFrame
    return SPR_dict, new_df


def anonymize_progression(csv_filepath, SPR_dict):
    # should take in SPR dict as well

    prog_df = pd.read_csv(csv_filepath, encoding = "ISO-8859-1")

    prog_df['SPR Code'] = prog_df['SPR Code'].astype(str)

    # loop through SPR Code column and make replacement in order to create new dataframe
    # and also to populate dictionary
    for (index, value) in prog_df['SPR Code'].iteritems():
        if value in SPR_dict:
            prog_df.at[index, 'SPR Code'] = SPR_dict[value]
        else:
            new_value = str(uuid.uuid4())
            new_dict_pair = {value: new_value}
            SPR_dict.update(new_dict_pair)
            prog_df.at[index, 'SPR Code'] = new_value

    # shuffle rows for increased anonymity
    new_df = shuffle(prog_df)

    # clean up dataframe to get rid of unnecessary columns
    new_df = new_df.dropna(axis='columns', how='all') # drop columns that have all NaN values
    new_df.drop(labels=['Student name'], axis=1, inplace=True)
    new_df.set_index('SPR Code', inplace=True)

    # return the SPR_dict and DataFrame
    return SPR_dict, new_df

