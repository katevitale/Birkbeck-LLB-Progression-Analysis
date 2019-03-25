import pandas as pd
import numpy as np
import xlrd
import csv
import uuid
from sklearn.utils import shuffle


def csv_from_xls(filename, output_path):
    '''
    Converts excel xls sheets to csv.
    Adapted from 'https://stackoverflow.com/questions/20105
    118/convert-xlsx-to-csv-correctly-using-python'.
    '''
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
    '''
    Takes in a path for csv file of a module table
    and a path for storing the dictionary of spr codes
    and unique ids assigned to each code.

    Returns an anonymized and restructured dataframe
    of the table and the complete spr code dictionary.

    DOES NOT HANDLE BLANK ROWS
    These need to be manually deleted prior to being
    entered into this function.
    '''

    # put csv contents into dataframe
    module_df = pd.read_csv(csv_filepath)


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

    # take off the /number at the end of SPR Codes
    new_df['SPR Code'] = (
        new_df['SPR Code'].astype(str).str.split('/', expand=True).iloc[:,0])

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
    '''
    Takes in a path for csv file of progression table
    and a dictionary of spr codes and unique ids assigned
    to each spr code.

    Returns an anonymized of the table and the updated
    spr code dictionary.

    DOES NOT HANDLE BLANK ROWS
    These need to be manually deleted prior to being
    entered into this function.
    '''

    prog_df = pd.read_csv(csv_filepath)

    prog_df['SPR Code'] = (
        prog_df['SPR Code'].astype(str).str.split('/', expand=True).iloc[:,0])

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

def make_module_summary(df,dfname,n_assessments):
    '''
    Takes a module dataframe, its name and its
    number of assessments.

    Returns a series of calculated values to
    input into a module summary table.

    '''
    year, module, tmp = dfname.split('_')
    n_total = len(df)
    ser = pd.Series()
    # Calculate passing stats
    ser['N (total)'] = n_total
    ser['% Pass (total)'] = (100 * sum(df['Result']=='P') / n_total)
    ser['% Att. 1'] = (100 * sum(df['Attempt'].astype('int') == 1) / n_total)
    ser['% Pass (Att. 1)'] = (100 * (sum((df['Result'] == 'P') & (df['Attempt'].astype('int') == 1) )
                                     / sum(df['Attempt'].astype('int') == 1)))
    if any(df['Attempt'].astype('int') == 2):
        ser['% Att. 2'] = (100 * sum(df['Attempt'].astype('int') == 2) / n_total)
        ser['% Pass (Att. 2)'] = (100 * (sum((df['Result'] == 'P')
                                           & (df['Attempt'].astype('int') == 2))
                                         / sum(df['Attempt'].astype('int') == 2)))
    else:
        ser['% Att. 2'] = np.NaN
        ser['% Pass (Att. 2)'] = np.NaN
    if any(df['Attempt'].astype('int') == 3):
        ser['% Att. 3'] = (100 * sum(df['Attempt'].astype('int') == 3) / n_total)
        ser['% Pass (Att. 3)'] = (100 * (sum((df['Result'] == 'P')
                                           & (df['Attempt'].astype('int') == 3) )
                                          / sum(df['Attempt'].astype('int') == 3)))
    else:
        ser['% Att. 3'] = np.NaN
        ser['% Pass (Att. 3)'] = np.NaN
    # Calculate retake stats
    if year != '201718':
        ser['% Retake Next Year'] = (100 * sum(df['Retake next year'] != 'N')
                                     / n_total)
        ser['% of Retake that Pass (total)'] = (100 * (sum(df['Retake and pass'] == True)
                                                      / sum(df['Retake next year'] != 'N')))
        if sum(df['Retake and pass'] == False) != 0:
            ser['% of Retake and Fail that Do Not Submit'] = (100 * (
                                                            sum(df['Retake and fail submit stat'] == 'No submit')
                                                          / sum(df['Retake and pass'] == False)))
            ser['% of Retake and Fail that Fail Submission'] = (100 * (
                                                            sum(df['Retake and fail submit stat'] == 'Failed submit')
                                                          / sum(df['Retake and pass'] == False)))
        else:
            ser['% of Retake and Fail that Do Not Submit'] = np.NaN
            ser['% of Retake and Fail that Fail Submission'] = np.NaN
    else:
        ser['% Retake Next Year'] = np.NaN
        ser['% of Retake that Pass (total)'] = np.NaN
        ser['% of Retake and Fail that Do Not Submit'] = np.NaN
        ser['% of Retake and Fail that Fail Submission'] = np.NaN
    # Calculations for only modules where assessment number == 1
    if n_assessments == 1:
        # Calculate reassessment stats
        ser['% Reassess (Either Asst FR or DR)'] = (100 * sum(df['Assessment 1 Grade'].isin(['FR', 'DR']))
                                                   / n_total)
        ser['% Reassess that Pass (F Otherwise)'] = (100 * sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                            & (df['Result'] == 'P'))
                                                    / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])))
        ser['% Reassess that Pass (P Otherwise)'] = np.NaN
        ser['% Reassess and final grade DR'] = (100 * sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                            & (df['Result'] == 'D'))
                                                / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])))
        ser['% Reassess and final grade blank'] = (100 * sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                            & (df['Result'].isnull()))
                                                    / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])))
        if sum(df['Assessment 1 Grade'].isin(['FR', 'DR']) != 0):
            try:
                ser['% Reassess & No Sub at Reassess'] = (100 * sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                                 & (df['Result'] == 'F')
                                                                 # to not catch passing students that don't submit reassess
                                                                 & (pd.to_numeric(df['Mark'],errors='coerce')
                                                                   == pd.to_numeric(df['Assessment 1 Mark'],errors='coerce')))
                                                      / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])))
                ser['% Reassess & Failed Sub at Reassess'] = (100 * sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                                     & (df['Result'] == 'F')
                                                                        # to not catch passing students
                                                                     & (pd.to_numeric(df['Mark'],errors='coerce')
                                                                       != pd.to_numeric(
                                                                           df['Assessment 1 Mark'],errors='coerce')))
                                                      / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])))
            except:
                print(year, module + ''': error processing ser['% Reassess & No Sub at Reassess']
                                        or ser['% Reassess & Failed Sub at Reassess']''')
        else:
            print(year, module + ": sum(df['Assessment 1 Grade'].isin(['FR', 'DR']) = 0")
        # Calculate Assessment 1 stats
        ser['Asst. 1: % Pass'] = (100 * (sum(df['Assessment 1 P or F'] == 'P')) / n_total)
        ser['Asst. 1: % F (Not DR or FR)'] = (100 * sum(df['Assessment 1 Grade'] == 'F') / n_total)
        ser['Asst. 1: % W'] = (100 * sum(df['Assessment 1 Grade'] == 'W') / n_total)
        ser['Asst. 1: % FR'] = (100 * sum(df['Assessment 1 Grade'] == 'FR') / n_total)
        ser['Asst. 1: % DR'] = (100 * sum(df['Assessment 1 Grade'] == 'DR') / n_total)
        ser['Asst. 1: % Blank'] = (100 * sum(df['Assessment 1 Grade'].isnull()) / n_total)
        ser['Asst. 1: % LF'] = (100 * sum(df['Assessment 1 Grade'] == 'LF') / n_total)
        ser['Asst. 1: % No Sub'] = (100 * sum(pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') == 0.0 )/ n_total)
        ser['Asst. 1: % Failed Sub'] = (100 * sum((pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') > 0.0)
                                                 & (pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') < 40.0))
                                        / n_total)
        ser['Asst. 1 F: % No Sub'] = (100 * sum((pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') == 0.0)
                                                & (df['Assessment 1 P or F'] == 'F'))
                                      / sum(df['Assessment 1 P or F'] == 'F'))
        ser['Asst. 1 F: % Failed Sub'] = (100 * sum((pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') != 0.0)
                                                & (df['Assessment 1 P or F'] == 'F'))
                                      / sum(df['Assessment 1 P or F'] == 'F'))
        if sum(df['Assessment 1 Grade'] == 'FR') != 0:
            ser['Asst. 1: % FR that Pass (F Otherwise)'] = (100 * sum((df['Assessment 1 Grade'] == 'FR')
                                                                      & (df['Result'] == 'P'))
                                                            / sum(df['Assessment 1 Grade'] == 'FR'))
        else:
            print(year, module + ': no Assessment 1 FR')
        if sum(df['Assessment 1 Grade'] == 'DR') != 0:
            ser['Asst. 1: % DR that Pass (F Otherwise)'] = (100 * sum((df['Assessment 1 Grade'] == 'DR')
                                                                      & (df['Result'] == 'P'))
                                                            / sum(df['Assessment 1 Grade'] == 'DR'))
        else:
            print(year, module + ': no Assessment 1 DR')
        ser['Asst. 1: % Submit that Pass'] = (100 * sum(
                                                (pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') > 0.0)
                                                & (df['Assessment 1 P or F'] == 'P'))
                                                / sum(pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') > 0.0))
        ser['Asst. 2: % Pass'] = np.NaN
        ser['Asst. 2: % F (Not DR or FR)'] = np.NaN
        ser['Asst. 2: % W'] = np.NaN
        ser['Asst. 2: % FR'] = np.NaN
        ser['Asst. 2: % DR'] = np.NaN
        ser['Asst. 2: % Blank'] = np.NaN
        ser['Asst. 2: % LF'] = np.NaN
        ser['Asst. 2: % No Sub'] = np.NaN
        ser['Asst. 2: % Failed Sub'] = np.NaN
        ser['Asst. 2: % FR that Pass (F Otherwise)'] = np.NaN
        ser['Asst. 2: % DR that Pass (F Otherwise)'] = np.NaN
        ser['Asst. 2: % Submit that Pass'] = np.NaN
    # Set of calculations for only modules where assignment number == 2
    elif n_assessments == 2:
    # Calculate reassessment stats
        ser['% Reassess (Either Asst FR or DR)'] = (100 * sum(df['Assessment 1 Grade'].isin(['FR', 'DR']) |
                                                              df['Assessment 2 Grade'].isin(['FR', 'DR']))
                                                            / n_total)
        if module != 'LSM':
            ser['% Reassess that Pass (F Otherwise)'] = (100 * sum((df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                                 | df['Assessment 2 Grade'].isin(['FR', 'DR']))
                                                                & ((0.5 * (pd.to_numeric(
                                                                               df['Assessment 1 Mark'],errors='coerce')
                                                                           + pd.to_numeric(
                                                                               df['Assessment 2 Mark'],errors='coerce')))
                                                                           < 40.0)
                                                                & (df['Result'] == 'P'))
                                                      / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                                 | df['Assessment 2 Grade'].isin(['FR', 'DR'])))
            ser['% Reassess that Pass (P Otherwise)'] = (100 * sum((df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                              | df['Assessment 2 Grade'].isin(['FR', 'DR']))
                                                              & ((0.5 * (pd.to_numeric(
                                                                            df['Assessment 1 Mark'],errors='coerce')
                                                                       + pd.to_numeric(
                                                                            df['Assessment 2 Mark'],errors='coerce')))
                                                                       >= 40.0)
                                                              & (df['Result'] == 'P'))
                                                       / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                             | df['Assessment 2 Grade'].isin(['FR', 'DR'])))
        else:
            ser['% Reassess that Pass (F Otherwise)'] = (100 * sum((df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                                 | df['Assessment 2 Grade'].isin(['FR', 'DR']))
                                                                 & (df['Result'] == 'P'))
                                                       / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                           | df['Assessment 2 Grade'].isin(['FR', 'DR'])))
            ser['% Reassess that Pass (P Otherwise)'] = np.NaN
        ser['% Reassess and final grade DR'] = (100 * sum((df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                          | df['Assessment 2 Grade'].isin(['FR', 'DR']))
                                                            & (df['Result'] == 'D'))
                                                / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                             | df['Assessment 2 Grade'].isin(['FR', 'DR'])))
        ser['% Reassess and final grade blank'] = (100 * sum((df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                          | df['Assessment 2 Grade'].isin(['FR', 'DR']))
                                                            & (df['Result'].isnull()))
                                                    / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                             | df['Assessment 2 Grade'].isin(['FR', 'DR'])))
        if module != 'LSM':
            ser['% Reassess & No Sub at Reassess'] = (100 * sum((df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                                 | df['Assessment 2 Grade'].isin(['FR', 'DR']))
                                                                & ((0.5 * (pd.to_numeric(
                                                                                df['Assessment 1 Mark'],errors='coerce')
                                                                          + pd.to_numeric(
                                                                                df['Assessment 2 Mark'],errors='coerce')))
                                                                           == df['Mark'])
                                                                & (df['Result'] == 'F'))
                                                        / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                             | df['Assessment 2 Grade'].isin(['FR', 'DR'])))
            ser['% Reassess & Failed Sub at Reassess'] = (100 * sum((df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                             | df['Assessment 2 Grade'].isin(['FR', 'DR']))
                                                            & ((0.5 * (pd.to_numeric(
                                                                         df['Assessment 1 Mark'],errors='coerce')
                                                                     + pd.to_numeric(
                                                                         df['Assessment 2 Mark'],errors='coerce')))
                                                                     != df['Mark'])
                                                            & (df['Result'] == 'F'))
                                                        / sum(df['Assessment 1 Grade'].isin(['FR', 'DR'])
                                                             | df['Assessment 2 Grade'].isin(['FR', 'DR'])))
        else:
            ser['% Reassess & No Sub at Reassess'] = np.NaN
            ser['% Reassess & Failed Sub at Reassess'] = np.NaN
        # Calculate Assessment 1 stats
        ser['Asst. 1: % Pass'] = (100 * (sum(df['Assessment 1 P or F'] == 'P')) / n_total)
        ser['Asst. 1: % F (Not DR or FR)'] = (100 * sum(df['Assessment 1 Grade'] == 'F') / n_total)
        ser['Asst. 1: % W'] = (100 * sum(df['Assessment 1 Grade'] == 'W') / n_total)
        ser['Asst. 1: % FR'] = (100 * sum(df['Assessment 1 Grade'] == 'FR') / n_total)
        ser['Asst. 1: % DR'] = (100 * sum(df['Assessment 1 Grade'] == 'DR') / n_total)
        ser['Asst. 1: % Blank'] = (100 * sum(df['Assessment 1 Grade'].isnull()) / n_total)
        ser['Asst. 1: % LF'] = (100 * sum(df['Assessment 1 Grade'] == 'LF') / n_total)
        if module != 'LSM':
            ser['Asst. 1: % No Sub'] = (100 * sum(pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') == 0.0 )/ n_total)
            ser['Asst. 1: % Failed Sub'] = (100 * sum((pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') > 0.0)
                                                 & (pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') < 40.0))
                                        / n_total)
            ser['Asst. 1 F: % No Sub'] = (100 * sum((pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') == 0.0)
                                                & (df['Assessment 1 P or F'] == 'F'))
                                      / sum(df['Assessment 1 P or F'] == 'F'))
            ser['Asst. 1 F: % Failed Sub'] = (100 * sum((pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') != 0.0)
                                                & (df['Assessment 1 P or F'] == 'F'))
                                      / sum(df['Assessment 1 P or F'] == 'F'))
            ser['Asst. 1: % Submit that Pass'] = (100 * sum(
                                                    (pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') > 0.0)
                                                    & (df['Assessment 1 P or F'] == 'P'))
                                                    / sum(pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') > 0.0))
        else:
            ser['Asst. 1: % No Sub'] = np.NaN
            ser['Asst. 1: % Failed Sub'] = np.NaN
            ser['Asst. 1 F: % No Sub'] = np.NaN
            ser['Asst. 1 F: % Failed Sub'] = np.NaN
            ser['Asst. 1: % Submit that Pass'] = np.NaN
        if sum(df['Assessment 1 Grade'] == 'FR') != 0:
            ser['Asst. 1: % FR that Pass (F Otherwise)'] = (100 * sum((df['Assessment 1 Grade'] == 'FR')
                                                                      & (df['Result'] == 'P'))
                                                            / sum(df['Assessment 1 Grade'] == 'FR'))
        else:
            print(year, module + ': no Assessment 1 FR')
        if sum(df['Assessment 1 Grade'] == 'DR') != 0:
            ser['Asst. 1: % DR that Pass (F Otherwise)'] = (100 * sum((df['Assessment 1 Grade'] == 'DR')
                                                                      & (df['Result'] == 'P'))
                                                            / sum(df['Assessment 1 Grade'] == 'DR'))
        else:
            print(year, module + ': no Assessment 1 DR')
        ser['Asst. 2: % Pass'] = (100 * (sum(df['Assessment 2 P or F'] == 'P')) / n_total)
        ser['Asst. 2: % F (Not DR or FR)'] = (100 * sum(df['Assessment 2 Grade'] == 'F') / n_total)
        ser['Asst. 2: % W'] = (100 * sum(df['Assessment 2 Grade'] == 'W') / n_total)
        ser['Asst. 2: % FR'] = (100 * sum(df['Assessment 2 Grade'] == 'FR') / n_total)
        ser['Asst. 2: % DR'] = (100 * sum(df['Assessment 2 Grade'] == 'DR') / n_total)
        ser['Asst. 2: % Blank'] = (100 * sum(df['Assessment 2 Grade'].isnull()) / n_total)
        ser['Asst. 2: % LF'] = (100 * sum(df['Assessment 2 Grade'] == 'LF') / n_total)
        if module != 'LSM':
            ser['Asst. 2: % No Sub'] = (100 * sum(pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') == 0.0 )/ n_total)
            ser['Asst. 2: % Failed Sub'] = (100 * sum((pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') > 0.0)
                                                     & (pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') < 40.0))
                                            / n_total)
            ser['Asst. 2 F: % No Sub'] = (100 * sum((pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') == 0.0)
                                                    & (df['Assessment 2 P or F'] == 'F'))
                                          / sum(df['Assessment 2 P or F'] == 'F'))
            ser['Asst. 2 F: % Failed Sub'] = (100 * sum((pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') != 0.0)
                                                    & (df['Assessment 2 P or F'] == 'F'))
                                          / sum(df['Assessment 2 P or F'] == 'F'))
            ser['Asst. 2: % Submit that Pass'] = (100 * sum(
                                                    (pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') > 0.0)
                                                    & (df['Assessment 2 P or F'] == 'P'))
                                                    / sum(pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') > 0.0))
        else:
            ser['Asst. 2: % No Sub'] = np.NaN
            ser['Asst. 2: % Failed Sub'] = np.NaN
            ser['Asst. 2 F: % No Sub'] = np.NaN
            ser['Asst. 2 F: % Failed Sub'] = np.NaN
            ser['Asst. 2: % Submit that Pass'] = np.NaN
        ser['Asst. 2: % FR that Pass (F Otherwise)'] = (100 * sum((df['Assessment 2 Grade'] == 'FR')
                                                                  & (pd.to_numeric(
                                                                      df['Assessment 1 Mark'],errors='coerce')
                                                                     + pd.to_numeric(
                                                                         df['Assessment 2 Mark'],errors='coerce')
                                                                     < 80.0)
                                                                  & (df['Result'] == 'P'))
                                                        / sum(df['Assessment 2 Grade'] == 'FR'))
        try:
            ser['Asst. 2: % DR that Pass (F Otherwise)'] = (100 * sum((df['Assessment 2 Grade'] == 'DR')
                                                                  & (pd.to_numeric(
                                                                      df['Assessment 1 Mark'],errors='coerce')
                                                                     + pd.to_numeric(
                                                                         df['Assessment 2 Mark'],errors='coerce')
                                                                     < 80.0)
                                                                  & (df['Result'] == 'P'))
                                                        / sum(df['Assessment 2 Grade'] == 'DR'))
        except:
            print(year,module + "ser['Asst. 2: % DR that Pass (F Otherwise)']")

    else:
        print(dfname, 'error with assignment number is not equal to 1 or 2.')
    # return the series
    ser = ser.T
    return ser
