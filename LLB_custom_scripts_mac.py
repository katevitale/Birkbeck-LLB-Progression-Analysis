import pandas as pd
import numpy as np
import xlrd
import csv
import uuid
from sklearn.utils import shuffle
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

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

def add_fr_and_dr_flags(df):
    '''
    Adds Reassessment (FR and DR) flags to the module files to
    indicate that student reassessed with FR or DR that particular
    year.'''
    df['DR Flag'] = df.apply(lambda row: any(row.isin(['DR'])) and (row['Result'] != 'D'), axis=1) # bc final grade can also be 'DR' (then Final Result == 'D')
    df['FR Flag'] = df.apply(lambda row: any(row.isin(['FR'])), axis=1)
    return df

def record_history(row):
    for record in row['Entire Record']:
        year, course, reassess_flag, dr_flag, attemptnum, mark, result = record.split()
        row[f'{course} History'].append(record)

def get_last_attempt_year(list_of_attempts):
    '''
    Returns year of student's last attempt
    given a list of attempts.
    '''
    if any(list_of_attempts):
        last_record = list_of_attempts[-1]
        l_yr, l_rs, l_att = last_record
        l_yr = int(l_yr[0:2] + l_yr[-2:]) # will record the last year in a multi-year term
        return l_yr

def get_last_year_on_record_from_attempts(row):
    '''
    Returns year of student's last attempt
    given a row of STUDENTS dataframe.
    Computes last year from list of attempts in each
    (module) Attempts column in the STUDENTS df
    using function 'get_last_attempt_year'.
    '''
    courses = ['CONAD','CONTRACT','LSM','TORT','EQUITY','LAND','CRIMINAL','EU','LT1','LT2']
    ser = pd.Series([row[f'{course} Attempts'] for course in courses]).apply(get_last_attempt_year)
    if any(ser):
        return max(ser)

def make_year_columns(row):
    list_years = ['201112','201213','201314','201415',
              '201516', '201617','201718']
    list_courses = ['CONAD','CONTRACT','LSM','TORT','CRIMINAL',
                'LAND','EQUITY','EU','LT1','LT2']
    first_year = row['Year started']
    if pd.isnull(row['Last year']):
        index = row.name
        print(index, 'error: last year is nan.')
        pass
    else:
        last_year = (str(row['Last year'])[:2]
                     + str(int(row['Last year'] - 1))[-2:]
                     + str(int(row['Last year']))[-2:])
        if (last_year == '201819'):
            pass
        else:
            for i, year in enumerate(list_years[list_years.index(first_year):
                                                list_years.index(last_year)+1], start=1):
                # make a column and get courses and put them in the column
                for course in list_courses:
                    for attempt in row[f'{course} Attempts']:
                        att_yr, att_res, att_num = attempt
                        if att_yr == year:
                            row[f'Year {i} Courses'].append((course, att_res, att_num))
        return row

def make_progression_columns(row):
    programme = row['Programme']
    for i in range(1,7,1):
        modules = [entry[0] if any(entry) else np.NaN for entry in row[f'Year {i} Courses']]
        if programme == 'PT':
            y1_courses_pt = ['CONAD','CONTRACT','LSM']
            y2_courses_pt = ['TORT','CRIMINAL']
            y3_courses_pt = ['LAND','EQUITY']
            y4_courses_pt = ['EU','LT1','LT2']
            if any(module for module in modules if module in y4_courses_pt):
                row[f'Year {i} Progression'] = 'Year 4'
            elif any(module for module in modules if module in y3_courses_pt):
                row[f'Year {i} Progression'] = 'Year 3'
            elif any(module for module in modules if module in y2_courses_pt):
                row[f'Year {i} Progression'] = 'Year 2'
            elif any(module for module in modules if module in y1_courses_pt):
                row[f'Year {i} Progression'] = 'Year 1'
            else:
                row[f'Year {i} Progression'] = np.NaN
        elif programme == 'FT':
            y1_courses_ft = ['CONAD','CONTRACT','LSM','TORT']
            y2_courses_ft = ['CRIMINAL','EQUITY']
            y3_courses_ft = ['EU','LT1','LT2']
            if any(module for module in modules if module in y3_courses_ft):
                row[f'Year {i} Progression'] = 'Year 3'
            elif any(module for module in modules if module in y2_courses_ft):
                row[f'Year {i} Progression'] = 'Year 2'
            elif any(module for module in modules if module in y1_courses_ft):
                row[f'Year {i} Progression'] = 'Year 1'
            else:
                row[f'Year {i} Progression'] = np.NaN
    return row

def get_final_prog_status(row):
    cols = [f'Year {i} Progression' for i in list(range(1,8,1))]
    ser = row[cols]
    if any(ser.notnull()):
        final_stat = ser[ser.notnull()][-1]
    else:
        final_stat = np.NaN
    return final_stat

def make_students_dataframe(module_files):
    # create dataframes with SPRcode as index
    student_attempts = pd.DataFrame()
    student_grades = pd.DataFrame()
    student_reassess_flags = pd.DataFrame()
    student_dr_flags = pd.DataFrame()
    student_marks = pd.DataFrame()

    for dfname, df in module_files.items():
        # Add reassess flags (fr and dr)
        module_files[dfname] = add_fr_and_dr_flags(df)
        year, module, tmp = dfname.split('_')
        student_attempts = student_attempts.join(df[['Attempt']], how='outer')
        student_attempts.rename(columns={'Attempt': f'{year} {module}'}, inplace=True)
        student_grades = student_grades.join(df[['Grade']], how='outer')
        student_grades.rename(columns={'Grade': f'{year} {module}'},inplace=True)
        if any(df.columns == 'Mark'):
            student_marks = student_marks.join(df[['Mark']], how='outer')
            student_marks.rename(columns={'Mark':f'{year} {module}'}, inplace=True)
        else:
            student_marks = student_marks.join(df[['Grade']], how='outer')
            student_marks.rename(columns={'Grade':f'{year} {module}'}, inplace=True)
            student_marks[f'{year} {module}'] = np.NaN
        student_reassess_flags = student_reassess_flags.join(df[['FR Flag']], how='outer')
        student_reassess_flags.rename(columns={'FR Flag': f'{year} {module}'},inplace=True)
        student_dr_flags = student_dr_flags.join(df[['DR Flag']], how='outer')
        student_dr_flags.rename(columns={'DR Flag': f'{year} {module}'},inplace=True)
    student_attempts = student_attempts.reindex(sorted(student_attempts.columns), axis=1)#sort by year
    student_zipped = pd.DataFrame(index = student_attempts.index, columns = student_attempts.columns)
    for index in student_attempts.index:
        for column in student_attempts.columns:
            if student_marks.at[index, column] == np.NaN:
                student_zipped.at[index, column] = str(student_reassess_flags.at[index, column]) \
                                                + ' ' + str(student_dr_flags.at[index, column]) \
                                                + ' ' + str(student_attempts.at[index,column]) \
                                                + ' ' + 'nan' + ' ' + str(student_grades.at[index,column])
            else:
                student_zipped.at[index, column] = str(student_reassess_flags.at[index, column]) \
                                                + ' ' + str(student_dr_flags.at[index, column]) \
                                                + ' ' + str(student_attempts.at[index,column]) \
                                                + ' ' + str(student_marks.at[index,column]) \
                                                + ' ' + str(student_grades.at[index,column])
    student_zipped = student_zipped.replace('nan nan nan nan nan', np.NaN)
    records = {}
    for index, row in student_zipped.iterrows():
        courses_on_record = row.dropna().index.tolist()
        results = row.dropna().values.tolist()
        records[index] = list(zip(courses_on_record,results))
    RECORDS = {}
    for SPRcode, record in records.items():
        new_list = []
        for (course, result) in record:
            new_list.append(course + ' ' + result)
        RECORDS[SPRcode] = new_list
    # make dictionaries about student variables for inputting into a future 'STUDENTS' table (clean this up too)
    year_entered = {}
    graduated_year = {}
    status_now = {}
    left_year = {}
    STUDENTS = pd.DataFrame(columns=['Entire Record',
                                     'CONAD History','CONTRACT History', 'LSM History', 'TORT History',
                                     'LAND History', 'CRIMINAL History','EQUITY History', 'EU History',
                                     'LT1 History', 'LT2 History', 'CONAD Attempts', 'CONTRACT Attempts',
                                    'LSM Attempts', 'TORT Attempts', 'LAND Attempts', 'CRIMINAL Attempts',
                                    'EQUITY Attempts', 'EU Attempts', 'LT1 Attempts', 'LT2 Attempts'])
    list_years = ['201112','201213','201314','201415','201516', '201617','201718']
    for SPRcode, list_records in RECORDS.items(): # for all students...
    #     try:
        first_year = list_records[0].split()[0]
        last_year = list_records[-1].split()[0]
        # fill in entire record column
        STUDENTS.at[SPRcode, 'Entire Record'] = list_records
        # if graduauted for students with enough of record in data set
        # does not include students that failed any previous courses but were allowed to progress...
        if (any([((record.split()[1] == 'LT2') & (record.split()[-1] == 'P')) for record in list_records])
            & any([((record.split()[1] == 'LT1') & (record.split()[-1] == 'P')) for record in list_records])
            & any([((record.split()[1] == 'EU') & (record.split()[-1] == 'P')) for record in list_records])
            & any([((record.split()[1] == 'EQUITY') & (record.split()[-1] == 'P')) for record in list_records])
    #         & any([((record.split()[1] == 'LAND') & (record.split()[-1] == 'P')) for record in list_records])
            & any([((record.split()[1] == 'CRIMINAL') & (record.split()[-1] == 'P')) for record in list_records])
            & any([((record.split()[1] == 'TORT') & (record.split()[-1] == 'P')) for record in list_records])
            & any([((record.split()[1] == 'CONAD') & (record.split()[-1] == 'P')) for record in list_records])
            & any([((record.split()[1] == 'CONTRACT') & (record.split()[-1] == 'P')) for record in list_records])):
                    graduated_year[SPRcode] = last_year
                    status_now[SPRcode] = 'Graduated'
        # if left prematurely -> does not include any students that left after 2018
        if not any([(record.split()[0] == '201718') for record in list_records]):
            # not still working on degree in 2018...
            if SPRcode not in status_now:
                # ...and did not graduate
                status_now[SPRcode] = 'Left prematurely - details unknown'
                left_year[SPRcode] = last_year
                if any([((record.split()[-3],record.split()[-1]) == ('3.0', 'F')) for record in list_records if (record.split()[0] == last_year)]):
                    # student eligible for termination
                    status_now[SPRcode] = 'Terminated'
                    left_year[SPRcode] = last_year
                elif any([((record.split()[0] == last_year) & (record.split()[-1]  == 'W')) for record in list_records]):
                    # student withdrawn
                    status_now[SPRcode] = 'Withdrawn'
                    left_year[SPRcode] = last_year
                elif any([((record.split()[0] == last_year) & (record.split()[-1]  == 'F')) for record in list_records]):
                    status_now[SPRcode] = 'Left after failing'
                    left_year[SPRcode] = last_year
                elif all([((record.split()[-1] == 'P')) for record in list_records if (record.split()[0] == last_year)]):
                    status_now[SPRcode] = 'Left in good standing'
                    left_year[SPRcode] = last_year
        # determine current status (progressing or not progressing) if not graduated or left
        if SPRcode not in status_now.keys():
            # determine if all courses passed for 201718
            if any([((record.split()[-3],record.split()[-1]) == ('3.0', 'F')) for record in list_records if (record.split()[0] == last_year)]):
                status_now[SPRcode] = 'Eligible for termination'
                left_year[SPRcode] = last_year
            if any([(record.split()[-1] in ['F','W']) for record in list_records if (record.split()[0] == last_year)]):
                status_now[SPRcode] = 'Not progressing'
            elif any([(record.split()[0] =='201718') for record in list_records]):
                status_now[SPRcode] = 'Progressing'
        # determine year entered if possible
        for year in list_years[1:]:
            if first_year == year:
                if any([f'{first_year} CONAD' in record for record in list_records]):
                    if any([f'{first_year} CONTRACT' in record for record in list_records]):
                        year_entered[SPRcode] = first_year
    # find the PT/FT status of each student
    student_pt_ft_statuses = pd.DataFrame()
    for dfname, df in module_files.items():
        year, module, tmp = dfname.split('_')
        student_pt_ft_statuses = (student_pt_ft_statuses.join(df[['Programme']], how='outer'))
        student_pt_ft_statuses.rename(columns={'Programme': f'{year} {module}'}, inplace=True)
    student_pt_ft_statuses = student_pt_ft_statuses.reindex(sorted(student_pt_ft_statuses.columns), axis=1)#sorted by year
    records = {}
    for index, row in student_pt_ft_statuses.iterrows():
        courses_on_record = row.dropna().index.tolist()
        programmes_listed = row.dropna().values.tolist()
        records[index] = list(zip(courses_on_record,programmes_listed))
    changed_programme = {}
    programmes = {}
    for SPRcode, record in records.items():
        first_programme = record[0][1]
        for (course, programme) in record:
            if programme != first_programme:
                changed_programme[SPRcode] = record
        if SPRcode in changed_programme:
            programmes[SPRcode] = 'Changed'
        else:
            if 'part' in first_programme:
                programmes[SPRcode] = 'PT'
            elif 'full' in first_programme:
                programmes[SPRcode] = 'FT'
            elif '6' in first_programme:
                programmes[SPRcode] = '6YR'
    # Make STUDENTS dataframe
    STUDENTS['Programme'] = pd.Series(programmes)
    STUDENTS['Year started'] = pd.Series(year_entered)
    STUDENTS['Current status'] = pd.Series(status_now)
    STUDENTS['Graduated year'] = pd.Series(graduated_year)
    STUDENTS['Left year from records'] = pd.Series(left_year)
    # STUDENTS['Year left prematurely'] = pd.Series(year_left_prematurely)
    # STUDENTS['Courses prior to leaving prematurely'] = pd.Series(courses_prior_to_leaving_prematurely)
    cols = ['Programme','Year started', 'Current status', 'Graduated year', 'Left year from records', 'Entire Record',
            'LSM History', 'CONTRACT History', 'CONAD History','TORT History',  'CRIMINAL History', 'LAND History',
           'EQUITY History', 'EU History', 'LT1 History', 'LT2 History', 'LSM Attempts', 'CONTRACT Attempts',
                                     'CONAD Attempts', 'TORT Attempts', 'CRIMINAL Attempts', 'LAND Attempts',
                                    'EQUITY Attempts', 'EU Attempts', 'LT1 Attempts', 'LT2 Attempts']
    STUDENTS = STUDENTS[cols]
    # remove students where do not have entire record from when they started
    STUDENTS = STUDENTS[STUDENTS['Year started'].notnull()]
    # determine the last time that the course shows up, and the attempt number of that time
    for name in ['CONAD History','CONTRACT History', 'LSM History', 'TORT History',
                    'LAND History', 'CRIMINAL History','EQUITY History', 'EU History',
                    'LT1 History', 'LT2 History']:
        STUDENTS[name] = np.empty((len(STUDENTS),0)).tolist()
    STUDENTS.apply(record_history, axis=1);
    # Record attempts
    for name in ['LSM Attempts','CONTRACT Attempts','CONAD Attempts', 'TORT Attempts',
        'CRIMINAL Attempts','LAND Attempts', 'EQUITY Attempts', 'EU Attempts',
        'LT1 Attempts', 'LT2 Attempts']:
        STUDENTS[name] = np.empty((len(STUDENTS),0)).tolist()
    for course in ['CONAD','CONTRACT','LSM','TORT','EQUITY','LAND','CRIMINAL','EU','LT1','LT2']:
        for index, row in STUDENTS.iterrows():
            for record in row[f'{course} History']:
                year, course, reassess_flag, dr_flag, attemptnum, mark, result = record.split()
                next_year = year.split('_')[0][:2] + \
                            str(int(year.split('_')[0][2:4]) + 1) + \
                            str(int(year.split('_')[0][4:]) + 1)
                previous_year = year.split('_')[0][:2] + \
                            str(int(year.split('_')[0][2:4]) - 1) + \
                            str(int(year.split('_')[0][4:]) - 1)
                attemptnum = int(float(attemptnum))
                if any(row[f'{course} Attempts']): # any attempts already for this course and student id
                # (NEED TO UNDERSTAND HOW TO MANUALLY INCREMENT WHEN previous attempts and attempt diff < 1) #
                    if type(row[f'{course} Attempts'][-1][-1]) == list:  # if there were multiple previous attempts
                        most_recent_previous_attempt = float(row[f'{course} Attempts'][-1][-1][-1])
                    else: # if there was a single previous attempt
                        most_recent_previous_attempt = float(row[f'{course} Attempts'][-1][-1])
                    attempt_diff = int(float(attemptnum) - most_recent_previous_attempt)
                        #subtract the most recent previous attempt
                    if attempt_diff < 1:
                        previous_year_name = previous_year + '_' + module + '_' + 'assessment'
                        try:
                            previous_year_result = module_files[previous_year_name].at[index,'Result']
                        except:
                            previous_year_result = np.NaN
                        if previous_year_result == 'D':
                            if attemptnum == 1:
                                attemptnum = int(most_recent_previous_attempt)
                            elif (reassess_flag == 'True'):
                                # could add 2 or 3 attempts here, depending whether there is a third assessment the following year
                                print(f'manual attempt increment of {attemptnum}', index, course, year)
                                attemptnum = int(most_recent_previous_attempt) + attemptnum
                        else: # catch where attemptnum doesn't increment and increment manually
                            if reassess_flag == 'True':
                                print(f'manual attempt increment of {attemptnum}', index, course, year)
                                attempt_diff = attemptnum
                                attemptnum = int(most_recent_previous_attempt) + attemptnum
                            else:
                                print('manual attempt increment of 1', index, course, year)
                                attempt_diff = attemptnum
                                attemptnum = int(most_recent_previous_attempt) + 1
                else: # if no previous attempts recorded
                    attempt_diff = int(float(attemptnum))
                    #############################
                if (reassess_flag == 'True'): # if attempt that year included an FR
                    if (attempt_diff == 3): # if the difference between the previous attempt and this one is 3
                        # there is an attempt and a reassessment that year
                        # and another reassessment the next year that isn't recorded there
                        row[f'{course} Attempts'].append((year, ['', ''], [attemptnum-2, attemptnum-1]))
                        row[f'{course} Attempts'].append((next_year, result, attemptnum))
                    elif (attempt_diff == 2):
                        row[f'{course} Attempts'].append((year, ['', result], [attemptnum-1, attemptnum]))
                    elif (attempt_diff == 1):
    #                     print('attempt_diff error', index, record, '*', row[f'{course} History'])
                        row[f'{course} Attempts'].append((year, result, attemptnum+1))
                elif dr_flag == 'True':
                    if (attempt_diff == 3):
                        row[f'{course} Attempts'].append((year, ['' ,''], [attemptnum-2, attemptnum-1]))
                        row[f'{course} Attempts'].append((next_year, result, attemptnum))
                    elif (attempt_diff == 2):
                        row[f'{course} Attempts'].append((year, ['', result], [attemptnum-1, attemptnum]))
                    else:
                        #the DR was not counted by admin but had an effect that should be recorded
                        row[f'{course} Attempts'].append((year, ['', result], [attemptnum, attemptnum]))
                else:
                    row[f'{course} Attempts'].append((year, result, attemptnum))
    # Get last year on record
    STUDENTS['Last year'] = STUDENTS.apply(get_last_year_on_record_from_attempts, axis=1)
    # Get year by year courses and progression status, and final progression status
    for i in range(1,8):
        STUDENTS[f'Year {i} Courses'] = np.empty((len(STUDENTS),0)).tolist()
    for i in range(1,8):
        STUDENTS[f'Year {i} Progression'] = np.NaN
    STUDENTS = STUDENTS.apply(make_year_columns, axis=1)
    STUDENTS = STUDENTS.apply(make_progression_columns, axis=1)
    STUDENTS['Final progression status'] = STUDENTS.apply(get_final_prog_status, axis=1)
    return STUDENTS, module_files

def make_attempts_dataframe(STUDENTS, module_files, n_assessments_module_dict):
    for dfname, df in module_files.items():
        module_files[dfname]['Real Attempt Number(s)'] = np.empty((len(df),0)).tolist()
        module_files[dfname]['Final Real Attempt Number'] = np.empty((len(df),0)).tolist()
    for course in ['CONAD','CONTRACT','LSM','TORT','EQUITY','LAND','CRIMINAL','EU','LT1','LT2']:
        for index, row in STUDENTS.iterrows():
            if any(row[f'{course} Attempts']):
                for record in row[f'{course} Attempts']:
                    year = record[0]
                    if year == '201819':
                        pass
                    else:
                        attempts_that_year = record[-1]
                        (module_files[f'{year}_{course}_assessment']
                        .at[index,'Real Attempt Number(s)']) = attempts_that_year
                        if type(attempts_that_year) == list:
                            if not any(attempts_that_year):
                                print('error: no attempts that year')
                                pass
                            else:
                                (module_files[f'{year}_{course}_assessment']
                                .at[index,'Final Real Attempt Number']) = attempts_that_year[-1]
                        else:
                            (module_files[f'{year}_{course}_assessment']
                            .at[index,'Final Real Attempt Number']) = attempts_that_year
                        # real attempt number is either empty list, integer, or list
    # let's replace the empty lists with np.NaN
    for dfname, df in module_files.items():
        for index, row in df.iterrows():
            if type(row['Real Attempt Number(s)']) == list:
                if not any(row['Real Attempt Number(s)']):
                    module_files[dfname].at[index, 'Real Attempt Number(s)'] = np.NaN
            if type(row['Final Real Attempt Number']) == list:
                if not any(row['Final Real Attempt Number']):
                    module_files[dfname].at[index, 'Final Real Attempt Number'] = np.NaN
    ATTEMPTS = {}
    tuples = []
    for index in STUDENTS.index:
        for course in  ['CONAD', 'CONTRACT', 'LSM', 'TORT', 'LAND', 'CRIMINAL',
        'EQUITY', 'EU', 'LT1', 'LT2']:
            tuples.append((index,course))
    index = pd.MultiIndex.from_tuples(tuples, names=['SPRcode', 'Module'])
    for attempt in ['first', 'second', 'third', 'fourth', 'fifth']:
        ATTEMPTS[f'{attempt}'] = pd.DataFrame(index=index, columns = [
        'Student Programme','Student Final Status', 'Year of Attempt',
        'Attempt Type', 'Module Mark', 'Module Grade', 'Module Result',
        'Asst 1 Grade', 'Asst 1 Result', 'Asst 1 Submit', 'Asst 2 Grade',
        'Asst 2 Result', 'Asst 2 Submit'])
    for dfname, df in module_files.items():
        year, module, tmp = dfname.split('_')
        for index, row in df.iterrows():
            if index in STUDENTS.index:
                previous_year_name = year.split('_')[0][:2] + \
                                str(int(year.split('_')[0][2:4]) - 1) + \
                                str(int(year.split('_')[0][4:]) - 1) + '_' + module + '_' + 'assessment'
                next_year_name = year.split('_')[0][:2] + \
                                str(int(year.split('_')[0][2:4]) + 1) + \
                                str(int(year.split('_')[0][4:]) + 1) + '_' + module + '_' + 'assessment'
                try:
                    previous_year_result = module_files[previous_year_name].at[index,'Result']
                except:
                    previous_year_result = np.NaN
                try:
                    if index in module_files[next_year_name].index:
                        if type(module_files[next_year_name].at[index,'Result']) != str:
                            # this year is a reassessment that was failed
                            second_reassessment_flag = True
                        else:
                            second_reassessment_flag = False
                    else:
                        second_reassessment_flag = False
                except:
                    second_reassessment_flag = False
                #########################################################
                if (type(row['Result']) == str):
                    # if this isn't the second reassessment (which is recorded independently)
                    primary_row = pd.Series()
                    primary_row['Student Programme'] = STUDENTS.at[index,'Programme']
                    primary_row['Student Final Status'] = STUDENTS.at[index, 'Current status']
                    primary_row['Year of Attempt'] = year
                    primary_row['Asst 1 Grade'] = df.at[index,'Assessment 1 Grade']
                    primary_row['Asst 1 Result'] = df.at[index, 'Assessment 1 P or F']
                    if module != 'LSM':
                        if pd.to_numeric(df.at[index, 'Assessment 1 Mark']) > 0:
                            primary_row['Asst 1 Submit'] = True
                        else:
                            primary_row['Asst 1 Submit'] = False
                    else:
                        if df.at[index, 'Assessment 1 Grade'] in ['P', 'LP']:
                            primary_row['Asst 1 Submit'] = True
                        else:
                            primary_row['Asst 1 Submit'] = False
                    if int(n_assessments_module_dict[dfname]) == 2:
                        primary_row['Asst 2 Grade'] = df.at[index, 'Assessment 2 Grade']
                        primary_row['Asst 2 Result'] = df.at[index, 'Assessment 2 P or F']
                        if module != 'LSM':
                            if pd.to_numeric(df.at[index, 'Assessment 2 Mark']) > 0:
                                primary_row['Asst 2 Submit'] = True
                            else:
                                primary_row['Asst 2 Submit'] = False
                        else:
                            if df.at[index, 'Assessment 2 Grade'] in ['P', 'LP']:
                                primary_row['Asst 2 Submit'] = True
                            else:
                                primary_row['Asst 2 Submit'] = False
                    else:
                        primary_row['Asst 2 Grade'] = np.NaN
                        primary_row['Asst 2 Result'] = np.NaN
                        primary_row['Asst 2 Submit'] = np.NaN
                    ####################################################################
                     # if a reassessment
                    ####################################################################
                    if (row['FR Flag'] == True) | (row['DR Flag'] == True): # if the year includes a reassessment
                        if second_reassessment_flag == False:
                            reassessment_row = primary_row.copy() # otherwise will overwrite primary_row!!!
                            reassessment_row['Module Grade'] = row['Grade']
                            reassessment_row['Module Result'] = row['Result']
                            if any(df.columns == 'Mark'):
                                reassessment_row['Module Mark'] = row['Mark']
                            else:
                                reassessment_row['Module Mark'] = np.NaN
                            # set all reassessment asst cols to nan first,
                            # then overwrite them where they can be deduced
                            asst_cols = ['Asst 1 Grade', 'Asst 1 Result',
                            'Asst 1 Submit', 'Asst 2 Grade','Asst 2 Result',
                            'Asst 2 Submit']
                            reassessment_row[asst_cols] = np.NaN
                            if int(n_assessments_module_dict[dfname]) == 1:
                                reassessment_row['Asst 1 Mark'] = \
                                                    reassessment_row['Module Mark']
                                reassessment_row['Asst 1 Result'] = \
                                                    reassessment_row['Module Result'] + ' calc'
                                if reassessment_row['Module Mark'] == 0.0:
                                    reassessment_row['Asst 1 Submit'] = False
                                else:
                                    reassessment_row['Asst 1 Submit'] = True
                            if (int(n_assessments_module_dict[dfname]) == 2):
                                if reassessment_row['Module Mark'] == 0.0:
                                    reassessment_row['Asst 1 Submit'] = False
                                    reassessment_row['Asst 2 Submit'] = False
                                    reassessment_row['Asst 1 Result'] = 'F calc'
                                    reassessment_row['Asst 2 Result'] = 'F calc'
                            # determine attempt type
                            if row['DR Flag'] == True:
                                reassessment_row['Attempt Type'] = 'DR Reassessment'
                            else:
                                reassessment_row['Attempt Type'] = 'FR Reassessment'
                            try:
                                # determine attempt number
                                if type(row['Real Attempt Number(s)']) == list:
                                    reassessment_attempt_num = row['Real Attempt Number(s)'][-1]
                                else:
                                    first_attempt_that_year = row['Real Attempt Number(s)']
                                # write row to appropriate ATTEMPTS table
                                if reassessment_attempt_num == 1: # DR reassessment
                                    ATTEMPTS['second'].loc[(index, module)] = reassessment_row
                                if reassessment_attempt_num == 2:
                                    ATTEMPTS['second'].loc[(index, module)] = reassessment_row
                                if reassessment_attempt_num == 3:
                                    ATTEMPTS['third'].loc[(index,module)] = reassessment_row
                                if reassessment_attempt_num == 4:
                                    ATTEMPTS['fourth'].loc[(index,module)] = reassessment_row
                                if reassessment_attempt_num == 5:
                                    ATTEMPTS['fifth'].loc[(index,module)] = reassessment_row
                            except:
                                # if there is no attempt number (ie started before 2011), then we don't care to write it anyway
                                if type(STUDENTS.at[index,'Year started']) == str:
                                    print(STUDENTS.at[index,'Year started'],'no attempt number', index, module, year)
                                pass
                        elif second_reassessment_flag == True:
                            # will need to make a row each to put in first reassessment and second reassessment
                            first_reassessment_row = primary_row.copy() # do not overwrite primary_row
                            second_reassessment_row = primary_row.copy() # do not overwrite primary_row or first_assessment_row
                            first_reassessment_row['Module Grade'] = np.NaN
                            second_reassessment_row['Module Grade'] = row['Grade']
                            first_reassessment_row['Module Result'] = 'F calc'
                            second_reassessment_row['Module Result'] = row['Result']
                            first_reassessment_row['Module Mark'] = np.NaN
                            if any(df.columns == 'Mark'):
                                second_reassessment_row['Module Mark'] = row['Mark']
                            else:
                                second_reassessment_row['Module Mark'] = np.NaN
                            asst_cols = ['Asst 1 Grade', 'Asst 1 Result',
                            'Asst 1 Submit', 'Asst 2 Grade','Asst 2 Result',
                            'Asst 2 Submit']
                            # set all reassessment asst cols to nan first
                            # then overwrite them where they can be deduced
                            first_reassessment_row[asst_cols] = np.NaN
                            second_reassessment_row[asst_cols] = np.NaN
                            if int(n_assessments_module_dict[dfname]) == 1:
                                first_reassessment_row['Asst 1 Result'] = 'F calc'
                                second_reassessment_row['Asst 1 Mark'] = \
                                            second_reassessment_row['Module Mark']
                                second_reassessment_row['Asst 1 Result'] = \
                                            second_reassessment_row['Module Result'] + ' calc'
                                if second_reassessment_row['Module Mark'] == 0.0:
                                    second_reassessment_row['Asst 1 Submit'] = False
                                else:
                                    second_reassessment_row['Asst 1 Submit'] = True
                            if (int(n_assessments_module_dict[dfname]) == 2):
                                if second_reassessment_row['Module Mark'] == 0.0:
                                    second_reassessment_row['Asst 1 Submit'] = False
                                    second_reassessment_row['Asst 2 Submit'] = False
                                    second_reassessment_row['Asst 1 Result'] = 'F calc'
                                    second_reassessment_row['Asst 2 Result'] = 'F calc'
                            # determine type of attempt
                            if (row['DR Flag'] == True):
                                first_reassessment_row['Attempt Type'] = 'DR Reassessment'
                            else:
                                first_reassessment_row['Attempt Type'] = 'FR Reassessment'
                            second_reassessment_row['Attempt Type'] = 'Reassessment Following Year'
                            # determine attempt number
                            if type(row['Real Attempt Number(s)']) == list:
                                first_reassessment_attempt_num = row['Real Attempt Number(s)'][-1]
                            else:
                                first_reassessment_attempt_num = row['Real Attempt Number(s)']
                            # write rows to appropriate ATTEMPTS table
                            if first_reassessment_attempt_num == 1:
                                print('error with attemptnum = 1 after reassessment',index, module, year)
                            if first_reassessment_attempt_num == 2:
                                ATTEMPTS['second'].loc[(index, module)] = first_reassessment_row
                                ATTEMPTS['third'].loc[(index, module)] = second_reassessment_row
                            if first_reassessment_attempt_num == 3:
                                ATTEMPTS['third'].loc[(index,module)] = first_reassessment_row
                                ATTEMPTS['fourth'].loc[(index, module)] = second_reassessment_row
                            if first_reassessment_attempt_num == 4:
                                ATTEMPTS['fourth'].loc[(index,module)] = first_reassessment_row
                                ATTEMPTS['fifth'].loc[(index, module)] = second_reassessment_row
                            if first_reassessment_attempt_num == 5:
                                ATTEMPTS['fifth'].loc[(index,module)] = first_reassessment_row
                            ####################################################################
                            # put primary rows in correct attempt dfs
                            ####################################################################
                        if not ((pd.to_numeric(year,errors='coerce') > 201314) & (module =='LSM')): #needs testing
                            try:
                                if (int(n_assessments_module_dict[dfname]) == 2): # num assignments
                                    primary_row['Module Mark'] = float((pd.to_numeric(row['Assessment 1 Weight'])
                                                                        * pd.to_numeric(row['Assessment 1 Mark']))
                                                                       + (pd.to_numeric(row['Assessment 2 Weight'])
                                                                        * pd.to_numeric(row['Assessment 2 Mark'])))
                                else:
                                    primary_row['Module Mark'] = row['Assessment 1 Mark']
                                if int(float(pd.to_numeric(primary_row['Module Mark']))) >= 40.0:
                                    primary_row['Module Grade'] = 'P calc'
                                    primary_row['Module Result'] = 'P calc'
                                else:
                                    primary_row['Module Grade'] = 'F calc'
                                    primary_row['Module Result'] = 'F calc'
                            except:
                                print('error in calculating mark',index, module, year, row['Assessment 1 Mark'])
                                # this is a reassessment that is taking the full year
                                # perhaps is being recorded below
                        else: # course is LSM and both assignments must be passed to pass
                            primary_row['Module Mark'] = np.NaN
                            if ((df.at[index,'Assessment 1 Grade'] in ['P','LP']) & (
                                df.at[index,'Assessment 2 Grade'] in ['P','LP'])):
                                primary_row['Module Grade'] = 'P calc'
                                primary_row['Module Result'] = 'P calc'
                            else:
                                primary_row['Module Grade'] = 'F calc'
                                primary_row['Module Result'] = 'F calc'
                    ####################################################################
                     # if no reassessment
                    ####################################################################
                    else: # no reassessment that year
                        primary_row['Module Grade'] = row['Grade']
                        primary_row['Module Result'] = row['Result']
                        if any(df.columns == 'Mark'):
                            primary_row['Module Mark'] = row['Mark']
                        else:
                            primary_row['Module Mark'] = np.NaN
                    ####################################################################
                     # store the first primary row
                    ####################################################################
                    if type(row['Real Attempt Number(s)']) == list:
                        first_attempt_that_year = row['Real Attempt Number(s)'][0]
                    else:
                        first_attempt_that_year = row['Real Attempt Number(s)']
                    if (first_attempt_that_year == 1): # initial assessment
                        if previous_year_result != 'D':
                            primary_row['Attempt Type'] = 'Initial Assessment'
                            ATTEMPTS['first'].loc[(index, module)] = primary_row
                        else:
                            primary_row['Attempt Type'] = 'DR Retake'
                            ATTEMPTS['second'].loc[(index, module)] = primary_row
                    else:# make retake
                        retake_attempt_num = first_attempt_that_year
                        primary_row['Attempt Type'] = 'Retake'
                        if retake_attempt_num == 2:
                            if previous_year_result != 'D':
                                ATTEMPTS['second'].loc[(index, module)] = primary_row
                            else:
                                primary_row['Attempt Type'] = 'DR Retake'
                                ATTEMPTS['third'].loc[(index, module)] = primary_row
                        elif retake_attempt_num == 3:
                            if previous_year_result != 'D':
                                ATTEMPTS['third'].loc[(index,module)] = primary_row
                            else:
                                primary_row['Attempt Type'] = 'DR Retake'
                                ATTEMPTS['fourth'].loc[(index, module)] = primary_row
                        elif retake_attempt_num == 4:
                            if previous_year_result != 'D':
                                ATTEMPTS['fourth'].loc[(index,module)] = primary_row
                            else:
                                primary_row['Attempt Type'] = 'DR Retake'
                                ATTEMPTS['fifth'].loc[(index, module)] = primary_row
                        elif retake_attempt_num == 5:
                            ATTEMPTS['fifth'].loc[(index,module)] = primary_row

    for attempt in ['first', 'second', 'third', 'fourth', 'fifth']:
        ATTEMPTS[f'{attempt}'].dropna(how='all', inplace = True)
        ATTEMPTS[f'{attempt}']['Module Mark'] = pd.to_numeric(ATTEMPTS[f'{attempt}']['Module Mark'])
    return ATTEMPTS, module_files

def create_outcomes_series(df, year_entered):
    outcomes = pd.Index(['Graduated','Failed out','Left in good standing',
                         'Still in program'])
    left_badly = ['Terminated', 'Withdrawn', 'Left after failing',
                  'Left prematurely - details unknown']
    still_in = ['Progressing', 'Not progressing']
    outcomes_ser= pd.Series(index=outcomes)
    selection_total_students =(df['Year started']
                               .str.contains(year_entered))
    selection_graduated_students = (selection_total_students
                                   & (df['Current status']
                                            == 'Graduated'))
    selection_left_badly_students = (selection_total_students
                                    & df['Current status']
                                        .isin(left_badly))
    selection_left_in_good_standing_students = (selection_total_students
                                            & (df['Current status']
                                            == 'Left in good standing'))
    selection_still_in_students = (selection_total_students
                                   & df['Current status']
                                   .isin(still_in))
    outcomes_ser['Graduated'] = (len(df[
                                    selection_graduated_students]))
    outcomes_ser['Failed out'] = (len(df[
                                    selection_left_badly_students]))
    outcomes_ser['Left in good standing'] = (len(df[
                                selection_left_in_good_standing_students]))
    outcomes_ser['Still in program'] = (len(df[
                                            selection_still_in_students]))
    return outcomes_ser

def get_each_year_distribution(df, cohort_ent_yr):
    years = [1,2,3]
    increments = [(year-1) for year in years]
    output_df = (pd.DataFrame(index=pd.Index([f'Year {y}' for y in years]),
                             columns = [f'Year {y} Curriculum'for y in years]))
    for i in increments:
        cutoff = int(cohort_ent_yr[:4]) + i
        selection_total_students = (df['Year started'].
                                    str.contains(cohort_ent_yr))
        year = i + 1
        for y in years:
            output_df.at[f'Year {year}',f'Year {y} Curriculum'] = \
                len(df[selection_total_students & (df[f'Year {year} Progression'] == f'Year {y}')])\
            / len(df[selection_total_students])
    return output_df

def get_year2_distribution(df, cohort_ent_yrs):
    if type(cohort_ent_yrs) == list:
        output_df = pd.DataFrame(index=pd.Index(cohort_ent_yrs))
        for year in cohort_ent_yrs:
            cutoff = int(year[:4]) + 1
            selection_total_students = \
                df['Year started'].str.contains(year)
            selection_still_in_students = \
                selection_total_students \
                    & (df['Last year'] > cutoff)
            selection_repeating_yr_1 = \
                selection_still_in_students \
                    & (df['Year 2 Progression'] == 'Year 1')
            selection_progressed_to_yr_2 = \
                selection_still_in_students \
                    & (df['Year 2 Progression'] == 'Year 2')
            output_df.at[year, 'Repeating Year 1'] = \
                (len(df[selection_repeating_yr_1])
                     / len(df[selection_total_students]))
            output_df.at[year, 'Progressed to Year 2'] = \
                ((len(df[selection_progressed_to_yr_2])
                        / len(df[selection_total_students])))
        return output_df
    else:
        output_ser = pd.Series(index=pd.Index(cohort_ent_yrs))
        cutoff = int(cohort_ent_yrs[:4]) + 1
        selection_total_students = df['Year started'].str.contains(cohort_ent_yr)
        selection_still_in_students = selection_total_students & (df['Last year'] > cutoff)
        selection_repeating_yr_1 = selection_still_in_students & (df['Year 2 Progression'] == 'Year 1')
        selection_progressed_to_yr_2 = selection_still_in_students & (df['Year 2 Progression'] == 'Year 2')
        output_ser['Repeating Year 1'] = len(df[selection_repeating_yr_1]) / len(df[selection_total_students])
        output_ser['Progressed to Year 2'] = (len(df[selection_progressed_to_yr_2]) / len(df[selection_total_students]))
        return output_ser

def get_year3_distribution(df, cohort_ent_yrs):
    output_df = pd.DataFrame(index=pd.Index(cohort_ent_yrs))
    for year in cohort_ent_yrs:
        cutoff = int(year[:4]) + 2
        selection_total_students = \
            df['Year started'].str.contains(year)
        selection_still_in_students = \
            selection_total_students \
                & (df['Last year'] > cutoff)
        selection_repeating_yr_1 = \
            selection_still_in_students \
                & (df['Year 3 Progression'] == 'Year 1')
        selection_repeating_yr_2 = \
            selection_still_in_students \
                & (df['Year 3 Progression'] == 'Year 2')
        selection_progressed_to_yr_3 = \
            selection_still_in_students \
                & (df['Year 3 Progression'] == 'Year 3')
        output_df.at[year, 'Repeating Year 1'] = \
            (len(df[selection_repeating_yr_1])
                 / len(df[selection_total_students]))
        output_df.at[year, 'Repeating Year 2'] = \
            (len(df[selection_repeating_yr_2])
                 / len(df[selection_total_students]))
        output_df.at[year, 'Progressed to Year 3'] = \
            ((len(df[selection_progressed_to_yr_3])
                    / len(df[selection_total_students])))
    return output_df

def plot_single_cohort_progression(df, year_entered):
    x = (get_each_year_distribution(df, year_entered)
        .index.tolist())
    w = 0.4
    distributions = {}
    plots = {}
    for year in [1,2,3]:
        distributions[year] = \
        get_each_year_distribution(df, year_entered)[f'Year {year} Curriculum']
    fig = plt.figure(figsize=(6,5))
    plt.bar(x, distributions[3], width=w, color='0.5')
    plt.bar(x, distributions[2], width=w,
                 bottom=distributions[3], color=['0','0.5', 'salmon'])
    plt.bar(x, distributions[1], width=w,
                 bottom=distributions[2]+distributions[3], color=['0.5','maroon', 'maroon'])
    plt.title(f'{year_entered} Cohort',fontsize=16, fontweight='bold')
    plt.ylabel('Proportion of Entering Cohort', fontweight='bold', fontsize=12)
    plt.xticks(fontsize=12, fontweight='bold')
    plt.yticks(fontsize=12)
    custom_lines = [Line2D([0], [0], color='0.5', lw=8),
                    Line2D([0], [0], color='maroon', lw=8),
                    Line2D([0], [0], color='salmon', lw=8)]
    plt.legend(custom_lines, ['Progressing on Time', 'Repeating Year 1', 'Pursuing Year 2 of Program in Year 3 of Study'],
               fontsize=12,frameon=False, loc=(0.65,0.8))
    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ttl = ax.title
    ttl.set_position([.55, 1.10])
    plt.show()

def plot_all_yrs_progression(df, years_of_interest):
    y = {}
    x = get_each_year_distribution(df, years_of_interest[0]).index.tolist()
    w = 0.4
    for year in years_of_interest:
        for i in [1,2,3]:
            y[(year,i)] = get_each_year_distribution(df, year)[f'Year {i} Curriculum']

    fig, axes = plt.subplots(2, 2, figsize=(8, 6), sharex=True, sharey=True)

    # Set the title for the figure
    fig.suptitle('LLB Progression across Years', fontsize=15)
    fig.text(0.05, 0.5, 'Proportion of entering cohort', fontsize=15,
             ha='center', va='center', rotation=90)
    # Top Left Subplot
    axes[0,0].bar(x, y[('201314',3)], color='0.5', width=w)
    axes[0,0].bar(x, y[('201314',2)], bottom=y[('201314',3)],
                  color=['0.5','0.5', 'salmon'],width=w)
    axes[0,0].bar(x, y[('201314',1)], bottom=y[('201314',3)]+y[('201314',2)],
                  color=['0.5','maroon', 'maroon'],width=w)
    axes[0,0].set_title("2013 Cohort", fontweight='bold')

    # Top Right Subplot
    axes[0,1].bar(x, y[('201415',3)], color='0.5', width=w)
    axes[0,1].bar(x, y[('201415',2)], bottom=y[('201415',3)],
                  color=['0.5','0.5', 'salmon'],width=w)
    axes[0,1].bar(x, y[('201415',1)], bottom=y[('201415',3)]+y[('201415',2)],
                  color=['0.5','maroon', 'maroon'],width=w)
    axes[0,1].set_title("2014 Cohort", fontweight='bold')


    # Bottom Left Subplot
    axes[1,0].bar(x, y[('201516',3)], color='0.5', width=w)
    axes[1,0].bar(x, y[('201516',2)], bottom=y[('201516',3)],
                  color=['0.5','0.5', 'salmon'],width=w)
    axes[1,0].bar(x, y[('201516',1)], bottom=y[('201516',3)]+y[('201516',2)],
                  color=['0.5','maroon', 'maroon'],width=w)
    axes[1,0].set_title("2015 Cohort", fontweight='bold')


    # Bottom Right Subplot
    axes[1,1].bar(x, y[('201617',3)], color='0.5', width=w)
    axes[1,1].bar(x, y[('201617',2)], bottom=y[('201617',3)],
                  color=['0.5','0.5', 'salmon'],width=w)
    axes[1,1].bar(x, y[('201617',1)], bottom=y[('201617',3)]+y[('201617',2)],
                  color=['0.5','maroon', 'maroon'],width=w)
    axes[1,1].set_title("2016 Cohort", fontweight='bold')

    custom_lines = [Line2D([0], [0], color='0.5', lw=8),
                    Line2D([0], [0], color='maroon', lw=8),
                    Line2D([0], [0], color='salmon', lw=8)]
    plt.legend(custom_lines, ['Progressing on Time', 'Repeating Year 1', 'Pursuing Year 2 of Program in Year 3 of Study'],
               fontsize=12,frameon=False, loc=(1.25,1.75))

    plt.show()

def get_prop_failures_yr_1_due_to_NS(first_attempts):
    sel_F_NS_FY_1_asst_crs_PT = ((first_attempts['Module'] == 'CONAD')
                                & (first_attempts['Student Programme'] == 'PT')
                                & first_attempts['Module Result'].isin(['F', 'F calc'])
                                & (first_attempts['Asst 1 Submit'] == False))
    sel_F_NS_FY_1_asst_crs_FT = (first_attempts['Module'].isin(['CONAD', 'TORT'])
                                & (first_attempts['Student Programme'] == 'FT')
                                & first_attempts['Module Result'].isin(['F', 'F calc'])
                                & (first_attempts['Asst 1 Submit'] == False))
    sel_F_NS_FY_2_asst_crs = (first_attempts['Module'].isin(['CONTRACT','LSM'])
                            & first_attempts['Module Result'].isin(['F', 'F calc'])
                            & ((first_attempts['Asst 1 Submit'] == False)
                              | (first_attempts['Asst 2 Submit'] == False)))
    sel_F_NS_any_FY_crs = sel_F_NS_FY_1_asst_crs_PT | sel_F_NS_FY_1_asst_crs_FT | sel_F_NS_FY_2_asst_crs
    sel_F_FY_crs_FT = (first_attempts['Module'].isin(['CONAD', 'TORT', 'CONTRACT', 'LSM'])
                            & (first_attempts['Student Programme'] == 'FT')
                            & first_attempts['Module Result'].isin(['F', 'F calc']))
    sel_F_FY_crs_PT = (first_attempts['Module'].isin(['CONAD', 'CONTRACT', 'LSM'])
                            & (first_attempts['Student Programme'] == 'PT')
                            & first_attempts['Module Result'].isin(['F', 'F calc']))
    sel_F_any_FY_crs = sel_F_FY_crs_FT | sel_F_FY_crs_PT
    proportion_F_FYC_NS = len(first_attempts[sel_F_NS_any_FY_crs]) / len(first_attempts[sel_F_any_FY_crs])
    return proportion_F_FYC_NS

def get_prob_pass_FYC_if_submit(first_attempts):
    sel_S_FY_1_asst_crs_PT = ((first_attempts['Module'] == 'CONAD')
                                & (first_attempts['Student Programme'] == 'PT')
                                & (first_attempts['Asst 1 Submit'] == True))
    sel_S_FY_1_asst_crs_FT = (first_attempts['Module'].isin(['CONAD', 'TORT'])
                                & (first_attempts['Student Programme'] == 'FT')
                                & (first_attempts['Asst 1 Submit'] == True))
    sel_S_FY_2_asst_crs = (first_attempts['Module'].isin(['CONTRACT','LSM'])
                            & ((first_attempts['Asst 1 Submit'] == True)
                              & (first_attempts['Asst 2 Submit'] == True)))
    sel_S_all_asst_FY_crs = sel_S_FY_1_asst_crs_PT | sel_S_FY_1_asst_crs_FT | sel_S_FY_2_asst_crs
    sel_S_all_asst_and_P_FY_crs = sel_S_all_asst_FY_crs & first_attempts['Module Result'].isin(['P', 'LP', 'P calc'])
    proportion_P_FYC_if_submit_all_asst = len(first_attempts[sel_S_all_asst_and_P_FY_crs]) \
                                            / len(first_attempts[sel_S_all_asst_FY_crs])
    return proportion_P_FYC_if_submit_all_asst

def make_module_summary(df,dfname,n_assessments):
    '''
    Takes a pandas dataframe 'module file', its name and its
    number of assessments.
    The module file has been modified from its original form in
    excel in the following ways:
    
    Returns a pandas Series of calculated values to
    input into a module summary table.
    '''
    year, module, tmp = dfname.split('_')
    n_total = len(df.index.unique())
    n_attend = sum(df['Attempt type'].isin(['Assessment', 'DR Retake', 'Retake']))
    # Make series for populating with calculated values
    ser = pd.Series()
    #####################################################################
    # Calculate general passing stats
    #####################################################################
    ser['N (total)'] = n_total
    ser['N (enrolled)'] = n_attend
    ser['% Pass (after reassessment same year)'] = (100 *
                        sum((df['Result']=='P')
                            & (df['Attempt type'] !=
                            'FR reassessment from previous year'))
                            / n_attend)
    ser['% Pass (first attempt that year)'] = (100 *
                        sum(df['Attempt type'].isin(['Assessment', 'Retake',
                                                     'DR Retake'])
                            & (df['Result'] == 'P'))
                            / n_attend)
    ser['% Pass (first attempt ever)'] = (100 *
                        sum((df['Attempt'] == 1.0)
                            & (df['Attempt type'] == 'Assessment')
                            & (df['Result'] == 'P'))
                        / sum((df['Attempt'] == 1.0)
                              & (df['Attempt type'] == 'Assessment')))
    ser['% Pass (all assessed that year)'] = (100 *
                        sum(df['Result']=='P') / n_total)
    ser['% Pass (after reassessment next year)'] = (100 *
                            sum((df['Final Result on Worksheet']=='P')
                                & df['Attempt type'].isin(['Assessment',
                                                            'DR Retake',
                                                            'Retake']))
                                / n_attend)
    #####################################################################
    # Calculate retake stats
    #####################################################################
    # All retakes
    if year != '201718':
        ser['% Retake Next Year'] = (100 * sum((df['Retake next year']
                                                .str.startswith('Y') == True)
                                        & df['Assessment 1 Grade'].notnull())
                                     / n_total)
        # If any students retook...
        if sum((df['Retake next year'].str.startswith('Y') == True)
              & df['Assessment 1 Grade'].notnull()) != 0:
            # Calculate the proportion that passed the retake
            ser['% of Retake that Pass'] = \
                                (100 * sum((df['Retake and pass'] == True)
                                            & df['Assessment 1 Grade'].notnull())
                        / sum((df['Retake next year'].str.startswith('Y') == True)
                            & df['Assessment 1 Grade'].notnull()))
        else: # If no students retook
            ser['% of Retake that Pass'] = np.NaN
    else: # If the year is 2017/18, set all retake variables to nan,
          # because there is no information about retakes (no 2019 sheets)
        ser['% Retake Next Year'] = np.NaN
        ser['% of Retake that Pass'] = np.NaN
    #####################################################################
    # Calculate general reassessment stats
    #####################################################################
    ser['% Reassess (Either Asst FR or DR)'] = \
        (100 * sum(df['Attempt type']
                    .isin(['FR reassessment', 'DR reassessment']))
               / n_attend)
    ser['% Reassess (Either Asst FR or DR) that Pass'] = \
        (100 * sum(df['Attempt type']
                    .isin(['FR reassessment', 'DR reassessment'])
                    & (df['Result'] == 'P'))
                / sum(df['Attempt type']
                        .isin(['FR reassessment', 'DR reassessment'])))
    # Calculate FR reassessment stats
    ser['% FR Reassess'] = \
        (100 * sum(df['Attempt type'] == 'FR reassessment')
               / n_attend)
    ser['% FR Reassess that Pass'] = \
        (100 * sum((df['Attempt type'] == 'FR reassessment')
                    & (df['Result'] == 'P'))
                / sum(df['Attempt type'] == 'FR reassessment'))
    # Calculate DR reassessment stats
    ser['% DR Reassess'] = \
        (100 * sum(df['Attempt type'] == 'DR reassessment')
               / n_attend)
    ser['% DR Reassess that Pass'] = \
        (100 * sum((df['Attempt type'] == 'DR reassessment')
                    & (df['Result'] == 'P'))
                / sum(df['Attempt type'] == 'DR reassessment'))
    # Calculate second reassessment stats
    if sum(df['Reassess next year'] == True) != 0:
        ser['% Second Reassess'] = (100 *
                            sum(df['Reassess next year'] == True)/ n_attend)
        ser['% Second Reassess that Pass'] = (100 *
                            sum((df['Reassess next year'] == True)
                               & (df['Reassess next year and pass'] == True))
                            / sum(df['Reassess next year'] == True))
    #####################################################################
    # Calculate general submission stats
    #####################################################################
    # For modules with one assessment
    if float(n_assessments) == 1.0:
        ser['% Submit all assignments'] = (100 *
                                        sum(df['Attempt type'].isin(
                                        ['Assessment', 'Retake', 'DR Retake'])
                                        & (df['Assessment 1 Mark'] != 0.0))
                                         / sum(df['Attempt type'].isin(
                                         ['Assessment', 'Retake', 'DR Retake'])))
        ser['% Submit all assignments that pass'] = (100 *
                                        sum(df['Attempt type'].isin(
                                        ['Assessment', 'Retake', 'DR Retake'])
                                        & (df['Assessment 1 Mark'] != 0.0)
                                        & (df['Result'] == 'P'))
                                         / sum(df['Attempt type'].isin(
                                         ['Assessment', 'Retake', 'DR Retake'])
                                         & (df['Assessment 1 Mark'] != 0.0)))
        ser['% Submit one of two assignments'] = np.nan
        ser['% Submit no assignments'] = (100 *
                                        sum(df['Attempt type'].isin(
                                        ['Assessment', 'Retake', 'DR Retake'])
                                        & (df['Assessment 1 Mark'] == 0.0))
                                          / sum(df['Attempt type'].isin(
                                          ['Assessment', 'Retake', 'DR Retake'])))

    # For modules with two assessments
    elif float(n_assessments) == 2.0:
        if module != 'LSM':
            ser['% Submit all assignments'] = (100 *
                            sum(df['Attempt type'].isin(
                            ['Assessment', 'Retake', 'DR Retake'])
                            & (df['Assessment 1 Mark'].astype('float') != 0.0)
                            & (df['Assessment 2 Mark'].astype('float') != 0.0))
                              / sum(df['Attempt type'].isin(
                              ['Assessment', 'Retake', 'DR Retake'])))
            ser['% Submit all assignments that pass'] = (100 *
                            sum(df['Attempt type'].isin(
                            ['Assessment', 'Retake', 'DR Retake'])
                            & (df['Assessment 1 Mark'].astype('float') != 0.0)
                            & (df['Assessment 2 Mark'].astype('float') != 0.0)
                            & (df['Result'] == 'P'))
                         / sum(df['Attempt type'].isin(
                            ['Assessment', 'Retake', 'DR Retake'])
                            & (df['Assessment 1 Mark'].astype('float') != 0.0)
                            & (df['Assessment 2 Mark'].astype('float') != 0.0)))
            ser['% Submit one of two assignments'] = (100 *
                            sum(df['Attempt type'].isin(
                            ['Assessment', 'Retake', 'DR Retake'])
                             & (((df['Assessment 1 Mark'].astype('float') != 0.0)
                             & (df['Assessment 2 Mark'].astype('float') == 0.0))
                             | ((df['Assessment 1 Mark'].astype('float') == 0.0)
                              & (df['Assessment 2 Mark'].astype('float') != 0.0))))
                          / sum(df['Attempt type'].isin(
                          ['Assessment', 'Retake', 'DR Retake'])))
            ser['% Submit no assignments'] = (100 *
                            sum(df['Attempt type'].isin(
                            ['Assessment', 'Retake', 'DR Retake'])
                            & (df['Assessment 1 Mark'].astype('float') == 0.0)
                            & (df['Assessment 2 Mark'].astype('float') == 0.0))
                          / sum(df['Attempt type'].isin(
                          ['Assessment', 'Retake', 'DR Retake'])))
        else: # module is LSM
            ser['% Submit all assignments'] = (100 *
                            sum(df['Attempt type'].isin(
                            ['Assessment', 'Retake', 'DR Retake'])
                            & (df['Assessment 1 P or F'] == 'P')
                            & (df['Assessment 2 P or F'] == 'P'))
                          / sum(df['Attempt type'].isin(
                          ['Assessment', 'Retake', 'DR Retake'])))
            ser['% Submit all assignments that pass'] = np.NaN
            ser['% Submit one of two assignments'] = (100 *
                                            sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                             & (((df['Assessment 1 P or F'] == 'P')
                                             & (df['Assessment 2 P or F'] != 'P'))
                                             | ((df['Assessment 1 P or F'] != 'P')
                                              & (df['Assessment 2 P or F'] == 'P'))))
                                          / sum(df['Attempt type'].isin(
                                          ['Assessment', 'Retake', 'DR Retake'])))
            ser['% Submit no assignments'] = (100 *
                                            sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Assessment 1 P or F'] != 'P')
                                            & (df['Assessment 2 P or F'] != 'P'))
                                          / sum(df['Attempt type'].isin(
                                          ['Assessment', 'Retake', 'DR Retake'])))

    #####################################################################
    # Calculate failure submission stats
    #####################################################################
    # if a student fails an attempt (result not P), how many assignments
    # did they submit?
    # For modules with one assessment
    if float(n_assessments) == 1.0:
        ser['% Fail that submit all assignments'] = (
                                    100 *
                                    sum(df['Attempt type'].isin(
                                        ['Assessment', 'Retake', 'DR Retake'])
                                        & (df['Result'] != 'P')
                                        & (df['Assessment 1 Mark'] != 0.0))
                                    / sum(df['Attempt type'].isin(
                                        ['Assessment', 'Retake', 'DR Retake'])
                                        & (df['Result'] != 'P')))
        ser['% Fail that submit one of two assignments'] = np.NaN
        ser['% Fail that submit no assignments'] = (
                                    100 *
                                    sum(df['Attempt type'].isin(
                                        ['Assessment', 'Retake', 'DR Retake'])
                                        & (df['Result'] != 'P')
                                        & (df['Assessment 1 Mark'] == 0.0))
                                    / sum(df['Attempt type'].isin(
                                        ['Assessment', 'Retake', 'DR Retake'])
                                        & (df['Result'] != 'P')))
    elif float(n_assessments) == 2.0:
        if module != 'LSM':
            ser['% Fail that submit all assignments'] = (
                                        100 *
                                        sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')
                                            & (df['Assessment 1 Mark'] != 0.0)
                                            & (df['Assessment 2 Mark'] != 0.0))
                                        / sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')))
            ser['% Fail that submit one of two assignments'] = (
                                        100 *
                                        sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')
                                            & (((df['Assessment 1 Mark'] == 0.0)
                                            & (df['Assessment 2 Mark'] != 0.0))
                                            | ((df['Assessment 1 Mark'] != 0.0)
                                            & (df['Assessment 2 Mark'] == 0.0))))
                                        / sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')))
            ser['% Fail that submit no assignments'] = (
                                        100 *
                                        sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')
                                            & (df['Assessment 1 Mark'] == 0.0)
                                            & (df['Assessment 2 Mark'] == 0.0))
                                        / sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')))
        else: # module is LSM
            ser['% Fail that submit all assignments'] = (
                                        100 *
                                        sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')
                                            & (df['Assessment 1 Grade'] == 'P')
                                            & (df['Assessment 2 Grade'] == 'P'))
                                        / sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')))
            ser['% Fail that submit one of two assignments'] = (
                                        100 *
                                        sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')
                                            & (((df['Assessment 1 Grade'] != 'P')
                                            & (df['Assessment 2 Grade'] == 'P'))
                                            | ((df['Assessment 1 Grade'] == 'P')
                                            & (df['Assessment 2 Grade'] != 'P'))))
                                        / sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')))
            ser['% Fail that submit no assignments'] = (
                                        100 *
                                        sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')
                                            & (df['Assessment 1 Grade'] != 'P')
                                            & (df['Assessment 2 Grade'] != 'P'))
                                        / sum(df['Attempt type'].isin(
                                            ['Assessment', 'Retake', 'DR Retake'])
                                            & (df['Result'] != 'P')))
    #####################################################################
    # Calculate Assessment stats - only possible for first attempt that yr
    #####################################################################
    ser['Asst. 1: % Pass'] = (100 * (sum(df['Assessment 1 P or F'] == 'P')) / n_attend)
    ser['Asst. 1: % F (Not DR or FR)'] = \
                (100 * sum(df['Assessment 1 Grade'] == 'F') / n_attend)
    ser['Asst. 1: % W'] = (100 * sum(df['Assessment 1 Grade'] == 'W') / n_attend)
    ser['Asst. 1: % FR'] = (100 * sum(df['Assessment 1 Grade'] == 'FR') / n_attend)
    ser['Asst. 1: % DR'] = (100 * sum(df['Assessment 1 Grade'] == 'DR') / n_attend)
    ser['Asst. 1: % Blank'] = (100 * sum(df['Assessment 1 Grade'].isnull()) / n_attend)
    ser['Asst. 1: % LF'] = (100 * sum(df['Assessment 1 Grade'] == 'LF') / n_attend)
    if module != 'LSM':
        ser['Asst. 1: % No Sub'] = (100 * sum(pd.to_numeric(df['Assessment 1 Mark'],
                                    errors='coerce') == 0.0 )/ n_attend)
        ser['Asst. 1: % Failed Sub'] = \
            (100 * sum((pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') > 0.0)
                     & (pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') < 40.0))
                    / n_attend)
        ser['Asst. 1 F: % No Sub'] = \
            (100 * sum((pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') == 0.0)
                        & (df['Assessment 1 P or F'] == 'F'))
                    / sum(df['Assessment 1 P or F'] == 'F'))
        ser['Asst. 1 F: % Failed Sub'] = \
            (100 * sum((pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') != 0.0)
                        & (df['Assessment 1 P or F'] == 'F'))
                  / sum(df['Assessment 1 P or F'] == 'F'))
        ser['Asst. 1: % Submit that Pass'] = \
            (100 * sum((pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') > 0.0)
                        & (df['Assessment 1 P or F'] == 'P'))
                / sum(pd.to_numeric(df['Assessment 1 Mark'], errors='coerce') > 0.0))
    else:
        ser['Asst. 1: % No Sub'] = np.NaN
        ser['Asst. 1: % Failed Sub'] = np.NaN
        ser['Asst. 1 F: % No Sub'] = np.NaN
        ser['Asst. 1 F: % Failed Sub'] = np.NaN
        ser['Asst. 1: % Submit that Pass'] = np.NaN
    #####################################################################
    # Add stats for second assessment if applicable
    #####################################################################
    if float(n_assessments) == 2.0:
        ser['Asst. 2: % Pass'] = (100 * (sum(df['Assessment 2 P or F'] == 'P')) / n_attend)
        ser['Asst. 2: % F (Not DR or FR)'] = (100 * sum(df['Assessment 2 Grade'] == 'F') / n_total)
        ser['Asst. 2: % W'] = (100 * sum(df['Assessment 2 Grade'] == 'W') / n_attend)
        ser['Asst. 2: % FR'] = (100 * sum(df['Assessment 2 Grade'] == 'FR') / n_attend)
        ser['Asst. 2: % DR'] = (100 * sum(df['Assessment 2 Grade'] == 'DR') / n_attend)
        ser['Asst. 2: % Blank'] = (100 * sum(df['Assessment 2 Grade'].isnull()) / n_attend)
        ser['Asst. 2: % LF'] = (100 * sum(df['Assessment 2 Grade'] == 'LF') / n_attend)
        if module != 'LSM':
            ser['Asst. 2: % No Sub'] = \
              (100 * sum(pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') == 0.0 )
               / n_attend)
            ser['Asst. 2: % Failed Sub'] = \
              (100 * sum((pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') > 0.0)
                       & (pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') < 40.0))
                / n_attend)
            ser['Asst. 2 F: % No Sub'] = \
              (100 * sum((pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') == 0.0)
                & (df['Assessment 2 P or F'] == 'F'))
                / sum(df['Assessment 2 P or F'] == 'F'))
            ser['Asst. 2 F: % Failed Sub'] = \
              (100 * sum((pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') != 0.0)
                       & (df['Assessment 2 P or F'] == 'F'))
                  / sum(df['Assessment 2 P or F'] == 'F'))
            ser['Asst. 2: % Submit that Pass'] = \
               (100 * sum((pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') > 0.0)
                        & (df['Assessment 2 P or F'] == 'P'))
                    / sum(pd.to_numeric(df['Assessment 2 Mark'], errors='coerce') > 0.0))
        else:
            ser['Asst. 2: % No Sub'] = np.NaN
            ser['Asst. 2: % Failed Sub'] = np.NaN
            ser['Asst. 2 F: % No Sub'] = np.NaN
            ser['Asst. 2 F: % Failed Sub'] = np.NaN
            ser['Asst. 2: % Submit that Pass'] = np.NaN
        if sum(df['Assessment 2 Grade'] == 'FR') != 0:
            ser['Asst. 2: % FR that Pass'] = \
                    (100 * sum((df['Assessment 2 Grade'] == 'FR')
                              & (pd.to_numeric(
                                  df['Assessment 1 Mark'],errors='coerce')
                                 + pd.to_numeric(
                                     df['Assessment 2 Mark'],errors='coerce')
                                 >= 80.0))
                    / sum(df['Assessment 2 Grade'] == 'FR'))
        try:
            ser['Asst. 2: % DR that Pass'] = \
                    (100 * sum((df['Assessment 2 Grade'] == 'DR')
                              & (pd.to_numeric(
                                  df['Assessment 1 Mark'],errors='coerce')
                                 + pd.to_numeric(
                                     df['Assessment 2 Mark'],errors='coerce')
                                 >= 80.0))
                    / sum(df['Assessment 2 Grade'] == 'DR'))
        except:
            print(year, module + "ser['Asst. 2: % DR that Pass']")
    #####################################################################
    # return the series
    #####################################################################
    ser = ser.T
    return ser
