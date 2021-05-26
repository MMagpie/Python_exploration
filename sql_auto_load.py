# -*- coding: utf-8 -*-
"""
Created on Tue May 25 17:16:23 2021

@author: MMagpie
"""

def val_choice(val_seq, sidestep=42):
    '''single variable choice w/ default value'''
    dict_type_str = "<class 'dict'>"
    str_type_str = "<class 'str'>"
    input_str = 'Enter Choice (or hit "Enter" for "{}")\n> '
    print('\nValues to choose from: {}'.format(val_seq))
    new_dict = {}
    
    '''allow option for auto entry'''
    if sidestep == 42:
        print('temporary sidestep variable set to override manual input')
    
    '''set list of optional values'''
    if str(type(val_seq)) == dict_type_str:
        for key, value in list(val_seq.items()):
            new_dict[str(value)] = key
        deflt_val_name = min(list(new_dict))
        if sidestep == 42: dval = deflt_val_name
        else: dval = input(input_str.format(deflt_val_name))
        if dval == '': val = new_dict[deflt_val_name]
        else: val = new_dict[dval]
    elif str(type(val_seq)) == str_type_str:
        deflt_val_name = val_seq
        if sidestep == 42: val = deflt_val_name
        else: val = input(input_str.format(deflt_val_name))
        if val == '': val = deflt_val_name
    else:
        deflt_val_name = val_seq[0]
        if sidestep == 42: val = deflt_val_name
        else: val = input(input_str.format(deflt_val_name))
        if val == '': val = deflt_val_name
    return val


'''-------------------------------------------------------------------------'''
def sqtbl_create_auto(file_name1, db_name1, tbl_name1):
    '''define fx to create and auto-fill a new SQLite database''' 
    
    import pandas as pd, sqlite3 as sql, re

    '''make exceptions for different formatting between python and SQLite object types'''
    pd_timestamp_type_str = "<class 'pandas._libs.tslibs.timestamps.Timestamp'>"
    pd_datetimes_type_str = "<class 'datetime.datetime'>"
        
    '''dict to match pandas & SQLite object types'''
    type_conver = {"<class 'int'>": 'integer',
                   "<class 'str'>": 'text',
                   "<class 'float'>": 'real',
                   pd_timestamp_type_str: 'text',
                   pd_datetimes_type_str: 'text',
                   "<class 'pandas._libs.tslibs.nattype.NaTType'>": 'text',
                   "other": 'text',
                   }
    
    '''key word reference in case import table uses "index" attribute; others can be manually added'''
    kw_conver = {'index': 'composite_ref',
                 }
    '''load Excel file to varible and collect sheet names for value list'''
    xl = pd.ExcelFile(file_name1)
    names_list = xl.sheet_names
    sheet_names_list = val_choice(names_list, sidestep=99)
    
    '''create data frame 1: excel file. set initial column name list'''
    df1 = pd.read_excel(file_name1, sheet_names_list)
    array_cols_list = list(df1.columns)
    
    '''remove spaces and problematic text from column names'''
    array_cols_list = [x.lower().replace('.', '_').replace(' ', '_').replace(':', '') for x in array_cols_list]
    
    '''replace "index" keyword in column names'''
    col_index = 0
    for col_val in array_cols_list:
        if col_val in kw_conver:
            array_cols_list[col_index] = kw_conver[col_val]
        col_index += 1
    
    '''"copy" column list'''
    array_assigns_list = array_cols_list[:]
    array_len = len(array_cols_list)
    vals_list = df1.values.tolist()
    db_insert_tup = tuple(array_cols_list)
    col_vals_list = vals_list[0]
    
    '''create type list for matching python to SQLite'''
    types_list = []
    for col_val in col_vals_list:
        col_type = str(type(col_val))
        try:
            types_list.append(type_conver[col_type].upper())
        except:
            types_list.append(type_conver['other'].upper())
    
    '''convert types from python to SQLite'''
    err_vals_list = []
    vals_tups_list = []
    for array in vals_list:
        array_start = 0
        for val in array:
            if type_conver[str(type(val))].lower() != types_list[array_start].lower():
                err_vals_list.append((val, str(type(val)), type_conver[str(type(val))], types_list[array_start]))
            if str(type(val)) == pd_timestamp_type_str:
                array[array_start] = "".join(re.findall('(\S*) ', str(val)))
            array_start += 1
        vals_tups_list.append(tuple(array))
        
    '''set formatting variables to auto-create table and load values into attributes'''
    tbl_create_base_str = ('CREATE TABLE IF NOT EXISTS '
                           + tbl_name1
                           + '('
                           + ('%s %s, ' * (array_len - 1))
                           + '%s %s'
                           + ')'
                           )
    tbl_insert_base_str = ('INSERT INTO '
                           + tbl_name1
                           + '('
                           + ('%s, ' * (array_len - 1))
                           + '%s'
                           + ') '
                           + 'VALUES('
                           + ('?, ' * (array_len - 1))
                           + '?'
                           + ')'
                           )
    
    '''set SQLite statements for creating table and inserting data'''
    col_index = 1
    for type_a in types_list:
        array_assigns_list.insert(col_index, type_a)
        col_index += 2
    db_create_tup = tuple(array_assigns_list)
    tbl_create_str = tbl_create_base_str % db_create_tup
    print('\ntable create statement:\n{}'.format(tbl_create_str))
    tbl_insert_str = tbl_insert_base_str % db_insert_tup
    print('\ntable insert statement:\n{}'.format(tbl_insert_str))
    
    
    '''connect to SQL and create db'''
    db_init = sql.connect(db_name1)
    db_cur = db_init.cursor()
    
    '''drop existing tables and create new'''
    tbl_drop_str = 'DROP TABLE IF EXISTS {}'.format(tbl_name1)
    db_cur.execute(tbl_drop_str)
    db_cur.execute(tbl_create_str)
    
    '''insert rows into new table'''
    arr_count = 0
    try_count = 0
    except_count = 0
    for array in vals_tups_list:
        arr_count += 1
        try:
            db_cur.execute(tbl_insert_str, array)
            if arr_count % 50 == 0:
                db_init.commit()
            try_count += 1
        except:
            print('Invalid array value: {}\n{}\n'.format(tbl_create_str, array))
            except_count += 1
    
    db_init.commit()
    print('\nValid row count = {}\nInvalid row count = {}'.format(try_count, except_count))
    db_cur.execute('SELECT * FROM {}'.format(tbl_name1))
    '''for row in db_cur:
        print(row)'''
    db_cur.close()
    db_init.close()
    return array_cols_list