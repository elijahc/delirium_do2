import os
import pandas as pd
import numpy as np
import pyarrow
import pyarrow.parquet as pq
import pyarrow.csv as csv
import tarfile
import hashlib

GENDER_MAP = {1:'Male',2:'Female'}
RACE_MAP = {
    1:'American Indian and Alaska Native',
    2:'Asian',
    3:'Black',
    4:'White',
    5:'Hawaiian or Pacific Islander',
    6:'Multiracial',
    7:'Other',
    99:'Unknown',
}

def sha1_hash_integers(integer_list):
    """
    Computes the SHA-1 hash of a series of integers.

    Args:
        integer_list (list): A list of integers to be hashed.

    Returns:
        str: The SHA-1 hash digest as a hexadecimal string.
    """
    hasher = hashlib.sha1()
    
    for num in integer_list:
        # Convert each integer to bytes.
        # 'big' endian byte order is common for hashing, and 4 bytes for a standard int.
        # Adjust byte length as needed based on the expected range of your integers.
        num_bytes = num.to_bytes((num.bit_length() + 7) // 8 or 1, 'big')
        hasher.update(num_bytes)
    return hasher.hexdigest()

def pid_hash(pids, issorted=False, truncate=10):
    
    if not isinstance(pids, list):
        pids = pids.tolist()
    
    if not issorted:
        print('sorting pids; if already sorted set issorted=True')
        pids = sorted(pids)

    out = sha1_hash_integers(pids)

    if isinstance(truncate, int):
        truncate = min(truncate, len(out))
        return out[:truncate]
    else:
        return out


def unpack(tar_fp,data_root):
    file = tarfile.open(tar_fp)

    file.extractall(data_root)
    file.close()

def check_and_load(fp,load_func=csv.read_csv,**kwargs):
    if os.path.exists(fp):
        print('Loading file: \n','\t{}'.format(fp))
        return load_func(fp,**kwargs)
    else:
        print('File not found: \n',fp)
        raise IOError

check_and_load_csv = check_and_load

def load_table(data_dir,fn,load_func=csv.read_csv,**kwargs):
    fp = os.path.join(data_dir,fn)
    
    return check_and_load_csv(fp,load_func,**kwargs)

def search(q,series):
    idxs = [q in c for c in series.values]
    return series[idxs]

def rebin_time(df, on=None, time_column='time'):
    accepted_binnings = ['hour','q4h','q8h','q12h','day']
    divisors = [1,4,8,12,24]
    assert on in accepted_binnings, 'binning must be one of {}'.format(accepted_binnings)
    on_map = {k:v for k,v in zip(accepted_binnings,divisors)}
    
    t = df['time'] / np.timedelta64(1,'D') * 24 / on_map[on]

    df['btime'] = t.apply(np.rint) * on_map[on]
    return df.copy()

def tidy_labs(df, hours=False):
    
    get_days = lambda d: pd.to_timedelta(d.lab_collection_days_since_birth-d.lab_collection_days_since_birth.min(),unit='day')

    df = df.rename(columns={'lab_component_name':'name','lab_result_value':'value','lab_collection_time':'time'})
    df.value = pd.to_numeric(df.value,errors='coerce')
    
    days = df.lab_collection_days_since_birth.apply(lambda s: pd.to_timedelta(s,unit='day'))
    
    df.time = pd.to_timedelta(df.time) + days
    df = df.dropna()
    return df[['encounter_id','time','name','value']].sort_values(['encounter_id','time'],ascending=True)

def tidy_flow(df,to_numeric=True):
    get_days = lambda d: pd.to_timedelta(d.flowsheet_days_since_birth-d.flowsheet_days_since_birth.min(),unit='day')

    days = df.flowsheet_days_since_birth.apply(lambda s: pd.to_timedelta(s,unit='day'))
    
    df = df.rename(columns={'display_name':'name','flowsheet_value':'value','flowsheet_time':'time'})
    df.time = pd.to_timedelta(df.time.astype(str)) + days
    df = df.dropna()

    if to_numeric:
        df['value'] = pd.to_numeric(df['value'],errors='coerce')
    
    return df[['encounter_id','time','name','value']].sort_values(['encounter_id','time'],ascending=True)

def tidy_meds(df):
    df = df.rename(columns={'medication_name':'name','dose':'value','administered_time':'time'})

    df.value = pd.to_numeric(df.value,errors='coerce')
    df.encounter_id = pd.to_numeric(df.encounter_id,errors='coerce')
    
    df.administered_days_since_birth = pd.to_numeric(df.administered_days_since_birth, errors='coerce')
    
    days = df.administered_days_since_birth.apply(lambda s: pd.to_timedelta(s,unit='day'))
    
    df.time = pd.to_timedelta(df.time) + days
    df = df.dropna()
    return df[['encounter_id','time','name','value']].sort_values(['encounter_id','time'],ascending=True)

def tidy_procs(df,t='time'):
    df = df.rename(columns={'order_name':'name', 'days_from_dob_procstart':'time'})

    df.encounter_id = pd.to_numeric(df.encounter_id,errors='coerce')
    df['value'] = df.time.astype(int)
    df.time = pd.to_numeric(df.time,errors='coerce')
    df.time = pd.to_timedelta(df.time,unit='day')
    
    df = df.dropna()
    return df[['encounter_id','time','name','value']].sort_values(['encounter_id','time'],ascending=True)

def pivot_tidy(df,t='time',values='value', aggfunc='mean'):
    by = 'encounter_id'
    if not 'encounter_id' in df.columns and 'person_id' in df.columns:
        by = 'person_id'
    return df.pivot_table(index=[by,t],values=values, aggfunc=aggfunc, columns='name')

def melt_tidy(df,t='hour',by='encounter_id'):
    return pd.melt(df.reset_index(),id_vars=[by,t],value_vars=df.reset_index().columns.tolist(),var_name='name')

def decode_gender(df, column='gender'):
    df[column] = df[column].replace(GENDER_MAP)
    return df

def decode_race(df, column='race'):
    df[column] = df[column].replace(RACE_MAP)
    return df

def ddb_to_pandas(table, t_start=None, t_end=None):
    tab = table.to_pandas()

    if 'time' in tab.columns:
        tab.time = pd.to_timedelta(tab.time)
    if t_start is not None:
        tab = tab[tab.time > pd.Timedelta(hours=int(t_start))]
    if t_end is not None:
        tab = tab[tab.time <= pd.Timedelta(hours=int(t_end))]
    return tab