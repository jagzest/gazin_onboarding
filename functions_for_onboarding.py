import pandas as pd
import model_engine
import boto3
import  numpy as np
def list_s3_files(bucket_name, prefix):
    ## initalize session s using amazon s3
    s3 = boto3.client('s3')

    # list objects with bucekt and prefix

    response  = s3.list_objects_v2(Bucket = bucket_name, Prefix = prefix)

    if 'Contents' in response:
        files = [item['Key'] for item in response['Contents']]
        fixed_list = []
        for file in files:
            fixed_list.append(f"s3://{bucket_name}/{file}")
        return fixed_list
            
    else:
        return []

def load_df_from_list(list, number):
    
    df = pd.read_parquet(list[number])
    return df
def clean_up_date_column(df, date_col):
    df = df.copy()
    
    s = df[date_col].astype('string')
    ## .strip removes whitespace 
    ## \. tells us look for any character that is followed by .0 at the end we remove 
    ## . means anything in regex so need to use backslash to look for it 
    # \D looks for all character that is not a digit and repalce it with nothing
    x = (s.str.strip().str.replace(r"\.0$", "", regex = True) # remove trailing 0 
    .str.replace(r"\D", "", regex = True))
    # zfill makes sure each one is 8 digits long and adds 0 to the left side until it has 8
    x = x.str.zfill(8)
    df[date_col] =  pd.to_datetime(x, format = "%m%d%Y", errors = "coerce").dt.normalize()
    return df.sort_values(by = date_col, ascending = True)

def build_final_inquiry_features(
    df, 
    key_col = 'ZEST_KEY', 
    prod_col = 'inquiry_product_type',
    date_inq_col = 'DATE_OF_INQUIRY', 
    date_ref_col = 'DATE_OF_REQUEST', 
    half_lives = (30,90),
    gap_days_col = 'days_since_last_inquiry',
    window_months=(1, 3, 6, 9, 12),
    product_groups = ['auto', 'mortgage', 'other']
    ):
    """
    Runs feature engineering
    """
    df = df.copy()

    ln2 = np.log(2.0)
    # Create count cols that is weighted by how close use half life of 30 and 90
    decay_col_beg = '__decay_'
    for H in half_lives:
        wcol = f'{decay_col_beg}{int(H)}d' 
        df[wcol]  = np.exp(-ln2 * df['days_from_application'].fillna(0)/ float(H))
    

    def final_features(df, filter_val= None):
        df = df.copy()
        if filter_val is not None:
            filtered_df = df[df[prod_col]== filter_val]
            prefix = filter_val
        else:
            filtered_df = df
            prefix = "all"
        ## Count each inquiry in weighted way so closer ones count more
        
        g = filtered_df.groupby(key_col)

        num_inq = g.size().rename(f'{prefix}_num_inq')

        # mean gap std, cv
        mean_gap = g[gap_days_col].mean().rename(f'{prefix}_mean_gap_days')
        std_gap = g[gap_days_col].std().rename(f'{prefix}_std_gap_days')
        gap_cv = (std_gap / mean_gap).rename(f'{prefix}_gap_cv')
          # last gap (gap for the most recent inquiry): take last non-null gap
        nonnull = filtered_df[filtered_df[gap_days_col].notna()]
        if not nonnull.empty:
            last_gap = nonnull.groupby(key_col)[gap_days_col].last().rename(f'{prefix}_days_since_last_inquiry')
        else:
            last_gap = pd.Series(dtype=float, name=f'{prefix}_days_since_last_inquiry')
        last_gap_m = (last_gap / 30.4375).rename(f'{prefix}_months_since_last_inquiry')

        ## percentiles vectorized
        qs = [0.10, 0.25, 0.50, 0.75, 0.90]
        ## computes multi quantiles for each group: series one row for every zest and quantile
        
        pct_ser = filtered_df.groupby(key_col)[gap_days_col].quantile(qs)  # MultiIndex (key, quantile)
        # unstack level = -1 pivots the last level (quantile level into columns) and then we rename it
        pct_df = pct_ser.unstack(level=-1).rename(columns={
            0.10: f'{prefix}_gap_p10',
            0.25: f'{prefix}_gap_p25',
            0.50: f'{prefix}_gap_p50',
            0.75: f'{prefix}_gap_p75',
            0.90: f'{prefix}_gap_p90'
        })

        ## sum in each window timeframe
        window_aggs = {}
        for m in window_months:
            cname = f'{prefix}_inq_{m}m'
            ## each part of the dictionary is a series
            window_aggs[cname] = g[f'in_last_{m}m'].sum().rename(cname)
        # decay sums
        decay_aggs = {}
        for H in half_lives:
            wcol = f'{decay_col_beg}{int(H)}d'
            decay_aggs[f'{prefix}_decay_{int(H)}d'] = g[wcol].sum().rename(f'{prefix}_decay_{int(H)}d')
        ## each part is pd.series or pd.DataFrame with zest__key as the index
        
        parts = [num_inq, mean_gap, std_gap, gap_cv, last_gap, last_gap_m, pct_df] + list(window_aggs.values()) + list(decay_aggs.values())
        feat = pd.concat(parts, axis = 1).reset_index()
        return feat
    base_df = final_features(df= df)
    for prod_type in product_groups:
        append_df = final_features(df = df, filter_val = prod_type)
        base_df = base_df.merge(append_df, on = key_col, how = 'inner')
    return base_df
            
            

        

        
    
    

    
    
    