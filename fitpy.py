import numpy as np
import pandas as pd
from datetime import datetime, timedelta








class SleepPreprocessor:
    """
    'Process the sleep data to suit the analysis process.'
    """
    def __init__(self):
        pass

    def fit(self, df):
        self.df = df.copy()

    def transform(self, stitch=True, filter='night_main'):
        """
        'Transform the sleep data with selected options.'

        INPUT:
            stitch: True, False
            filter: 'night_main', 'night_nap', 'day_main', 'day_nap', None
        """
        if stitch == True: stitched_sleep_df = self._stitch(self.df)
        filtered_sleep_df = self._filter(stitched_sleep_df, filter=filter)

        return filtered_sleep_df

    def _stitch(self, df):
        """
        'Stitches sleep logs that are less than 120 minutes apart from each other.' 
        
        INPUT: 
            sleep_df: pd.DataFrame
        OUTPUT:
            new_df: pd.DataFrame
        """

        def get_delta(df):
            """
            'Calculates minutes elapsed from wake to sleep.'

            INPUT:
                sleep_df: pd.DataFrame
            OUTPUT:
                new_df: pd.Series
            """
            
            new_df = pd.DataFrame()
            new_df['delta'] = df['start'] - df['end'].shift(1)
            new_df['delta'].fillna(pd.Timedelta(seconds=0), inplace=True)            
            return new_df['delta'].apply(lambda x: int(x.seconds//60))


        new_df=df.copy()
        later = np.where((get_delta(new_df) < 120) & (get_delta(new_df) > 0))[0]
        earlier = later - 1
        stitch_ind = list(zip(earlier, later))
        for e, l in stitch_ind:
            new_start = new_df.loc[e,'start']
            new_end = new_df.loc[l,'end']
            new_bed = (new_df.loc[l,'end'] - new_df.loc[e,'start']).seconds/60
            new_asleep = new_df.loc[l, 'asleep'] + new_df.loc[e, 'asleep']
            new_effi = int(round(new_asleep / new_bed * 100, 0))
            if pd.notna(new_df.loc[l,'deep'] + new_df.loc[e, 'deep']):
                new_deep = new_df.loc[l,'deep'] + new_df.loc[e, 'deep']
            else: new_deep = np.nan

            if pd.notna(new_df.loc[l,'rem'] + new_df.loc[e, 'rem']):
                new_rem = new_df.loc[l,'rem'] + new_df.loc[e, 'rem']
            else: new_rem = np.nan

            if pd.notna(new_df.loc[l,'awake'] + new_df.loc[e, 'awake']):
                new_awake = new_df.loc[l,'awake'] + new_df.loc[e, 'awake'] + (new_df.loc[l, 'start']-new_df.loc[e,'end']).seconds/60%1440
            else: new_awake = np.nan

            if pd.notna(new_df.loc[l,'awakening'] + new_df.loc[e, 'awakening']):
                new_awakening = new_df.loc[l,'awakening'] + new_df.loc[e, 'awakening']
            else: new_awakening = np.nan
            
            new_dict = {'start':new_start, 'end':new_end, 'bed':new_bed, 'asleep':new_asleep, 'deep':new_deep, 'rem':new_rem, 'effi':new_effi, 'awakening':new_awakening, 'awake': new_awake}

            new_df.drop([e,l], inplace=True)        
            new_df = pd.concat([new_df, pd.DataFrame(new_dict, index=[100])], sort=False)
        new_df.sort_values('start', inplace=True)
        new_df.reset_index(inplace=True, drop=True)
        return new_df



    def _filter(self, df, filter='night_main'):
        """
        'Filters types of sleeps: night_main, night_nap, day_main, day_nap.'

        INPUT:
            sleep_df: pd.DataFrame
        OUTPUT:
            new_df: pd.DataFrame
        """

        if filter == 'night_main':
            mask = \
            ((df['start'].apply(lambda x: x.time()) >= datetime(1,1,1,17,0).time()) | \
            (df['start'].apply(lambda x: x.time()) < datetime(1,1,1,5,0).time())) & \
            (df['asleep'] >= 180)
        elif filter == 'night_nap':
            mask = \
            ((df['start'].apply(lambda x: x.time()) >= datetime(1,1,1,22,0).time()) | \
            (df['start'].apply(lambda x: x.time()) < datetime(1,1,1,10,0).time())) & \
            (df['asleep'] < 180)
        elif filter == 'day_main':
            mask = \
            (df['start'].apply(lambda x: x.time()) > datetime(1,1,1,5,0).time()) & \
            (df['start'].apply(lambda x: x.time()) < datetime(1,1,1,17,0).time()) & \
            (df['asleep'] >= 180)
        elif filter == 'day_nap':
            mask = \
            (df['start'].apply(lambda x: x.time()) > datetime(1,1,1,10,0).time()) & \
            (df['start'].apply(lambda x: x.time()) < datetime(1,1,1,22,0).time()) & \
            (df['asleep'] < 180)

        elif filter == None:
            mask = list(range(df.shape[0]))

        new_df = df.loc[mask, :].copy()
        new_df.reset_index(inplace=True, drop=True)
        return new_df




