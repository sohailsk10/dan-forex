import os
import time
import pytz
import numpy as np
import pandas as pd
import MetaTrader5 as mt5
from datetime import datetime, timedelta
from silence_tensorflow import silence_tensorflow
from tensorflow.keras.models import load_model
from sklearn.preprocessing import StandardScaler
from urllib.parse import quote
from sqlalchemy import create_engine
silence_tensorflow()
import psycopg2


N_PAST = 48
N_FUTURE = 1
BASE_MODEL_DIR = 'trained_models'
BASE_CSV_DIR = 'live_data_csv'

currency_ticks = os.listdir(BASE_MODEL_DIR)

INTERVALS_LIST = []
for i in currency_ticks:
    INTERVALS_LIST = os.listdir(os.path.join(BASE_MODEL_DIR, f'{i}'))

prediction_list = []

mydb = psycopg2.connect(database="postgres", user = "postgres", password = "forex@123", host = "34.66.176.253", port = "5432")
# print(mydb)
# conn = create_engine("postgresql+psycopg2://postgres:%s@34.66.176.253/postgres" % quote('forex@123'))


def insert_to_db(time, currency, time_interval, actual_high, actual_low, predicted_high, predicted_low,target_datetime):
    try:
        mycursor = mydb.cursor()
        sql = "INSERT INTO multiple_currency_interval_prediction (time, currency, time_interval, actual_high, actual_low, predicted_high, predicted_low, target_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (time, currency, time_interval, actual_high, actual_low, predicted_high, predicted_low, target_datetime)
        print(sql, val)
        mycursor.execute(sql, val)
        mydb.commit()
        print(mycursor.rowcount, f" Record inserted successfully into table for {currency} at {time} for {time_interval} ")

    except:
        print(f"Failed inserting record into currency table for {currency} at time interval {time_interval}")
        # mydb.rollback()

def create_dir(dir_name):
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)

def next_weekday(d, weekday):
    days_ahead = weekday - d.weekday()
    if days_ahead <= 0:  # Target day already happened this week
        days_ahead += 7
    return d + timedelta(days_ahead)

def get_rsi(file, value, n):
    """
    calculates -> RSI value
    takes argument -> dataframe, column name, period value
    returns dataframe by adding column : 'RSI_' + column name
    """
    delta = file[value].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(span=n, adjust=False).mean()
    ema_down = down.ewm(span=n, adjust=False).mean()
    rs = ema_up / ema_down
    file['RSI_' + value] = 100 - (100 / (1 + rs))

    return file


def moving_avg(ultratech_df, value, fast_p, slow_p):
    """
    calculates -> slow moving average, fast moving average
    takes argument -> dataframe, column name, slow period, fast period
    returns dataframe by adding columns -> 'MA_Slow_HLCC/4',  'SMA_period', MA_Fast_HLCC/4', 'FMA_period'
    """
    ultratech_df['MA_Slow_HLCC/4'] = ultratech_df[value].rolling(window=17, min_periods=1).mean()
    ultratech_df['SMA_period'] = slow_p
    ultratech_df['MA_Fast_HLCC/4'] = ultratech_df[value].rolling(window=7, min_periods=1).mean()
    ultratech_df['FMA_period'] = fast_p

    return ultratech_df


def get_forecast_df(model, x_train, col_ind, col_name):
    """
    returns next prediction in a dataframe
    """

    global forecast_period_dates
    forecast = model.predict(x_train[-N_FUTURE:])  # forecast
    forecast_copies = np.repeat(forecast, train_set.shape[1], axis=-1)
    y_pred_future = sc.inverse_transform(forecast_copies)[:, col_ind]
    forecast_dates = []
    for time_i in forecast_period_dates:
        forecast_dates.append(time_i)

    df_forecast = pd.DataFrame({'time': np.array(forecast_dates), col_name: y_pred_future})
    df_forecast['time'] = pd.to_datetime(df_forecast['time'])

    return df_forecast


def get_data_mt5(currency_name, interval):
    if not mt5.initialize():
        print("initialize() failed, error code =", mt5.last_error())
        return None
    try:
        utc_from = datetime.now(tz=pytz.utc) - timedelta(days=150)
        utc_to = datetime.now(tz=pytz.utc)

        ticks = mt5.copy_ticks_range(currency_name, utc_from, utc_to, mt5.COPY_TICKS_ALL)
        ticks_frame = pd.DataFrame(ticks)
        ticks_frame['time'] = pd.to_datetime(ticks_frame['time'], unit='s')
        ticks_frame = ticks_frame.set_index(ticks_frame['time'])

        data_ask = ticks_frame['ask'].resample(interval).ohlc()
        data_bid = ticks_frame['bid'].resample(interval).ohlc()

        data = pd.DataFrame()
        data['open'] = (data_ask['open'] + data_bid['open']) / 2
        data['high'] = (data_ask['high'] + data_bid['high']) / 2
        data['low'] = (data_ask['low'] + data_bid['low']) / 2
        data['close'] = (data_ask['close'] + data_bid['close']) / 2
        data = data.reset_index()

        data = data.reset_index()
        data['HLCC/4'] = (data['high'] + data['low'] + data['close'] + data['close']) / 4
        data = get_rsi(data, 'HLCC/4', 14)
        data = moving_avg(data, 'HLCC/4', 17, 7)
        data = data.dropna()
        data = data.tail(5000)
        mt5.shutdown()

        return data
    except:
        return None


cols = ['high', 'low', 'RSI_HLCC/4', 'MA_Slow_HLCC/4', 'MA_Fast_HLCC/4']
sc = StandardScaler()

# ============= INCONSTANT VARIABLES ============= #
predicted_currency = []

last_predicted_data = []

create_dir(BASE_CSV_DIR)
create_dir(BASE_MODEL_DIR)

all_loaded_models = []

for i in currency_ticks:
    for j in INTERVALS_LIST:
        high_model_path = os.path.join(BASE_MODEL_DIR, f'{i}', f'{j}', 'high_model', 'high.h5')
        low_model_path = os.path.join(BASE_MODEL_DIR, f'{i}', f'{j}', 'low_model', 'low.h5')
        high_model = load_model(high_model_path, compile = False)
        low_model = load_model(low_model_path, compile = False)
        all_loaded_models.append([high_model, low_model, i])


predicted_df = pd.DataFrame(columns=['datetime','currency','time_interval','predicted_high','predicted_low','target_datetime'])

predicted = pd.DataFrame(columns=['time', 'currency', 'time_interval', 'actual_high', 'actual_low', 'predicted_high', 'predicted_low', 'target_datetime'])

actual_df = pd.DataFrame(columns=['datetime','currency','time_interval','actual_high','actual_low'])

while True:
    # =============== CURRENT UTC TIME =============== #
    utc_time = datetime.utcnow()
    utc_year = utc_time.strftime('%Y')
    utc_month = utc_time.strftime('%m')
    utc_day = utc_time.strftime('%d')
    utc_hour = utc_time.strftime('%H')
    utc_minute = utc_time.strftime('%M')
    utc_second = utc_time.strftime('%S')
    utc_day_name = utc_time.strftime('%a')

    current_utc_dt = datetime(int(utc_year), int(utc_month), int(utc_day), int(utc_hour), int(utc_minute),
                              int(utc_second))

    if utc_day_name != 'Sat' and utc_day_name != 'Sun':
        mydb = psycopg2.connect(database="postgres", user="postgres", password="forex@123", host="34.66.176.253",
                                port="5432")

        for currency in currency_ticks:
            for time_ in INTERVALS_LIST:
                _minute = time_.split('Min')[0]
                _minute = int(_minute)
                # try:
                #     _minute = time_[:2]
                #     _minute = int(_minute)
                # except:
                #     _minute = time_[0]
                # _minute = int(_minute)
                og_df = get_data_mt5(currency, time_)
                models = all_loaded_models[currency_ticks.index(currency)]
                high_new_model = models[0]
                low_new_model = models[1]
                model_currency = models[2]

                last_data = [i for i in last_predicted_data if i[0] == currency and i[1] == time_]

                if og_df is not None and len(og_df) != 0:
                    # =============== LAST DATA DATETIME =============== #
                    last_year = (og_df['time'].tail(1).apply(lambda x: x.strftime('%Y'))).values[-1]
                    last_month = (og_df['time'].tail(1).apply(lambda x: x.strftime('%m'))).values[-1]
                    last_day = (og_df['time'].tail(1).apply(lambda x: x.strftime('%d'))).values[-1]
                    last_hour = (og_df['time'].tail(1).apply(lambda x: x.strftime('%H'))).values[-1]
                    last_minute = (og_df['time'].tail(1).apply(lambda x: x.strftime('%M'))).values[-1]
                    last_second = (og_df['time'].tail(1).apply(lambda x: x.strftime('%S'))).values[-1]

                    last_data_dt = datetime(int(last_year), int(last_month), int(last_day),
                                            int(last_hour), int(last_minute), int(last_second))

                    time_difference = current_utc_dt - last_data_dt
                    minutes = time_difference.total_seconds() / 60

                    # =============== NEXT MONDAY DATE =============== #
                    dt = datetime(int(utc_time.strftime('%Y')), int(utc_time.strftime('%m')),
                                  int(utc_time.strftime('%d')))
                    next_monday = next_weekday(dt, 0)
                    next_monday = datetime(int(next_monday.strftime("%Y")), int(next_monday.strftime("%m")),
                                           int(next_monday.strftime("%d")), int(next_monday.strftime("%H")),
                                           int(next_monday.strftime("%M")), int(next_monday.strftime("%S")))

                    # ============= NEXT SATURDAY DATE =============== #
                    next_saturday = next_weekday(dt, 5)

                    if len(last_data) > 0 and (pd.to_datetime(last_data_dt) - last_data[-1][3]).total_seconds() == 0:
                        continue

                    # =============== CHECKING WHY NEW DATA IS NOT FETCHED =============== #
                    elif minutes > _minute:
                        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                        print('                  TRADING IS OFF -> NO NEW DATA')
                        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                        time.sleep(2)
                        # predicted_df = predicted_df[
                        #     predicted_df.currency != currency and predicted_df.time_interval != time_]
                        last_predicted_data = [i for i in last_predicted_data if i[0] != currency and i[1] != time_]
                        # actual_df = actual_df[actual_df.currency != currency and actual_df.time_interval != time_]

                    elif time_difference.total_seconds() > 10:
                        train_dates = og_df['time']

                        # =============== PREDICTION DATETIME CALCULATION =============== #
                        from_date = pd.to_datetime(train_dates.values[-1]) + timedelta(minutes=_minute)
                        forecast_period_dates = pd.date_range(from_date, periods=N_FUTURE, freq=time_).tolist()
                        forecast_period_dates = [
                            next_monday if next_monday > forecast_period_dates[-1] >= next_saturday else
                            forecast_period_dates
                            [-1]]


                        train_set = og_df[cols].astype(float)
                        scaled_data = sc.fit_transform(train_set)
                        x_train = []

                        # =============== GETTING N_PAST DATA FOR PREDICTION =============== #
                        for x in range(N_PAST, len(scaled_data) - N_FUTURE + 1):
                            x_train.append(scaled_data[x + 1 - N_PAST:x + 1, 0:scaled_data.shape[1]])

                        x_train = np.array(x_train)

                        # =============== PREDICTIONS =============== #
                        # print("X -------------------------------- X ---------------------------------- X")
                        # print("                                   ", currency, "PREDICTIONS")
                        # print("X -------------------------------- X ---------------------------------- X")

                        high_df_forecast = get_forecast_df(high_new_model, x_train, 0, 'high')
                        low_df_forecast = get_forecast_df(low_new_model, x_train, 1, 'low')

                        # print()
                        # print('Current Time: ', pd.to_datetime(og_df['time'].values[-1]),
                        #       '\nPrediction time: ', pd.to_datetime(str(high_df_forecast['time'].values[-1])),
                        #       '\nPredicted High value: ' + str(high_df_forecast['high'].values[-1]),
                        #       '\nPredicted Low value: ' + str(low_df_forecast['low'].values[-1]))
                        # print()

                        if not high_df_forecast.empty and not low_df_forecast.empty:
                            if og_df['time'].values[-1] <= high_df_forecast['time'].values[-1]:
                                # predicted_df.append(
                                #     {'datetime': og_df['time'].values[-1],
                                #      'currency': currency,
                                #      'time_interval': time_,  # added newly
                                #      'predicted_high': high_df_forecast['high'].values[-1],
                                #      'predicted_low': low_df_forecast['low'].values[-1],
                                #      'target_datetime': forecast_period_dates[-1]},
                                #     ignore_index=True)
                                #
                                # predicted = predicted.append(
                                #     {'time': og_df['time'].values[-1],
                                #      'currency': currency,
                                #      'time_interval': time_,
                                #      'actual_high': og_df['high'].values[-1],
                                #      'actual_low': og_df['low'].values[-1],
                                #      'predicted_high': high_df_forecast['high'].values[-1],
                                #      'predicted_low': low_df_forecast['low'].values[-1],
                                #      'target_datetime': high_df_forecast['time'].values[-1]}, ignore_index=True)

                                current_time = pd.to_datetime(og_df['time'].values[-1])
                                high_value = np.float32(og_df['high'].values[-1]).item()
                                low_value = np.float32(og_df['low'].values[-1]).item()
                                predicted_high_value = np.float32(high_df_forecast['high'].values[-1]).item()
                                predicted_low_value = np.float32(low_df_forecast['low'].values[-1]).item()
                                target_time = pd.to_datetime(high_df_forecast['time'].values[-1])
                                #
                                # # predicted.to_csv('predicted.csv')
                                # predicted.to_sql(con=conn, name='multiple_currency_interval_prediction',
                                #                  if_exists='replace', index=False)
                                # print("Values Inserted to Database")
                                insert_to_db(current_time, currency, time_, high_value, low_value, predicted_high_value,
                                             predicted_low_value, target_time)

                                last_predicted_data.append(
                                    [currency, time_, forecast_period_dates[-1], og_df['time'].values[-1]])

        # time.sleep(20)

    else:
        # =============== NEXT MONDAY DATE =============== #
        dt = datetime(int(utc_year), int(utc_month), int(utc_day))
        next_monday = next_weekday(dt, 0)

        # =============== CHECKING WHY NEW DATA IS NOT FETCHED =============== #
        diff = (pd.to_datetime(next_monday) - datetime.utcnow())
        print()
        print(int((diff.total_seconds() / 60)), 'Minutes', int((diff.total_seconds() / 60) / 60), 'Hours')
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print('                   TRADING IS OFF -> WEEKEND')
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        time.sleep(diff.total_seconds() + 900)
