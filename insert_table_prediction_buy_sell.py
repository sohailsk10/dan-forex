import os
import MetaTrader5 as mt5
import pytz
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from buy_sell_v2 import check_relevant

BASE_MODEL_DIR = 'trained_models'

mydb = psycopg2.connect(database="postgres", user = "postgres", password = "forex@123", host = "34.66.176.253", port = "5432")

INTERVALS_LIST = ['1Min', '5Min', '15Min', '30Min', '60Min', '240Min']
currency_ticks = os.listdir(BASE_MODEL_DIR)

currency_predicted_list = []
ticks_list_1min = []
ticks_list_5min = []
ticks_list_15min = []
ticks_list_30min = []
ticks_list_60min = []
ticks_list_240min = []

freeze_dict = {}
currency_dict = {}
currency_dict_low = {}
freeze_dict_low = {}
prev_t_datetime = {}

freeze_1Min = False
freeze_5Min = False


def insert_to_db_1(currency, time_interval, high, high_prediction, date_time_hit_high, low, low_prediction, date_time_hit_low):
    mycursor = mydb.cursor()
    sql = "INSERT INTO predicted_high_low (currency, time_interval, high, high_prediction, date_time_hit_high, low, low_prediction, date_time_hit_low) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    val = (currency, time_interval, high, high_prediction, date_time_hit_high, low, low_prediction, date_time_hit_low)
    mycursor.execute(sql, val)
    mydb.commit()
    # print(mycursor.rowcount, f" Record inserted successfully into table for {currency} at {time} for {time_interval} ")

    # except(Exception, psycopg.Error) as bulk_insert_error:
    #     print("Failed inserting record into currency table {}".format(bulk_insert_error))
    #     mydb.rollback()


def fetch_data(currency):
    global currency_predicted_list
    dict = {}
    # for time_interval in INTERVALS_LIST:
    #     mycursor1 = mydb.cursor()
    #     sql = "SELECT predicted_high, predicted_low, target_datetime FROM multiple_currency_interval_prediction where " \
    #           "currency = '" + currency + "' and time_interval = '" + time_interval + "' order by time desc limit 1; "
    #     mycursor1.execute(sql)
    #     data = mycursor1.fetchall()
    #     dict[time_interval] = data

    mycursor = mydb.cursor()
    sql = "INSERT INTO currency_buy_sell (currency, buy, sell, current_price) VALUES (%s, %s, %s, %s)"
    val = (currency, None, None, None)
    mycursor.execute(sql, val)
    mydb.commit()

    currency_predicted_list.append(dict)
    for k, v in dict.items():
        try:
            insert_to_db_1(currency, k, None, v[0][0], None, None, v[0][1], None)
        except:
            insert_to_db_1(currency, k, None, None, None, None, None, None)


def update_sql(sql_query):
    mycursor = mydb.cursor()
    mycursor.execute(sql_query)
    mydb.commit()
    # print(mycursor.rowcount, "rows affected")


# update_sql("TRUNCATE Table predicted_high_low")
update_sql("TRUNCATE Table currency_buy_sell")

for currency in currency_ticks:
    fetch_data(currency)


def fetch_prediction_datetime(currency, time_):
    mycursor = mydb.cursor()
    update_predicted_time = "SELECT date_time_hit_high, date_time_hit_low from predicted_high_low WHERE currency = '" + currency + "' and time_interval = '" + time_ + "'"
    mycursor.execute(update_predicted_time)
    return mycursor.fetchall()[0]


def update_prediction_value(currency, time_):
    mycursor = mydb.cursor()
    sql = "SELECT target_datetime, predicted_high, predicted_low FROM multiple_currency_interval_prediction where time_interval = '" + time_ + "' and currency = '" + currency + "' order by time desc limit 1;"
    mycursor.execute(sql)
    try:
        t, h, l = mycursor.fetchall()[0]
    except:
        return None, None, None
    update_predicted_sql = "UPDATE predicted_high_low SET high_prediction = '" + str(h) + "', low_prediction = '" + str(
        l) + "'  WHERE currency = '" + currency + "' and time_interval = '" + time_ + "'"
    update_sql(update_predicted_sql)
    return t, h, l


def max_check(currency, high, time_interval, currency_number, p_high):
    currency_dict[currency][currency_number].append(high)
    max_high = max(currency_dict[currency][currency_number])
    sql = "UPDATE predicted_high_low SET high = '" + str(
        max_high) + "' WHERE currency = '" + currency + "' and time_interval = '"+ time_interval +"'"
    update_sql(sql)
    if max_high > p_high:
        time_sql = "UPDATE predicted_high_low SET date_time_hit_high = '" + datetime.utcnow().strftime(
            "%Y%m%d%H%M%S") + \
                   "' WHERE currency = '" + currency + "' and time_interval = '"+ time_interval +"'"
        update_sql(time_sql)
        freeze_dict[currency].insert(currency_number, True)
        currency_dict[currency][currency_number].clear()


def min_check(currency, low, time_interval, currency_number, p_low):
    currency_dict_low[currency][currency_number].append(low)
    min_low = min(currency_dict_low[currency][currency_number])
    sql = "UPDATE predicted_high_low SET low = '" + str(
        min_low) + "' WHERE currency = '" + currency + "' and time_interval = '"+ time_interval +"'"
    update_sql(sql)
    if min_low < p_low:
        time_sql = "UPDATE predicted_high_low SET date_time_hit_low = '" + datetime.utcnow().strftime(
            "%Y%m%d%H%M%S") + \
                   "' WHERE currency = '" + currency + "' and time_interval = '"+ time_interval +"'"
        update_sql(time_sql)
        freeze_dict_low[currency].insert(currency_number, True)
        currency_dict_low[currency][currency_number].clear()


def update_actual_high_low(currency, time_):
    global freeze_1Min, freeze_5Min, freeze_dict, currency_dict, currency_dict_low, freeze_dict_low, prev_t_datetime
    datetime_now = datetime.utcnow()
    high, low, current_value = get_data_mt5(currency)
    t_datetime, p_high, p_low = update_prediction_value(currency, time_)

    check_relevant(currency, time_, current_value, p_high, p_low)

    # t_datetime_high, t_datetime_low = fetch_prediction_datetime(currency, time_)
    # print("-", datetime_now, t_datetime, t_datetime_high, t_datetime_low, p_high, p_low, currency, time_,
    #       len(currency_dict[currency][INTERVALS_LIST.index(time_)]))
    #
    # if t_datetime is not None and high is not None:
    #     if t_datetime_high is not None:
    #         if datetime_now > t_datetime:
    #             print("DateTime Change", currency, time_)
    #             query = "UPDATE predicted_high_low set date_time_hit_high = NULL, high = NULL WHERE currency = '" + currency + "' and time_interval = '" + time_ + "'"
    #             update_sql(query)
    #
    #             if time_ == "1Min":
    #                 currency_dict[currency][0].clear()
    #                 freeze_dict[currency][0] = False
    #             elif time_ == "5Min":
    #                 currency_dict[currency][1].clear()
    #                 freeze_dict[currency][1] = False
    #             elif time_ == "15Min":
    #                 currency_dict[currency][2].clear()
    #                 freeze_dict[currency][2] = False
    #             elif time_ == "30Min":
    #                 currency_dict[currency][3].clear()
    #                 freeze_dict[currency][3] = False
    #             elif time_ == "60Min":
    #                 currency_dict[currency][4].clear()
    #                 freeze_dict[currency][4] = False
    #             elif time_ == "240Min":
    #                 currency_dict[currency][5].clear()
    #                 freeze_dict[currency][5] = False
    #
    #         else:
    #             if time_ == "1Min":
    #                 freeze_dict[currency].insert(0, True)
    #             elif time_ == "5Min":
    #                 freeze_dict[currency].insert(1, True)
    #             elif time_ == "15Min":
    #                 freeze_dict[currency].insert(2, True)
    #             elif time_ == "30Min":
    #                 freeze_dict[currency].insert(3, True)
    #             elif time_ == "60Min":
    #                 freeze_dict[currency].insert(4, True)
    #             elif time_ == "240Min":
    #                 freeze_dict[currency].insert(5, True)
    #
    #     if t_datetime_low is not None:
    #         if datetime_now > t_datetime:
    #             print("DateTime Change", currency, time_)
    #             query = "UPDATE predicted_high_low set date_time_hit_low = NULL, low = NULL WHERE currency = '" + currency + "' and time_interval = '" + time_ + "'"
    #             update_sql(query)
    #
    #             if time_ == "1Min":
    #                 currency_dict_low[currency][0].clear()
    #                 freeze_dict_low[currency][0] = False
    #             elif time_ == "5Min":
    #                 currency_dict_low[currency][1].clear()
    #                 freeze_dict_low[currency][1] = False
    #             elif time_ == "15Min":
    #                 currency_dict_low[currency][2].clear()
    #                 freeze_dict_low[currency][2] = False
    #             elif time_ == "30Min":
    #                 currency_dict_low[currency][3].clear()
    #                 freeze_dict_low[currency][3] = False
    #             elif time_ == "60Min":
    #                 currency_dict_low[currency][4].clear()
    #                 freeze_dict_low[currency][4] = False
    #             elif time_ == "240Min":
    #                 currency_dict_low[currency][5].clear()
    #                 freeze_dict_low[currency][5] = False
    #
    #         else:
    #             if time_ == "1Min":
    #                 freeze_dict_low[currency].insert(0, True)
    #             elif time_ == "5Min":
    #                 freeze_dict_low[currency].insert(1, True)
    #             elif time_ == "15Min":
    #                 freeze_dict_low[currency].insert(2, True)
    #             elif time_ == "30Min":
    #                 freeze_dict_low[currency].insert(3, True)
    #             elif time_ == "60Min":
    #                 freeze_dict_low[currency].insert(4, True)
    #             elif time_ == "240Min":
    #                 freeze_dict_low[currency].insert(5, True)
    #
    #     if datetime_now < t_datetime:
    #         if time_ == "1Min":
    #             if not freeze_dict[currency][0]:
    #                 max_check(currency, high, "1Min", 0, p_high)
    #             if not freeze_dict_low[currency][0]:
    #                 min_check(currency, low, "1Min", 0, p_low)
    #
    #         elif time_ == "5Min":
    #             if not freeze_dict[currency][1]:
    #                 max_check(currency, high, "5Min", 1, p_high)
    #             if not freeze_dict_low[currency][1]:
    #                 min_check(currency, low, "5Min", 1, p_low)
    #
    #         elif time_ == "15Min":
    #             if not freeze_dict[currency][2]:
    #                 max_check(currency, high, "15Min", 2, p_high)
    #             if not freeze_dict_low[currency][2]:
    #                 min_check(currency, low, "15Min", 2, p_low)
    #
    #         elif time_ == "30Min":
    #             if not freeze_dict[currency][3]:
    #                 max_check(currency, high, "30Min", 3, p_high)
    #             if not freeze_dict_low[currency][3]:
    #                 min_check(currency, low, "30Min", 3, p_low)
    #
    #         elif time_ == "60Min":
    #             if not freeze_dict[currency][4]:
    #                 max_check(currency, high, "60Min", 4, p_high)
    #             if not freeze_dict_low[currency][4]:
    #                 min_check(currency, low, "60Min", 4, p_low)
    #
    #         elif time_ == "240Min":
    #             if not freeze_dict[currency][5]:
    #                 max_check(currency, high, "240Min", 5, p_high)
    #             if not freeze_dict_low[currency][5]:
    #                 min_check(currency, low, "240Min", 5, p_low)


def get_data_mt5(currency):
    if not mt5.initialize():
        print("initialize() failed, error code =", mt5.last_error())
        return None
    try:
        utc_from = datetime.now(tz=pytz.utc) - timedelta(minutes=1)
        utc_to = datetime.now(tz=pytz.utc)
        ticks = mt5.copy_ticks_range(currency, utc_from, utc_to, mt5.COPY_TICKS_ALL)
        # print("currency", currency)
        ticks_frame = pd.DataFrame(ticks)
        ticks_frame['time'] = pd.to_datetime(ticks_frame['time'], unit='s', errors='coerce')
        ticks_frame = ticks_frame.set_index(ticks_frame['time'])
        current_value = ((ticks_frame['ask'] + ticks_frame['bid']) / 2)
        # print(current_value.tail(1).values[-1])
        # print(current_value.tail(1))
        # print(current_value[0])
        # print(current_value.values[-1])

        # df.groupby(dr5minute.asof).agg({'Low': lambda s: s.min(),
        #                                 'High': lambda s: s.max(),
        #                                 'Open': lambda s: s[0],
        #                                 'Close': lambda s: s[-1],
        #                                 'Volume': lambda s: s.sum()})

        # ohlc_dict = {
        #     'data_bid': lambda s: s.mean(),
        #     'data_ask': lambda s: s.mean(),
        # }
        #
        # tick_f = ticks_frame.resample('1s', closed='left', label='left').apply(ohlc_dict)
        # print(tick_f)
        # ticks_frame.resample('1s', closed='left', label='left').apply(ohlc_dict)



        data_ask = ticks_frame['ask'].resample('1s').ohlc()
        data_bid = ticks_frame['bid'].resample('1s').ohlc()
        # current_value = (data_ask + data_bid) / 2
        high = (data_ask['high'].tail(1).values[-1] + data_bid['high'].tail(1).values[-1]) / 2
        low = (data_ask['low'].tail(1).values[-1] + data_bid['low'].tail(1).values[-1]) / 2

        mt5.shutdown()
        return high, low, current_value.tail(1).values[-1]
    except:
        mt5.shutdown()
        return None, None, None


for currency in currency_ticks:
    freeze_dict[currency] = [False, False, False, False, False, False]
    freeze_dict_low[currency] = [False, False, False, False, False, False]
    currency_dict[currency] = [[], [], [], [], [], []]
    currency_dict_low[currency] = [[], [], [], [], [], []]

# print('=====================')
# print(freeze_dict)
# print(freeze_dict_low)
# print(currency_dict)
# print(currency_dict_low)
while True:
    for currency in currency_ticks:
        for time_ in INTERVALS_LIST:
            update_actual_high_low(currency, time_)
