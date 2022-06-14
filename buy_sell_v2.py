import psycopg2

mydb = psycopg2.connect(database="postgres", user = "postgres", password = "forex@123", host = "34.66.176.253", port = "5432")


weights = {'1Min': 5, '5Min': 8, '15Min': 12, '30Min': 15, '60Min': 20, '240Min': 40}

# AUDUSD = {'1Min': 0, '5Min': 0, '15Min': 0, '30Min': 0, '60Min': 0, '240Min': 0}
# EURAUD = {'1Min': 0, '5Min': 0, '15Min': 0, '30Min': 0, '60Min': 0, '240Min': 0}
# EURUSD = {'1Min': 0, '5Min': 0, '15Min': 0, '30Min': 0, '60Min': 0, '240Min': 0}
# GBPUSD = {'1Min': 0, '5Min': 0, '15Min': 0, '30Min': 0, '60Min': 0, '240Min': 0}
# USDCHF = {'1Min': 0, '5Min': 0, '15Min': 0, '30Min': 0, '60Min': 0, '240Min': 0}
# USDJPY = {'1Min': 0, '5Min': 0, '15Min': 0, '30Min': 0, '60Min': 0, '240Min': 0}
# print(weights['1Min'])

time_relevance = [
    ['1Min', 'hlb'], ['5Min', 'hlb'], ['15Min', 'hlb'], ['30Min', 'hlb'], ['60Min', 'hlb'], ['240Min', 'hlb'],
    ['1Min', 'hlb'], ['5Min', 'hlb'], ['15Min', 'hlb'], ['30Min', 'hlb'], ['60Min', 'hlb'], ['240Min', 'hlb'],
    ['1Min', 'hlb'], ['5Min', 'hlb'], ['15Min', 'hlb'], ['30Min', 'hlb'], ['60Min', 'hlb'], ['240Min', 'hlb'],
    ['1Min', 'hlb'], ['5Min', 'hlb'], ['15Min', 'hlb'], ['30Min', 'hlb'], ['60Min', 'hlb'], ['240Min', 'hlb'],
    ['1Min', 'hlb'], ['5Min', 'hlb'], ['15Min', 'hlb'], ['30Min', 'hlb'], ['60Min', 'hlb'], ['240Min', 'hlb'],
    ['1Min', 'hlb'], ['5Min', 'hlb'], ['15Min', 'hlb'], ['30Min', 'hlb'], ['60Min', 'hlb'], ['240Min', 'hlb']
]

interval_time = ['1Min', '5Min', '15Min', '30Min', '60Min', '240Min']


def populate_db(cr_name):
    mycursor = mydb.cursor()
    sql = "INSERT INTO currency_buy_sell (currency, buy, sell, current_price) VALUES (%s, %s, %s, %s)"
    val = (cr_name, None, None, None)
    mycursor.execute(sql, val)
    mydb.commit()


def add_to_db(curn_name, h_per, l_pur, curr_price):
    mycursor = mydb.cursor()
    sql = "UPDATE currency_buy_sell set buy='" + str(h_per) + "', sell='" + str(l_pur) + "', current_price='" + str(
        curr_price) + "' where currency='" + str(curn_name) + "'"
    mycursor.execute(sql)
    mydb.commit()
    # print(mycursor.rowcount, f" Record inserted successfully into table for {currency} at {time} for {time_interval} ")

    # except(Exception, psycopg.Error) as bulk_insert_error:
    #     print("Failed inserting record into currency table {}".format(bulk_insert_error))
    #     mydb.rollback()


def buy_sell(cr_name, h_irr_sop, l_irr_sop, hl_rel, cur_price):
    if hl_rel == 'high_irr':
        print('For ' + cr_name + ' SELL ONLY : ' + str(h_irr_sop))
        add_to_db(cr_name, str(h_irr_sop), '0', cur_price)
        # return 0, h_irr_sop         # Only Sell
    elif hl_rel == 'low_irr':
        print('For ' + cr_name + ' BUY ONLY : ' + str(l_irr_sop))
        add_to_db(cr_name, '0', str(l_irr_sop), cur_price)
        # return l_irr_sop, 0         # Only Buy
    else:
        print('For ' + cr_name + ' BUY : ' + str(l_irr_sop), ' SELL : ' + str(h_irr_sop))
        add_to_db(cr_name, str(l_irr_sop), str(h_irr_sop), cur_price)
        # return l_irr_sop, h_irr_sop     # Buy, Sell


def before_append(cur_name, intv, relv, cur_price):
    if cur_name == 'AUDUSD':
        start = 0
        time_relevance.insert(interval_time.index(intv) + start, [intv, relv])
    elif cur_name == 'EURAUD':
        start = 6
        time_relevance.insert(interval_time.index(intv) + start, [intv, relv])
    elif cur_name == 'EURUSD':
        start = 12
        time_relevance.insert(interval_time.index(intv) + start, [intv, relv])
    elif cur_name == 'EURAUD':
        start = 18
        time_relevance.insert(interval_time.index(intv) + start, [intv, relv])
    elif cur_name == 'GBPUSD':
        start = 24
        time_relevance.insert(interval_time.index(intv) + start, [intv, relv])
    elif cur_name == 'USDCHF':
        start = 30
        time_relevance.insert(interval_time.index(intv) + start, [intv, relv])
    else:
        start = 36
        time_relevance.insert(interval_time.index(intv) + start, [intv, relv])

    h_count = 0
    l_count = 0
    both_count = 0
    for t in range(0 + start, 6 + start):
        if time_relevance[t][1] == 'h_irr':
            h_count += weights[time_relevance[t][0]]
            # h_count += weights[t[0]]
        elif time_relevance[t][1] == 'l_irr':
            l_count += weights[time_relevance[t][0]]
        elif time_relevance[t][1] == 'both':
            both_count += weights[time_relevance[t][0]]

    if h_count > l_count and h_count > both_count:
        buy_sell(cur_name, h_count, l_count, 'high_irr', cur_price)
    elif l_count > h_count and l_count > both_count:
        buy_sell(cur_name, h_count, l_count, 'low_irr', cur_price)
    else:
        buy_sell(cur_name, h_count, l_count, 'both_rel', cur_price)


def check_relevant(curr_name, interval, cur_price, h_pred, l_pred):
    try:
        if h_pred and l_pred:
            if h_pred > cur_price > l_pred:
                # - Both relevant
                # time_relevance.append([interval, 'both'])
                before_append(curr_name, interval, 'both', cur_price)
            elif h_pred < cur_price and l_pred < cur_price:
                # - Low relevant
                # - High irrelevant
                # time_relevance.append([interval, 'h_irr'])
                before_append(curr_name, interval, 'h_irr', cur_price)
            elif h_pred > cur_price and l_pred > cur_price:
                # - High relevant
                # - Low irrelevant
                # time_relevance.append([interval, 'l_irr'])
                before_append(curr_name, interval, 'l_irr', cur_price)
    except:
        return None

# def update_sql(sql_query):
#     mycursor = mydb.cursor()
#     mycursor.execute(sql_query)
#     mydb.commit()
# print(mycursor.rowcount, "rows affected")


# update_sql("TRUNCATE Table currency_buy_sell")


# time_relevance = [['1Min', 'h_irr'], ['5Min', 'h_irr'], ['15Min', 'l_irr'], ['30Min', 'both']]
# time_relevance = [['1Min', 'both'], ['1Min', 'both'], ['1Min', 'both'], ['1Min', 'h_irr']]
# time_relevance = [['1Min', 'l_irr'], ['1Min', 'l_irr'], ['1Min', 'h_irr'], ['1Min', 'l_irr']]
