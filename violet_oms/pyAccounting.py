import pandas as pd
import numpy as np
from jdfs import *
import sys

# np.set_printoptions(suppress=True, threshold=sys.maxsize)
pd.set_option('display.float_format', lambda x: '%.8f' % x)
pd.set_option("display.max_columns", None)

orderLogFormat = "/root/violet_algo_actor/regtests/reg_ubuntu/reg_1/Logs/orderLog_{}.csv"
positionLogFormat = "/root/violet_algo_actor/regtests/reg_ubuntu/reg_1/Logs/positionLog_{}.csv"
accountingLogFormat = "/root/violet_algo_actor/regtests/reg_ubuntu/reg_1/Logs/accountingLog_{}.csv"

dates = [20230301,
         20230302,
         20230303,
         20230306,
         20230307,
         20230308,
         20230309,
         20230310,
         20230313,
         20230314,
         20230315,
         20230316,
         20230317,
         20230320,
         20230321,
         20230322
         #  20230323,
         #  20230324,
         #  20230327,
         #  20230328,
         #  20230329,
         #  20230330,
         #  20230331,
         #  20230403
         ]

md_bod_format = '/com_md_opt_chn/md_bod/locale_default/source_default/1.0.0/daily/{}/{}.parquet'
md_bod_pool = 'com_md_opt_chn'
md_bod_ns = 'md_bod'

md_eod_format = '/com_md_opt_chn/md_eod/locale_default/tianruan/1.0.1/daily/{}/{}.parquet'
md_eod_pool = 'com_md_opt_chn'
md_eod_ns = 'md_eod'


class params:
    def __init__(self, row: pd.Series):
        self.jkey = row["jkey"]
        self.margin_unit = row["live_margin_unit"]
        self.long_open_unit = row["long_open_unit"]
        self.short_open_unit = row["short_open_unit"]
        self.long_close_unit = row["long_close_unit"]
        self.short_close_unit = row["short_close_unit"]
        self.long_openS_unit = row["long_openS_unit"]
        self.short_openS_unit = row["short_openS_unit"]
        self.long_closeS_unit = row["long_closeS_unit"]
        self.short_closeS_unit = row["short_closeS_unit"]
        self.multiplier = row["multiplier"]


calc_nav = list()
calc_cum_amt_buy = list()
calc_cum_amt_sell = list()
calc_cum_amt_trade = list()
calc_cash = list()
calc_position_worth = list()
calc_comm_fee = list()
calc_margin = list()
accounting_nav = list()
accounting_cash = list()
accounting_position_worth = list()
accounting_margin = list()

cash = 0
position_worth = 0
margin = 0
carry_over_state = pd.DataFrame(
    {'jkey': [], 'inv_S': []})

for date in dates:
    print(date)

    init_nav = 10000000

    print(orderLogFormat.format(date))
    print(positionLogFormat.format(date))
    print(accountingLogFormat.format(date))
    print(md_bod_format.format(date//10000, date))
    print(md_eod_format.format(date//10000, date))

    orderLog = pd.read_csv(orderLogFormat.format(date))
    positionLog = pd.read_csv(positionLogFormat.format(date))
    accountingLog = pd.read_csv(accountingLogFormat.format(date))

    mdbod = read_file(md_bod_format.format(
        date//10000, date), md_bod_pool, md_bod_ns)
    mdeod = read_file(md_eod_format.format(
        date//10000, date), md_eod_pool, md_eod_ns)

    # rebalance
    carry_over_state = carry_over_state.merge(
        mdbod[["jkey", "live_margin_unit"]], how='left', on=["jkey"])
    if carry_over_state.empty == False:
        margin = (carry_over_state["inv_S"] *
                  carry_over_state["live_margin_unit"]).cumsum().iloc[-1]
    cash = init_nav - position_worth - margin
    print("rebalance")
    print("cash =", cash)
    print("position worth =", position_worth)
    print("margin =", margin)
    sod_position_worth = position_worth
    sod_margin = margin

    # print(orderLog)
    multipliers = positionLog.drop_duplicates(["jkey"], keep="first")[[
        "jkey", "multiplier"]]
    settle_prices = mdeod.drop_duplicates(
        ["jkey"], keep="first")[["jkey", "settle"]]
    # print(positionLog)
    # print(accountingLog)

    mdbod = multipliers.merge(mdbod, how='left', on=["jkey"])
    mdbod = mdbod.merge(settle_prices, how='left', on=["jkey"])
    mdbod = mdbod[["jkey", "multiplier", "live_margin_unit", "settle"]]

    # print(mdbod)

    # print("order", sorted(list(set(orderLog["jkey"]))))
    # print("position", sorted(list(set(positionLog["jkey"]))))
    # print(sorted(list(paramdict.keys())))

    # cum trade amount
    # orderLog = orderLog.merge(
    #     mdbod[["jkey", "multiplier"]], how='left', on=["jkey"])
    # orderLog["trade_amt"] = np.where(orderLog.updateType == 4, np.where(
    #     orderLog.orderSide == -1, 1, -1), 0) * orderLog["tradePrice"] * orderLog["qtyFilled"] * orderLog["multiplier"]

    # comm fee
    orderLog['comm_fee'] = np.where(orderLog.updateType == 4,
                                    np.where((orderLog.orderSide == -1) &
                                             (orderLog.offsetType == 1), 0, 1.8), 0) * orderLog["qtyFilled"]
    # cum_trade_amt = orderLog.groupby(
    #     "jkey")[["trade_amt"]].sum().reset_index(drop=False)
    # cum_trade_amt = cum_trade_amt["trade_amt"].cumsum().iloc[-1]
    cum_comm_fee = orderLog.groupby(
        "jkey")[["comm_fee"]].sum().reset_index(drop=False)
    cum_comm_fee = cum_comm_fee["comm_fee"].cumsum().iloc[-1]

    mdbod = mdbod[["jkey", "live_margin_unit", "settle"]]

    # calculate from last states
    groupJkey = positionLog.groupby("jkey")
    lastStatus = groupJkey.last()
    lastStatus = lastStatus.merge(mdbod, how='left', on=["jkey"])
    cum_amt_buy = lastStatus["cumAmountBuy"].cumsum().iloc[-1]
    cum_amt_sell = lastStatus["cumAmountSell"].cumsum().iloc[-1]
    lastStatus["sell_minus_buy"] = lastStatus["cumAmountSell"] - \
        lastStatus["cumAmountBuy"]
    position_worth = ((lastStatus["inv_L"] - lastStatus["inv_S"]) *
                      lastStatus["settle"] * lastStatus["multiplier"]).cumsum().iloc[-1]
    margin = (lastStatus["inv_S"] *
              lastStatus["live_margin_unit"]).cumsum().iloc[-1]

    carry_over_state = pd.DataFrame(
        {'jkey': [], 'inv_S': []})
    carry_over_state["jkey"] = lastStatus["jkey"]
    carry_over_state["inv_S"] = np.where(
        lastStatus["inv_L"] > lastStatus["inv_S"], 0, lastStatus["inv_S"]-lastStatus["inv_L"])

    print("calc nav =", init_nav - cum_comm_fee +
          cum_amt_sell - cum_amt_buy + position_worth - sod_position_worth)
    print("calc nav plus =", cash - cum_comm_fee + cum_amt_sell -
          cum_amt_buy + position_worth + margin)
    print("calc cum amt buy =", cum_amt_buy)
    print("calc cum amt sell =", cum_amt_sell)
    print("calc cum amt sell-buy =", cum_amt_sell - cum_amt_buy)
    # print("calc cum amt trade =", cum_trade_amt)
    print("calc cash =", cash - cum_comm_fee + cum_amt_sell -
          cum_amt_buy - (margin - sod_margin))
    print("calc position_worth =", position_worth)
    print("calc comm fee =", cum_comm_fee)
    print("calc margin =", margin)
    print("accounting nav =", accountingLog["nav"].iloc[-1])
    print("accounting cash =", accountingLog["cash"].iloc[-1])
    print("accounting position worth =",
          accountingLog["positionWorth"].iloc[-1])
    print("accounting margin =", accountingLog["margin"].iloc[-1])

    calc_nav.append(init_nav - cum_comm_fee +
                    cum_amt_sell - cum_amt_buy + position_worth - sod_position_worth)
    calc_cum_amt_buy.append(cum_amt_buy)
    calc_cum_amt_sell.append(cum_amt_sell)
    calc_cum_amt_trade.append(cum_amt_sell - cum_amt_buy)
    calc_cash.append(cash - cum_comm_fee + cum_amt_sell -
                     cum_amt_buy - (margin - sod_margin))
    calc_position_worth.append(position_worth)
    calc_comm_fee.append(cum_comm_fee)
    calc_margin.append(margin)
    accounting_nav.append(accountingLog["nav"].iloc[-1])
    accounting_cash.append(accountingLog["cash"].iloc[-1])
    accounting_position_worth.append(accountingLog["positionWorth"].iloc[-1])
    accounting_margin.append(accountingLog["margin"].iloc[-1])


stat = pd.DataFrame({'date': dates, 'calc_nav': calc_nav, 'calc_cum_amt_buy': calc_cum_amt_buy,
                     'calc_cum_amt_sell': calc_cum_amt_sell,
                    'calc_cum_amt_trade': calc_cum_amt_trade,
                     'calc_cash': calc_cash,
                     'calc_position_worth': calc_position_worth,
                     'calc_comm_fee': calc_comm_fee,
                     'calc_margin': calc_margin,
                     'accounting_nav': accounting_nav,
                     'accounting_cash': accounting_cash,
                     'accounting_position_worth': accounting_position_worth,
                     'accounting_margin': accounting_margin})

stat["nav_diff"] = stat["calc_nav"] - stat["accounting_nav"]
stat["cash_diff"] = stat["calc_cash"] - stat["accounting_cash"]
stat["position_worth_diff"] = stat["calc_position_worth"] - \
    stat["accounting_position_worth"]
stat["margin_diff"] = stat["calc_margin"] - stat["accounting_margin"]

print(stat)
