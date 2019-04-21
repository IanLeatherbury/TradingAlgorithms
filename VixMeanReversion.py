from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume, Returns
from quantopian.pipeline.data.quandl import yahoo_index_vix

import pandas as pd
import numpy as np
from scipy import stats

def initialize(context):    
    # Define context variables that can be accessed in other methods of
    # the algorithm.
    context.long_leverage = 0.5
    context.short_leverage = -0.5
    context.returns_lookback = 5
    
    context.vxx = sid(38054)
    context.xiv = sid(40516)
    context.SPY = sid(8554)
    context.SSO = sid(32270) 
    context.AGG = sid(25485)   
           
    # Rebalance on the first trading day of each week at 11AM.
    schedule_function(rebalance, 
                      date_rules.week_start(days_offset=0),
                      time_rules.market_open(hours = 1, minutes = 30))
    
    # Record tracking variables at the end of each day.
    schedule_function(record_vars,
                      date_rules.every_day(),
                      time_rules.market_close(minutes=1))
    
def rebalance(context,data):

    #SPY 200 days moving average
    moving_average = data.history(yahoo_index_vix.close, 'price', 200, '1d')[:-1].mean()
    
    #SPY 12 month returns
    prices = data.history(context.SPY, "price", bar_count=365, frequency="1d")
    pct_change = (prices.ix[-1] - prices.ix[0]) / prices.ix[0]

    #if (SPY > moving average) or 12 month average > 0, purchase
    if data.current(context.SPY, 'price') > moving_average and pct_change > 0 and data.can_trade(context.SPY) and data.can_trade(context.SSO):
        log.info("leverage = 1.75")
        # order(security, amount, style=LimitOrder(price))
        order_target_percent(context.SPY, .25)#, style=LimitOrder(data.current(context.SPY, 'price')*.75))
        order_target_percent(context.SSO, .75)#, style=LimitOrder(data.current(context.SSO, 'price')*.75))    
        
    elif data.current(context.SPY, 'price') > moving_average or pct_change > 0 and data.can_trade(context.SPY) and data.can_trade(context.SSO):    
        log.info("leverage = 1.25")
        order_target_percent(context.SPY, .75)#, style=LimitOrder(data.current(context.SPY, 'price')*.75))
        order_target_percent(context.SSO, .25)#, style=LimitOrder(data.current(context.SSO, 'price')*.75))     
        
    else:
        #sell
         log.info("sell SPY/SSO, buy AGG")
         order_target_percent(context.SPY, 0)#, style=LimitOrder(data.current(context.SPY, 'price')))
         order_target_percent(context.SSO, 0)#, style=LimitOrder(data.current(context.SSO, 'price'))) 
         order_target_percent(context.AGG, 1)

def record_vars(context, data):
    """
    This function is called at the end of each day and plots certain variables.
    """
            
    # Record and plot the leverage of our portfolio over time as well as the 
    # number of long and short positions. Even in minute mode, only the end-of-day 
    # leverage is plotted.
    # record(leverage = context.account.leverage, long_count=longs, short_count=shorts)