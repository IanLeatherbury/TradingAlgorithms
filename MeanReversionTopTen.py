"""
Mean reversion algo using pipeline. 
1. Gets top returns for high dollar volume stocks.
2. Shorts and longs
3. Incorporates a Earnings Calendar Risk framework
4. Eliminates leveraged etfs
"""
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume, Returns
from quantopian.pipeline.data.eventvestor import EarningsCalendar
from quantopian.pipeline.factors.eventvestor import (
    BusinessDaysUntilNextEarnings,
    BusinessDaysSincePreviousEarnings
)
import numpy as np
    
def initialize(context):
    
    #set some values
    context.returns_lookback = 5
    context.long_leverage = 1.5
    context.short_leverage = -1.5
    
    # create the trading guard to avoid over-leveraged ETFs 
    # set_asset_restrictions(security_lists.restrict_leveraged_etfs)
    
    #attach pipeline using custom make_pipeline method
    attach_pipeline(make_pipeline(context), 'mean_reversion_algo')
    
    #set slippage and commission
    set_slippage(slippage.VolumeShareSlippage(volume_limit=0.025, price_impact=0.1))   
    set_commission(commission.PerShare(cost=0.0075, min_trade_cost=1))
    
    #schedule when to rebalance and record variables
    schedule_function(rebalance, date_rules.week_start(0), time_rules.market_open(hours=1, minutes=30))    
    schedule_function(record_vars, date_rules.every_day(), time_rules.market_close(minutes=1))

def make_pipeline(context):
    #create pipeline
    pipe = Pipeline()
    
    #use built in factor AverageDollarVolume to screen for liquid stocks
    avg_dollar_volume = AverageDollarVolume(window_length=1)
    pipe.add(avg_dollar_volume, 'avg_dollar_volume')
    
    #use built in factor Returns to get returns over the recent_lookback window
    recent_returns = Returns(window_length=context.returns_lookback)
    pipe.add(recent_returns, 'recent_returns')
    
    #filter out stocks in the top % of dollar volume
    high_dollar_volume = avg_dollar_volume.percentile_between(95,100)
    
    #rank the recent returns in the high dollar volume group and add to pipe
    pipe.add(recent_returns.rank(mask=high_dollar_volume), 'recent_returns_rank') 
    
    #get stocks with the highest and lowest returns in the high dollar volume         group
    low_returns = recent_returns.percentile_between(0,10,mask=high_dollar_volume)
    
    high_returns = recent_returns.percentile_between(90,100,mask=high_dollar_volume)            
             
    #add high and low returns as columns to pipeline for easier data mgmt        
    pipe.add(high_returns, 'high_returns')
    pipe.add(low_returns, 'low_returns')    
    
    ######Earnings Announcement Risk Framework#############
    # https://www.quantopian.com/data/eventvestor/earnings_calendar
    # EarningsCalendar.X is the actual date of the announcement
    # E.g. 9/12/2015
    pipe.add(EarningsCalendar.next_announcement.latest, 'next')
    pipe.add(EarningsCalendar.previous_announcement.latest, 'prev')
    
    # BusinessDaysX is the integer days until or after the closest
    # announcement. So if AAPL had an earnings announcement yesterday,
    # prev_earnings would be 1. If it's the day of, it will be 0.
    # For BusinessDaysUntilNextEarnings(), it is common that the value
    # is NaaN because we typically don't know the precise date of an
    # earnings announcement until about 15 days before
    ne = BusinessDaysUntilNextEarnings()
    pe = BusinessDaysSincePreviousEarnings()
    pipe.add(ne, 'next_earnings')
    pipe.add(pe, 'prev_earnings')
    
    # The number of days before/after an announcement that you want to
    # avoid an earnings for.
    # pipe.set_screen ( (ne.isnan() | (ne > 3)) & (pe > 3) )    
    
    #####Set Pipe based on the desired framework######             
    #screen the pipeline by the high and low returns(and high dollar volume, implicitly
    pipe.set_screen(high_returns | low_returns | ( (ne.isnan() | (ne > 3)) & (pe > 3) ) ) 
    # pipe.set_screen(high_returns | low_returns) 
    
    return pipe

def before_trading_start(context, data):
    """
    Called every day before market open. This is where we get our stocks that         made it through the pipeline.
    """
    #pipeline_output returns a pandas dataframe that has columns for each factor we added to the pipeline, using pipe.add(), and has a row for each security that made it through
    context.output = pipeline_output('mean_reversion_algo')
    
    context.results = pipeline_output('mean_reversion_algo').iloc[:200]
    log.info(context.results.iloc[:5])
    
    #get the securities we want to long from our pipeline output
    #note on python syntax. context.output[] returns the pandas dataframe. context.output[context.output['column']] returns a particular column.
    context.long_secs = context.output[context.output['low_returns']]
    
    #get our shorts securities
    context.short_secs = context.output[context.output['high_returns']]
    
    #get our list of securities for the day
    context.security_list = context.short_secs.index.union(context.long_secs.index).tolist()
    
    #convert them to a set, for faster lookup
    context.security_set = set(context.security_list)
    
def assign_weights(context):

    #assign weights equally relative to leverage
    #example: (.5 leverage)/(17 stocks) = .029 leverage per stock
    context.long_weight = context.long_leverage/len(context.long_secs)
    
    #assign short weights
    context.short_weight = context.short_leverage/len(context.short_secs)

def rebalance(context, data):
    
    #assign leverage to each security
    assign_weights(context)
    
    #get our open_orders, so we don't over order
    open_orders = get_open_orders()
    
    #order long and short securities
    for sec in context.security_list:
        if sec not in open_orders and data.can_trade(sec) and sec not in security_lists.leveraged_etf_list:
           if sec in context.long_secs.index:
               order_target_percent(sec, context.long_weight)
           elif sec in context.short_secs.index:
               order_target_percent(sec, context.short_weight)
               
    #sell everything that's not in the list
    for sec in context.portfolio.positions:
        if sec not in context.security_set and data.can_trade(sec):
            order_target_percent(sec, 0)
            
    #log this week's long and short orders            
    log.info("This week's longs: "+", ".join([long_.symbol for long_ in context.long_secs.index]))
    log.info("This week's shorts: "+", ".join([short_.symbol for short_ in context.short_secs.index]))
    
def record_vars(context, data):
    
    longs = shorts = 0
    
    for position in context.portfolio.positions.itervalues():
        if position.amount > 0:
            longs += 1  
        if position.amount < 0:
            shorts += 1
            
    record(leverage = context.account.leverage, long_counts = longs, short_count = shorts)            

def handle_data(context,data):
    pass