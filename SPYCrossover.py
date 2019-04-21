from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume
from quantopian.pipeline.filters.morningstar import Q500US
from quantopian.pipeline.factors import Returns
from quantopian.pipeline import CustomFactor
 
def initialize(context):
    """
    Called once at the start of the algorithm.
    """   
    # Rebalance every day, 1 hour after market open.
    schedule_function(my_rebalance, date_rules.month_end(), time_rules.market_open(hours=1))
     
    #S&P 500    
    context.SPY = sid(8554)

    #SSO = SPY at 2x leverage
    context.SSO = sid(32270) 
    
    #AGG iShares Core U.S. Aggregate Bond ETF, Barclays U.S. Aggregate Bond Index is a relatively stable index of high-quality,       #investment-grade (78% AAA-rated) bonds with an average maturity under five years.
    context.AGG = sid(25485)   
    
    context.stop_price_SPY = 0
    context.stop_price_SSO = 0
    context.stop_pct = 0.90
    
    set_slippage(slippage.VolumeShareSlippage(volume_limit=0.025, price_impact=0.1))
    set_commission(commission.PerShare(cost=0.0075, min_trade_cost=1))
 
def my_rebalance(context,data):
    
    #SPY 200 days moving average
    moving_average = data.history(context.SPY, 'price', 200, '1d')[:-1].mean()
    
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
        
         record(leverage = context.account.leverage)