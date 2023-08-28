import random

def impliedVolatility(maturity,market_price,strikePrice,r,underlying_stock_price):
    iv_random=random.randint(1,1000)
    plus_minus=random.randint(1,2)
    if(plus_minus==1):
        print(iv_random)
        return('+'+str(iv_random/100))
    elif(plus_minus==2):
        print(iv_random)
        return('-'+str(iv_random/100)) 