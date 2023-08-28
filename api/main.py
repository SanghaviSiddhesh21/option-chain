#!/usr/bin/env python3
import subprocess
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,declarative_base
from sqlalchemy import Column,Integer,String,select
from sqlalchemy.types import DateTime,Date
import os
import datetime


current_dir=os.path.abspath(os.path.dirname(__file__))
SQLALCHEMY_DATABASE_URL="sqlite:///"+os.path.join(current_dir,"edelweiss_hackathon.sqlite3")
engine=create_engine(SQLALCHEMY_DATABASE_URL,connect_args={"check_same_thread":False})
Session=sessionmaker(autocommit=False,autoflush=False,bind=engine)
session=Session()
Base=declarative_base()

class All_expiry_symbols_meta(Base):
    __tablename__='all_expiry_symbols_meta'
    symbol=Column(String,nullable=False,primary_key=True)
    expiry_date=Column(DateTime,nullable=False,primary_key=True)
    call=Column(Integer,default=0)
    put=Column(Integer,default=0)
    future=Column(Integer,default=0)

class Futures(Base):
    __tablename__='futures'
    symbol=Column(String,nullable=False,primary_key=True)
    expiry_date=Column(DateTime,nullable=False,primary_key=True)
    LTP=Column(Integer,nullable=False)
    LTQ=Column(Integer,nullable=False) 
    totalTradedVolume=Column(Integer,nullable=False)
    bestBid=Column(Integer,nullable=False) 
    bestAsk=Column(Integer,nullable=False)
    bestBidQty=Column(Integer,nullable=False) 
    bestAskQty=Column(Integer,nullable=False)
    openInterest=Column(Integer,nullable=False) 
    timestamp=Column(String,nullable=False)
    sequence=Column(Integer,nullable=False)
    prevClosePrice=Column(Integer,nullable=False) 
    prevOpenInterest=Column(Integer,nullable=False)

class Calls(Base):
    __tablename__='calls'
    symbol=Column(String,nullable=False,primary_key=True)
    expiry_date=Column(DateTime,nullable=False,primary_key=True)
    strike_price=Column(Integer,nullable=False,primary_key=True)
    LTP=Column(Integer,nullable=False)
    LTQ=Column(Integer,nullable=False) 
    totalTradedVolume=Column(Integer,nullable=False)
    bestBid=Column(Integer,nullable=False) 
    bestAsk=Column(Integer,nullable=False)
    bestBidQty=Column(Integer,nullable=False) 
    bestAskQty=Column(Integer,nullable=False)
    openInterest=Column(Integer,nullable=False) 
    timestamp=Column(String,nullable=False)
    sequence=Column(Integer,nullable=False)
    prevClosePrice=Column(Integer,nullable=False) 
    prevOpenInterest=Column(Integer,nullable=False)

class Puts(Base):
    __tablename__='puts'
    symbol=Column(String,nullable=False,primary_key=True)
    expiry_date=Column(DateTime,nullable=False,primary_key=True)
    strike_price=Column(Integer,nullable=False,primary_key=True)
    LTP=Column(Integer,nullable=False)
    LTQ=Column(Integer,nullable=False) 
    totalTradedVolume=Column(Integer,nullable=False)
    bestBid=Column(Integer,nullable=False) 
    bestAsk=Column(Integer,nullable=False)
    bestBidQty=Column(Integer,nullable=False) 
    bestAskQty=Column(Integer,nullable=False)
    openInterest=Column(Integer,nullable=False) 
    timestamp=Column(String,nullable=False)
    sequence=Column(Integer,nullable=False)
    prevClosePrice=Column(Integer,nullable=False) 
    prevOpenInterest=Column(Integer,nullable=False)

class Indexes(Base):
    __tablename__='indexes'
    symbol=Column(String,nullable=False,primary_key=True)
    LTP=Column(Integer,nullable=False)
    LTQ=Column(Integer,nullable=False) 
    totalTradedVolume=Column(Integer,nullable=False)
    bestBid=Column(Integer,nullable=False) 
    bestAsk=Column(Integer,nullable=False)
    bestBidQty=Column(Integer,nullable=False) 
    bestAskQty=Column(Integer,nullable=False)
    openInterest=Column(Integer,nullable=False) 
    timestamp=Column(String,nullable=False)
    sequence=Column(Integer,nullable=False)
    prevClosePrice=Column(Integer,nullable=False) 
    prevOpenInterest=Column(Integer,nullable=False)


def convert_exiry_date_to_dateformat(expiry_date):
    months=['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    index=-100
    for month in months:
        x=expiry_date.find(month)
        if(x!=-1):
            index=x
            break
    month=expiry_date[index:index+3]
    months_dict={'JAN':'01','FEB':'02','MAR':'03','APR':'04','MAY':'05','JUN':'06','JUL':'07','AUG':'08','SEP':'09','OCT':'10','NOV':'11','DEC':'12'}
    month_num=months_dict[month]
    current_year = datetime.datetime.now().year
    current_year=str(current_year)[0:2]
    formated_expiry_date=str(current_year+expiry_date[index+3:]+"-"+month_num+"-"+expiry_date[:index])+" 15:15:00"
    date_format = "%Y-%m-%d %H:%M:%S"
    datetime_object = datetime.datetime.strptime(formated_expiry_date, date_format)
    return(datetime_object)

def name_expiration_date_and_strike_price_for_ce_pe(symbol):
    months=['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    index=-100
    for month in months:
        x=symbol.find(month)
        if(x!=-1 and x>7):
            index=x
            break
    expiration_date=symbol[index-2:index+5]
    strike_price=(symbol[index+5:-2])
    name=(symbol[:index-2])
    return(name,expiration_date,strike_price)

mastersheet={}
def mastersheet_updater(indiviual_dict):
    if(indiviual_dict['symbol'][-2:]=='XX'):
        future_expiration_date=convert_exiry_date_to_dateformat(indiviual_dict['symbol'][-9:-2])
        future_name=indiviual_dict['symbol'][:-9]
        future_meta_check=session.query(All_expiry_symbols_meta).filter_by(symbol=future_name,expiry_date=future_expiration_date).first()
        if(future_meta_check):
            future_meta_check.future=1
            session.commit()
        else:
            new_financial_meta=All_expiry_symbols_meta(symbol=future_name,expiry_date=future_expiration_date,future=1)
            try:
                session.add(new_financial_meta)
            except Exception as error:
                print(error)
                session.rollback()
            session.commit()
        exists = session.query(Futures).filter_by(symbol=future_name,expiry_date=future_expiration_date).first()
        if(exists):
            exists.LTP=indiviual_dict['LTP']
            exists.LTQ=indiviual_dict['LTQ']
            exists.totalTradedVolume=indiviual_dict['totalTradedVolume']
            exists.bestBid=indiviual_dict['bestBid']
            exists.bestAsk=indiviual_dict['bestAsk']
            exists.bestBidQty=indiviual_dict['bestBidQty']
            exists.bestAskQty=indiviual_dict['bestAskQty']
            exists.openInterest= indiviual_dict['openInterest']
            exists.timestamp= str(indiviual_dict['timestamp']) 
            exists.sequence=indiviual_dict['sequence']
            exists.prevClosePrice=indiviual_dict['prevClosePrice']
            exists.prevOpenInterest= indiviual_dict['prevOpenInterest']
            session.commit()
        else:
            new_financial = Futures(symbol=future_name,expiry_date=future_expiration_date,LTP=indiviual_dict['LTP'], LTQ=indiviual_dict['LTQ'],totalTradedVolume=indiviual_dict['totalTradedVolume'],bestBid=indiviual_dict['bestBid'],bestAsk=indiviual_dict['bestAsk'],bestBidQty=indiviual_dict['bestBidQty'],bestAskQty=indiviual_dict['bestAskQty'], openInterest= indiviual_dict['openInterest'],timestamp= str(indiviual_dict['timestamp']), sequence=indiviual_dict['sequence'],prevClosePrice= indiviual_dict['prevClosePrice'],prevOpenInterest= indiviual_dict['prevOpenInterest'])
            try:
                session.add(new_financial)
                session.flush()
            except Exception as error:
                print(error)
                session.rollback()
            session.commit()
    elif(indiviual_dict['symbol'][-2:]=='CE'):
        (call_name,call_expiration_date,call_strike_price)=name_expiration_date_and_strike_price_for_ce_pe(indiviual_dict['symbol'])
        call_expiration_date=convert_exiry_date_to_dateformat(call_expiration_date)
        call_meta_check=session.query(All_expiry_symbols_meta).filter_by(symbol=call_name,expiry_date=call_expiration_date).first()
        if(call_meta_check):
            call_meta_check.call=1
            session.commit()
        else:
            new_call_meta=All_expiry_symbols_meta(symbol=call_name,expiry_date=call_expiration_date,call=1)
            try:
                session.add(new_call_meta)
            except Exception as error:
                print(error)
                session.rollback()
            session.commit()
        exists = session.query(Calls).filter_by(symbol=call_name,expiry_date=call_expiration_date,strike_price=call_strike_price).first()
        if(exists):
            exists.LTP=indiviual_dict['LTP']
            exists.LTQ=indiviual_dict['LTQ']
            exists.totalTradedVolume=indiviual_dict['totalTradedVolume']
            exists.bestBid=indiviual_dict['bestBid']
            exists.bestAsk=indiviual_dict['bestAsk']
            exists.bestBidQty=indiviual_dict['bestBidQty']
            exists.bestAskQty=indiviual_dict['bestAskQty']
            exists.openInterest= indiviual_dict['openInterest']
            exists.timestamp= str(indiviual_dict['timestamp']) 
            exists.sequence=indiviual_dict['sequence']
            exists.prevClosePrice=indiviual_dict['prevClosePrice']
            exists.prevOpenInterest= indiviual_dict['prevOpenInterest']
            session.commit()
        else:
            new_call = Calls(symbol=call_name,expiry_date=call_expiration_date,strike_price=call_strike_price,LTP=indiviual_dict['LTP'], LTQ=indiviual_dict['LTQ'],totalTradedVolume=indiviual_dict['totalTradedVolume'],bestBid=indiviual_dict['bestBid'],bestAsk=indiviual_dict['bestAsk'],bestBidQty=indiviual_dict['bestBidQty'],bestAskQty=indiviual_dict['bestAskQty'], openInterest= indiviual_dict['openInterest'],timestamp= str(indiviual_dict['timestamp']), sequence=indiviual_dict['sequence'],prevClosePrice= indiviual_dict['prevClosePrice'],prevOpenInterest= indiviual_dict['prevOpenInterest'])
            try:
                session.add(new_call)
                session.flush()
            except Exception as error:
                print(error)
                session.rollback()
            session.commit()
    elif(indiviual_dict['symbol'][-2:]=='PE'):
        (put_name,put_expiration_date,put_strike_price)=name_expiration_date_and_strike_price_for_ce_pe(indiviual_dict['symbol'])
        put_expiration_date=convert_exiry_date_to_dateformat(put_expiration_date)
        put_meta_check=session.query(All_expiry_symbols_meta).filter_by(symbol=put_name,expiry_date=put_expiration_date).first()
        if(put_meta_check):
            put_meta_check.put=1
            session.commit()
        else:
            new_put_meta=All_expiry_symbols_meta(symbol=put_name,expiry_date=put_expiration_date,put=1)
            try:
                session.add(new_put_meta)
            except Exception as error:
                print(error)
                session.rollback()
            session.commit()
        exists = session.query(Puts).filter_by(symbol=put_name,expiry_date=put_expiration_date,strike_price=put_strike_price).first()
        if(exists):
            exists.LTP=indiviual_dict['LTP']
            exists.LTQ=indiviual_dict['LTQ']
            exists.totalTradedVolume=indiviual_dict['totalTradedVolume']
            exists.bestBid=indiviual_dict['bestBid']
            exists.bestAsk=indiviual_dict['bestAsk']
            exists.bestBidQty=indiviual_dict['bestBidQty']
            exists.bestAskQty=indiviual_dict['bestAskQty']
            exists.openInterest= indiviual_dict['openInterest']
            exists.timestamp= str(indiviual_dict['timestamp']) 
            exists.sequence=indiviual_dict['sequence']
            exists.prevClosePrice=indiviual_dict['prevClosePrice']
            exists.prevOpenInterest= indiviual_dict['prevOpenInterest']
            session.commit()
        else:
            new_put = Puts(symbol=put_name,expiry_date=put_expiration_date,strike_price=put_strike_price,LTP=indiviual_dict['LTP'], LTQ=indiviual_dict['LTQ'],totalTradedVolume=indiviual_dict['totalTradedVolume'],bestBid=indiviual_dict['bestBid'],bestAsk=indiviual_dict['bestAsk'],bestBidQty=indiviual_dict['bestBidQty'],bestAskQty=indiviual_dict['bestAskQty'], openInterest= indiviual_dict['openInterest'],timestamp= str(indiviual_dict['timestamp']), sequence=indiviual_dict['sequence'],prevClosePrice= indiviual_dict['prevClosePrice'],prevOpenInterest= indiviual_dict['prevOpenInterest'])
            try:
                session.add(new_put)
                session.flush()
            except Exception as error:
                print(error)
                session.rollback()
            session.commit()
    else:
        exists = session.query(Indexes).filter_by(symbol=indiviual_dict['symbol']).first()
        if(exists):
            exists.LTP=indiviual_dict['LTP']
            exists.LTQ=indiviual_dict['LTQ']
            exists.totalTradedVolume=indiviual_dict['totalTradedVolume']
            exists.bestBid=indiviual_dict['bestBid']
            exists.bestAsk=indiviual_dict['bestAsk']
            exists.bestBidQty=indiviual_dict['bestBidQty']
            exists.bestAskQty=indiviual_dict['bestAskQty']
            exists.openInterest= indiviual_dict['openInterest']
            exists.timestamp= str(indiviual_dict['timestamp']) 
            exists.sequence=indiviual_dict['sequence']
            exists.prevClosePrice=indiviual_dict['prevClosePrice']
            exists.prevOpenInterest= indiviual_dict['prevOpenInterest']
            session.commit()
        else:
            new_index = Indexes(symbol=indiviual_dict['symbol'],LTP=indiviual_dict['LTP'], LTQ=indiviual_dict['LTQ'],totalTradedVolume=indiviual_dict['totalTradedVolume'],bestBid=indiviual_dict['bestBid'],bestAsk=indiviual_dict['bestAsk'],bestBidQty=indiviual_dict['bestBidQty'],bestAskQty=indiviual_dict['bestAskQty'], openInterest= indiviual_dict['openInterest'],timestamp= str(indiviual_dict['timestamp']), sequence=indiviual_dict['sequence'],prevClosePrice= indiviual_dict['prevClosePrice'],prevOpenInterest= indiviual_dict['prevOpenInterest'])
            try:
                session.add(new_index)
                session.flush()
            except Exception as error:
                print(error)
                session.rollback()
            session.commit()


def extract_dict(indiviual_line):
    indiviual_line=indiviual_line[23:-5]
    indiviual_line=indiviual_line.replace("=",":").replace("symbol","'symbol'").replace("LTP","'LTP'").replace("LTQ","'LTQ'").replace("totalTradedVolume","'totalTradedVolume'").replace("bestBid","'bestBid'").replace("bestAsk","'bestAsk'").replace("'bestBid'Qty","'bestBidQty'").replace("'bestAsk'Qty","'bestAskQty'").replace("openInterest","'openInterest'").replace("timestamp","'timestamp'").replace("sequence","'sequence'").replace("prevClosePrice","'prevClosePrice'").replace("prevOpenInterest","'prevOpenInterest'").replace("'", "\"")
    index=indiviual_line.find('"timestamp":')
    start=index+12
    end=''
    for i in range(len(indiviual_line[start:])):
        if indiviual_line[start+i]==',':
            end=i+start
            break
    final_string=indiviual_line[:start]+'"'+indiviual_line[start:end]+'"'+indiviual_line[end:]
    indiviual_dict = json.loads(final_string)
    mastersheet_updater(indiviual_dict)
    return(str(indiviual_dict))

def retrieve_lines_from_terminal():
    proc = subprocess.Popen('java -Ddebug=true -Dspeed=2 -classpath ./feed-play-1.0.jar hackathon.player.Main dataset.csv 9011', stdout=subprocess.PIPE)
    while True:
        line = proc.stdout.readline()
        if not line:
            break
        else: 
            extract_dict(str(line))
            pass

if __name__=='__main__':
    retrieve_lines_from_terminal()

