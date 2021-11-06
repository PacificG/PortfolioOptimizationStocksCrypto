from datetime import datetime
from concurrent import futures
import os
import pandas as pd
from pandas import DataFrame
import pandas_datareader.data as web
import argparse

def download_stock(stock, args, start_time, now_time):
	""" try to query the iex for a stock, if failed note with print """
	data = args.data
	years = args.years
	try:
		print(stock)
		stock_df = web.DataReader(stock,'yahoo', start_time, now_time)
		stock_df['Name'] = stock
		output_name = stock + f'_data{now_time.year}.csv'
		# check if folder named individual exits and create if not
		if not os.path.exists(f'individual_{data}_{now_time.year}_{years}years'):
			os.makedirs(f'individual_{data}_{now_time.year}_{years}years')
		stock_df.to_csv(f'individual_{data}_{now_time.year}_{years}years/' + output_name)
	except:
		bad_names.append(stock)
		print('bad: %s' % (stock))

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Download stock data from yahoo finance')
	parser.add_argument('--data', type=str, default='stock',choices=['stock', 'crypto'], help='stock or crypto')
	parser.add_argument('--years', type=str, default='5', help='Years of data to be fetched')
	parser.add_argument('--companies', type=list, default=['AAPL','MSFT','GOOG','AMZN','FB','INTC','CSCO','NVDA','AMD','TSLA','NFLX','BABA','SNAP','TWTR','BA','PYPL','AMD','TSLA','NFLX','BABA','SNAP','TWTR','BA','PYPL'], help='List of companies to be fetched')
	args = parser.parse_args()
	""" set the download window """
	now_time = datetime.now()
	start_time = datetime(now_time.year - int(args.years), now_time.month , now_time.day)

	""" list of s_anp_p companies """
	# s_and_p = list(pd.read_csv('data/constituents.csv')['Symbol'])
	s_and_p = args.companies
	bad_names =[] #to keep track of failed queries

	"""here we use the concurrent.futures module's ThreadPoolExecutor
		to speed up the downloads buy doing them in parallel 
		as opposed to sequentially """

	#set the maximum thread number
	max_workers = 50

	workers = min(max_workers, len(s_and_p)) #in case a smaller number of stocks than threads was passed in
	with futures.ThreadPoolExecutor(workers) as executor:
		res = executor.map(lambda x: download_stock(x, args, start_time, now_time), s_and_p)

	
	""" Save failed queries to a text file to retry """
	if len(bad_names) > 0:
		with open('failed_queries.txt','w') as outfile:
			for name in bad_names:
				outfile.write(name+'\n')

	#timing:
	finish_time = datetime.now()
	duration = finish_time - now_time
	minutes, seconds = divmod(duration.seconds, 60)
	print('getSandP_threaded.py')
	print(f'The threaded script took {minutes} minutes and {seconds} seconds to run.')
	#The threaded script took 0 minutes and 31 seconds to run.
