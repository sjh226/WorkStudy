import pandas as pd
import numpy as np
import pyodbc
import sys
import matplotlib.pyplot as plt


def action_pull():
	try:
		connection = pyodbc.connect(r'Driver={SQL Server Native Client 11.0};'
									r'Server=SQLDW-L48.BP.Com;'
									r'Database=TeamOptimizationEngineering;'
									r'trusted_connection=yes'
									)
	except pyodbc.Error:
		print("Connection Error")
		sys.exit()

	cursor = connection.cursor()
	SQLCommand = ("""
		SELECT	*
		  FROM [TeamOptimizationEngineering].[Reporting].[ActionListHistory] AL
	""")

	cursor.execute(SQLCommand)
	results = cursor.fetchall()

	df = pd.DataFrame.from_records(results)
	connection.close()

	try:
		df.columns = pd.DataFrame(np.matrix(cursor.description))[0]
	except:
		df = None
		print('Dataframe is empty')

	return df.drop_duplicates()

def pm_dist(df):
	plt.close()
	fig, ax = plt.subplots(1, 1, figsize=(10, 10))

	df.loc[:, 'pm'] = df.loc[:, 'Comment'].str.contains('pm').astype(int)

	month_df = df.loc[df['Action Date'] < '2018-06-01', ['Action Date', 'pm']]
	month_df.loc[:, 'Action Date'] = pd.to_datetime(month_df.loc[:, 'Action Date'])

	month_df = month_df.groupby(pd.Grouper(freq='M', key='Action Date')).sum()
	months = ['Jan 2017', 'Feb 2017', 'Mar 2017', 'Apr 2017', 'May 2017', 'Jun 2017', \
			  'Jul 2017', 'Aug 2017', 'Sep 2017', 'Oct 2017', 'Nov 2017', 'Dec 2017', \
			  'Jan 2018', 'Feb 2018', 'Mar 2018', 'Apr 2018', 'May 2018']

	ax.bar(range(month_df.shape[0]), month_df['pm'].values, .8, color='#51b57e')
	plt.xticks(range(month_df.shape[0]), months)
	plt.xticks(rotation='vertical')
	plt.title('PM Count by Month')
	plt.xlabel('Month and Year')
	plt.ylabel('PM Count')
	plt.savefig('figures/pm_dist.png')

def plunger_events(df):
	plt.close()
	fig, ax = plt.subplots(1, 1, figsize=(10, 10))

	df.loc[:, 'plunger'] = df.loc[:, 'Action Type - No count'].str.contains('Plgr|Vent').astype(int)

	p_df = df.loc[df['Action Date'] < '2018-06-01', ['Action Date', 'plunger']]
	p_df.loc[:, 'Action Date'] = pd.to_datetime(p_df.loc[:, 'Action Date'])
	p_df = p_df.groupby(pd.Grouper(freq='W-MON', key='Action Date')).sum()

	months = ['Jan 2017', 'Feb 2017', 'Mar 2017', 'Apr 2017', 'May 2017', 'Jun 2017', \
			  'Jul 2017', 'Aug 2017', 'Sep 2017', 'Oct 2017', 'Nov 2017', 'Dec 2017', \
			  'Jan 2018', 'Feb 2018', 'Mar 2018', 'Apr 2018', 'May 2018']

	ax.bar(range(p_df.shape[0]), p_df['plunger'].values, .8, color='#c18100')
	plt.xticks(list(range(p_df.shape[0]))[::5], months)
	plt.xticks(rotation='vertical')
	plt.title('Plunger Events by Week')
	plt.xlabel('Week')
	plt.ylabel('Count of Plunger Events')
	plt.savefig('figures/plunger_dist.png')

def plunger_insp(df):
	plt.close()
	fig, ax = plt.subplots(1, 1, figsize=(10, 10))

	df.loc[:, 'inspect'] = df.loc[:, 'Action Type - No count'].str.contains('Plgr. Insp.').astype(int)
	df.loc[:, 'change'] = df.loc[:, 'Action Type - No count'].str.contains('Plgr. Change').astype(int)

	p_df = df.loc[df['Action Date'] < '2018-06-01', ['Action Date', 'inspect', 'change']]
	p_df.loc[:, 'Action Date'] = pd.to_datetime(p_df.loc[:, 'Action Date'])
	p_df = p_df.groupby(pd.Grouper(freq='W-MON', key='Action Date')).sum()
	p_df = p_df.loc[p_df['inspect'] < 200, :]

	months = ['Jan 2017', 'Feb 2017', 'Mar 2017', 'Apr 2017', 'May 2017', 'Jun 2017', \
			  'Jul 2017', 'Aug 2017', 'Sep 2017', 'Oct 2017', 'Nov 2017', 'Dec 2017', \
			  'Jan 2018', 'Feb 2018', 'Mar 2018', 'Apr 2018', 'May 2018']

	ax.bar(range(p_df.shape[0]), p_df['inspect'].values, .8, color='#6b2ecc', \
		   alpha=.8, label='Plunger Inspections')
	ax.bar(range(p_df.shape[0]), p_df['change'].values, .8, color='#3a7bd1', \
		   alpha=.4, label='Plunger Changes')
	plt.xticks(list(range(p_df.shape[0]))[::5], months)
	plt.xticks(rotation='vertical')
	plt.legend()
	plt.title('Distrobution of Plunger Inspections and Changes by Week')
	plt.xlabel('Week')
	plt.ylabel('Count of Plunger-Related Events')
	plt.savefig('figures/plunger_inspect_clean.png')


if __name__ == '__main__':
	# action_df = action_pull()
	# action_df.to_csv('data/comment_action.csv')
	a_df = pd.read_csv('data/comment_action.csv', encoding='ISO-8859-1')

	# pm_dist(a_df[a_df['Comment'].notnull()])

	# plunger_events(a_df)
	plunger_insp(a_df)
