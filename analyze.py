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

def dispatch_pull():
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
		SELECT	ALH.Wellkey
				,ALH.BusinessUnit
				,ALH.Area
				,ALH.WellName
				,ALH.[Owner Nickname]
				,ALH.assetAPI
				,ALH.PriorityLevel
				,ALH.PriorityType
				,ALH.DispatchReason
				,ALH.Person_assigned
				,D.Person_assigned AS DispatchAssigned
				,ALH._id
				,ALH.[Action Date]
				,ALH.[Action Type - No count]
				,ALH.[Action Type]
				,ALH.[Action Type 1]
				,ALH.[Action Type 2]
				,ALH.Comment
				,ALH.CommentAction
		  FROM [TeamOptimizationEngineering].[Reporting].[ActionListHistory] ALH
		  JOIN [OperationsDataMart].[Dimensions].[Wells] W
		    ON W.Wellkey = ALH.Wellkey
		  LEFT OUTER JOIN (SELECT	FacilityKey
									,D.LocationID
									,CalcDate
									,SiteName
									,PriorityLevel
									,Reason
									,Person_assigned
									,Job_Rank
							 FROM [TeamOptimizationEngineering].[dbo].[L48_Dispatch] D
						   INNER JOIN
								  (SELECT	LocationID
											,MAX(CalcDate) AS MDate
									  FROM [TeamOptimizationEngineering].[dbo].[L48_Dispatch]
									  GROUP BY LocationID, CAST(CalcDate AS DATE)) AS MD
							ON	MD.LocationID = D.LocationID
							AND MD.MDate = D.CalcDate) D
		    ON D.FacilityKey = W.Facilitykey
			AND CAST(D.CalcDate AS DATE) = CAST(ALH.[Action Date] AS DATE)
		WHERE ALH.BusinessUnit IN ('North', 'West')
		AND D.Person_assigned IS NULL
		AND ALH.[Action Date] >= '02-14-2018'
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

def gauge_pull():
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
		SELECT	Wellkey
				,BusinessUnit
				,OwnerNTID
				,[Action Date]
				,[Action Type - No count]
				,Comment
				,CommentAction
		  FROM [TeamOptimizationEngineering].[Reporting].[ActionListHistory]
		  WHERE [Action Type - No count] = 'Gauge'
		     OR (CommentAction LIKE '%gaug%'
			 	AND CommentAction LIKE '%tank%'
				AND [Action Type - No count] = 'WM Completed')
		ORDER BY Wellkey, [Action Date]
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
	plt.title('Distribution of Plunger Inspections and Changes by Week')
	plt.xlabel('Week')
	plt.ylabel('Count of Plunger-Related Events')
	plt.savefig('figures/plunger_inspect_clean.png')

def gauge_events(df):
	plt.close()
	fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(10, 10))

	df['gauge'] = df.loc[:, 'Action Type - No count'].str.contains('Gauge').astype(int)
	df['wm'] = df.loc[:, 'Action Type - No count'].str.contains('WM').astype(int)

	g_df = df.loc[:, ['Wellkey', 'BusinessUnit', 'Action Date', 'gauge', 'wm']]
	g_df = g_df.groupby(['Wellkey', 'BusinessUnit', \
						 pd.Grouper(freq='D', key='Action Date')]).sum().reset_index()

	for bu, axis in zip(g_df['BusinessUnit'].unique(), [ax1, ax2, ax3, ax4]):
		wm_df = g_df[(g_df['gauge'] == 0) & (g_df['wm'] == 1) & (g_df['BusinessUnit'] == bu)].groupby(\
				pd.Grouper(freq='M', key='Action Date')).count().reset_index()
		both_df = g_df[(g_df['gauge'] == 1) & (g_df['wm'] == 1) & (g_df['BusinessUnit'] == bu)].groupby(\
				pd.Grouper(freq='M', key='Action Date')).count().reset_index()
		gauge_df = g_df[(g_df['gauge'] == 1) & (g_df['wm'] == 0) & (g_df['BusinessUnit'] == bu)].groupby(\
				pd.Grouper(freq='M', key='Action Date')).count().reset_index()

		axis.bar(both_df['Action Date'].values, both_df['gauge'].values, 12,
				 color='#0e2bce', alpha=.6, label='Matched Gauge Report')
		axis.bar(wm_df['Action Date'].values + np.timedelta64(12, 'D'),
				 wm_df['gauge'].values, 12, color='#000d56', alpha=.4,
				 label='Unmatched WM Gauge')
		axis.set_title('{}'.format(bu))
		axis.legend()
		axis.xaxis.set_visible(True)
		axis.yaxis.set_visible(True)
		plt.setp(axis.xaxis.get_majorticklabels(), rotation=90)
		axis.set_xlabel('Month')
		axis.set_ylabel('Count of Events')

	plt.suptitle('WM Entries Compared to Gauging Events', y=.995)
	plt.tight_layout()
	plt.savefig('figures/gauging_gap.png')

	extra_wm_df = g_df[(g_df['gauge'] == 0) & (g_df['wm'] == 1)]\
				  .groupby('BusinessUnit').sum().reset_index()
	match_df = g_df[(g_df['gauge'] == 1) & (g_df['wm'] == 1)]\
			   .groupby('BusinessUnit').sum().reset_index()
	extra_wm_df.rename(index=str, columns={'wm':'wm_entry'}, inplace=True)
	match_df.rename(index=str, columns={'gauge':'matched_entry'}, inplace=True)

	g_df = extra_wm_df.merge(match_df, on='BusinessUnit')
	g_df = g_df.loc[:, ['BusinessUnit', 'wm_entry', 'matched_entry']]

	gauge_table_plot(g_df)
	return g_df

def gauge_table_plot(df):
	plt.close()
	fig, ax = plt.subplots(1, 1, figsize=(7, 2))

	ax.axis('off')
	ax.table(cellText=df.values, colLabels=['BusinessUnit', 'WM Gauging Activity without Report', \
											'Matched Gauge Report'], loc='center')
	plt.tight_layout()
	plt.title('Comparison of WM Entires to Those with Matched Events', y=.8)
	plt.savefig('figures/gauge_table.png')

def work_dist(df, graph_per='driver'):
	plt.close()
	fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(10, 10))

	drivers = {'east': 42, 'midcon': 61, 'north': 69, 'west': 140}
	wells = {'east': 880, 'midcon': 2853, 'north': 2003, 'west': 3834}

	grouped_df = df.groupby(['BusinessUnit', 'Action Type - No count'], as_index=False).count()

	for bu, axis in zip(df['BusinessUnit'].unique(), [ax1, ax2, ax3, ax4]):
		bu_df = grouped_df.loc[grouped_df['BusinessUnit'] == bu, :]

		bu_safety = bu_df.loc[(bu_df['Action Type - No count'] == 'Safety 2.0') |
							  (bu_df['Action Type - No count'] == 'Safety 3.0'),
							  '_id'].sum()
		safety_df = pd.DataFrame([['West', 'Safety', bu_safety]],
								 columns=bu_df.columns)
		bu_df = bu_df.append(safety_df)

		bu_df = bu_df.loc[(bu_df['Action Type - No count'] != 'Safety 2.0') &
						  (bu_df['Action Type - No count'] != 'Safety 3.0'), :]

		if graph_per == 'driver':
			divisor = drivers[bu.lower()]
		elif graph_per == 'well':
			divisor = wells[bu.lower()]
		axis.bar(bu_df['Action Type - No count'].values,
				 bu_df['_id'].values / divisor, .8,
				 color='#00b232', label='Action Type Counts')
		axis.set_title('{}'.format(bu))
		axis.xaxis.set_visible(True)
		axis.yaxis.set_visible(True)
		plt.setp(axis.xaxis.get_majorticklabels(), rotation=90)
		axis.set_xlabel('Action')
		axis.set_ylabel('Count of Events per {}'.format(graph_per.title()))

	plt.suptitle('Action Counts by BU (Excliding Dispatch)', y=.995)
	plt.tight_layout()
	plt.savefig('figures/nondispatch_work_{}.png'.format(graph_per))

def dispatch_wm(df):
	words = set(' '.join(list(df['CommentAction'].unique())).split())
	full_words = {'tank': 'Tank', 'autom': 'Automation', 'plunger': 'Plunger',
				  'equip': 'Equipment Failure', 'choke': 'Choke',
				  'site': 'Site Check', 'batteri': 'Batteries',
				  'fuel': 'Fuel Gas', 'comp': 'Compressor', 'gaug': 'Gauge',
				  'engin': 'Engine', 'rig': 'Rig Work', 'glycol': 'Glycol',
				  'rtu': 'RTU', 'pump': 'Pump', 'discharg': 'Discharge'}

	for word in ['check', 'dump', 'fluid', 'ga', 'troubleshoot', 'valv',
				 'level', 'oil', 'pm', 'surfac', 'replac']:
		words.remove(word)
	for word in words:
		df[word] = df.loc[:, 'CommentAction'].str.contains(word).astype(int)


	for bu in df['BusinessUnit'].unique():
		plt.close()

		count_df = df.loc[df['BusinessUnit'] == bu, :]

		word_dic = {}
		for word in words:
			word_dic[full_words[word]] = count_df[word].sum()

		plt.bar(word_dic.keys(), word_dic.values(), .8,
				color='#ff6523')

		plt.title('WM Entries Outside of Dispatch for {}'.format(bu))
		plt.xlabel('Action')
		plt.ylabel('Count of Actions')
		plt.xticks(rotation='vertical')
		plt.tight_layout()
		plt.savefig('figures/wm_dispatch_{}.png'.format(bu.lower()))

def gauge_counts(df):
	plt.close()

	count_df = df.groupby('BusinessUnit', as_index=False).count()
	drivers = {'east': 42, 'midcon': 61, 'north': 69, 'west': 140}
	for bu in count_df['BusinessUnit'].unique():
		count_df.loc[count_df['BusinessUnit'] == bu, 'drv'] = \
						count_df.loc[count_df['BusinessUnit'] == bu, 'Wellkey']\
		 				/ drivers[bu.lower()]

	plt.bar(count_df['BusinessUnit'], count_df['drv'], .8,
			color='#359333')

	plt.title('Gauge Events by BU')
	plt.xlabel('Business Unit')
	plt.ylabel('Count of Gauge Events per Worker')
	plt.tight_layout()
	plt.savefig('figures/bu_gauging.png')


if __name__ == '__main__':
	# action_df = action_pull()
	# action_df.to_csv('data/comment_action.csv')
	a_df = pd.read_csv('data/comment_action.csv', encoding='ISO-8859-1')

	gauge_df = gauge_pull()
	g_df = gauge_events(gauge_df)
	gauge_counts(gauge_df[['BusinessUnit', 'Wellkey']])

	# pm_dist(a_df[a_df['Comment'].notnull()])

	# plunger_events(a_df)
	# plunger_insp(a_df)

	# dis_df = dispatch_pull()
	# work_dis_df = dis_df[['BusinessUnit', '_id', 'Action Type - No count']]
	# for g_type in ['driver', 'well']:
	# 	work_dist(work_dis_df, g_type)

	# work_df = a_df.loc[(a_df['Action Type - No count'] != 'WM Completed') &
	# 						  (a_df['Action Type - No count'] != 'Gauge') &
	# 						  (a_df['Action Type - No count'] != 'SF'),
	# 				   ['BusinessUnit', '_id', 'Action Type - No count']]
	# for g_type in ['driver', 'well']:
	# 	work_dist(work_df, g_type)

	# dispatch_wm(dis_df.loc[(dis_df['Action Type - No count'] == 'WM Completed') &
	# 				   	   (dis_df['CommentAction'].notnull()), :])
