import pandas as pd
import numpy as np
import pyodbc
import sys
import matplotlib.pyplot as plt
from matplotlib import pylab


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
		DROP TABLE IF EXISTS #tmp;

		SELECT	[_id],
				[TankCode],
				[gaugeDate],
				[liquidAmount],
		      	ROW_NUMBER() OVER(PARTITION BY [tankcode] ORDER BY [gaugedate] DESC) AS [rk]
		INTO 	[#TMP]
		FROM    EDW.Enbase.GaugeData AS GD
		WHERE   [liquidAmount] IS NOT NULL;
	""")

	cursor.execute(SQLCommand)

	SQLCommand = ("""
		DROP TABLE IF EXISTS #CarryOverIds;

		SELECT 	[t1].[_id]
		  INTO 	[#CarryOverIds]
		  FROM  #TMP AS T
		INNER JOIN #TMP AS T1
				ON T1.RK = T.Rk - 1
		       AND T.tankCode = T1.tankCode;
	""")

	cursor.execute(SQLCommand)

	SQLCommand = ("""
		SELECT	*
		  FROM [TeamOptimizationEngineering].[Reporting].[ActionListHistory] AL
		WHERE [Action Date] >= '2017-03-30'
		  AND [Action Date] <= '2018-04-12'
		  AND _id NOT IN (SELECT *
		  					FROM #CarryOverIds)
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
		DROP TABLE IF EXISTS #tmp;

		SELECT	[_id],
				[TankCode],
				[gaugeDate],
				[liquidAmount],
		      	ROW_NUMBER() OVER(PARTITION BY [tankcode] ORDER BY [gaugedate] DESC) AS [rk]
		INTO 	[#TMP]
		FROM    EDW.Enbase.GaugeData AS GD
		WHERE   [liquidAmount] IS NOT NULL;
	""")

	cursor.execute(SQLCommand)

	SQLCommand = ("""
		DROP TABLE IF EXISTS #CarryOverIds;

		SELECT 	[t1].[_id]
		  INTO 	[#CarryOverIds]
		  FROM  #TMP AS T
		INNER JOIN #TMP AS T1
				ON T1.RK = T.Rk - 1
		       AND T.tankCode = T1.tankCode;
	""")

	cursor.execute(SQLCommand)

	SQLCommand = ("""
		SELECT	Wellkey
				,BusinessUnit
				,OwnerNTID
				,[Action Date]
				,[Action Type - No count]
				,Comment
				,CommentAction
		  FROM [TeamOptimizationEngineering].[Reporting].[ActionListHistory] ALH
		  WHERE [Action Type - No count] = 'Gauge'
		     OR (CommentAction LIKE '%gaug%'
			 	AND CommentAction LIKE '%tank%'
				AND [Action Type - No count] = 'WM Completed')
			 AND ALH._id NOT IN (SELECT *
			 					   FROM #CarryOverIds)
			 AND [Action Date] < '2018-05-01'
			 AND createdby NOT LIKE '%ibex%'
			 AND createdby NOT LIKE '%readyoil%'
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

def site_report(df, graph_per='driver'):
	plt.close()
	fig, ax = plt.subplots(1, 1, figsize=(4, 4))

	colors = {'east': '#128e14', 'midcon': '#b57800',
			  'north': '#7759f9', 'west': '#59b7f9'}

	grouped_df = df.groupby(['BusinessUnit', 'Action Type - No count'], as_index=False).mean()

	width = 0.25
	i = 0
	for bu in df['BusinessUnit'].unique():
		bu_df = grouped_df.loc[grouped_df['BusinessUnit'] == bu, :]

		ax.bar(1 + (width * i),
			   bu_df['agg_dur'].values / 60, width,
			   color=colors[bu.lower()], label=bu)
		i += 1

	plt.title('Time Spent on Site Reports')
	plt.xlabel('Business Unit')
	plt.ylabel('Average Minutes Spent')
	plt.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
	plt.legend()
	plt.tight_layout()
	plt.savefig('figures/site_report.png')

def work_dist(df, graph_per='driver'):
	plt.close()
	# fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(10, 10))
	fig, ax = plt.subplots(1, 1, figsize=(10, 10))

	drivers = {'east': 42, 'midcon': 61, 'north': 69, 'west': 140}
	wells = {'east': 880, 'midcon': 2853, 'north': 2003, 'west': 3834}
	colors = {'east': '#128e14', 'midcon': '#b57800',
			  'north': '#7759f9', 'west': '#59b7f9'}

	df.loc[(df['Action Type - No count'] == 'Safety 2.0') |
		   (df['Action Type - No count'] == 'Safety 3.0'),
		   'Action Type - No count'] = 'Safety'
	df.loc[(df['Action Type - No count'] == 'Plgr. Change') |
		   (df['Action Type - No count'] == 'Plgr. Insp.') |
		   (df['Action Type - No count'] == 'Plgr. Incomplete'),
		   'Action Type - No count'] = 'Plunger'
	df.loc[df['Action Type 1'] == 'Compressor - Gas Lift',
		   'Action Type - No count'] = 'Compressor'
	df.loc[(df['Action Type 1'] == 'Gas Scrubber') |
		   (df['Action Type 1'] == 'Gas Scrubber Separator') |
	  	   (df['Action Type 1'] == 'Sand Separator') |
		   (df['Action Type 1'] == 'Dehy') |
		   (df['Action Type 1'] == 'Fuel Gas/Start Gas') |
	  	   (df['Action Type 1'] == 'Heat Medium') |
	  	   (df['Action Type 1'] == 'Heater Treater') |
	  	   (df['Action Type 1'] == 'Instrument Air') |
	  	   (df['Action Type 1'] == 'Sales Valve') |
	  	   (df['Action Type 1'] == 'Sales Valve (PV)') |
	  	   (df['Action Type 1'] == 'Amine') |
	  	   (df['Action Type 1'] == 'Separator Inlet Valve (XV)'),
	  	   'Action Type - No count'] = 'Separator'
	df.loc[(df['Action Type 1'] == 'Tanks') |
		   (df['Action Type 1'] == 'Tanks/Pits') |
		   (df['Action Type 1'] == 'Water Transfer'),
	       'Action Type - No count'] = 'Liquids'
	df.loc[(df['Action Type 1'] == 'Pumping Unit') |
		   (df['Action Type 1'] == 'Recirc Pump'),
		   'Action Type - No count'] = 'Pumping System'
	df.loc[df['Action Type 1'] == 'Instrumentation',
		   'Action Type - No count'] = 'Instrumentation'
	df.loc[df['Action Type 1'] == 'Wellhead',
		   'Action Type - No count'] = 'Wellhead'
	df.loc[(df['Action Type - No count'] == 'WM Completed') &
		   (df['CommentAction'].str.contains('rtu')),
		   'Action Type - No count'] = 'RTU'
	df.loc[(df['Action Type - No count'] == 'WM Completed') &
		   (df['CommentAction'].str.contains('pump')),
		   'Action Type - No count'] = 'Pumping System'
	df.loc[(df['Action Type - No count'] == 'WM Completed') &
		   (df['CommentAction'].str.contains('pm')),
		   'Action Type - No count'] = 'PM'
	df.loc[(df['Action Type - No count'] == 'WM Completed') &
		   (df['CommentAction'].str.contains('plunger')),
		   'Action Type - No count'] = 'Plunger'
	df.loc[(df['Action Type - No count'] == 'WM Completed') &
		   (df['CommentAction'].str.contains('comp')),
		   'Action Type - No count'] = 'Compressor'

	df_short = df.loc[(df['Action Type - No count'] != 'Troubleshoot Charg. Sys.') &
					  (df['Action Type - No count'] != 'cIBatches') &
					  (df['Action Type - No count'] != 'documentSurfaceEquipment') &
					  (df['Action Type - No count'] != 'extBuildUp') &
					  (df['Action Type - No count'] != 'fluidShot') &
					  (df['Action Type - No count'] != 'pressureCheck') &
					  (df['Action Type - No count'] != 'snowRemoval') &
					  (df['Action Type - No count'] != 'soapSticks') &
					  (df['Action Type - No count'] != 'SF') &
					  (df['Action Type - No count'] != 'WM Completed') &
					  (df['Action Type - No count'] != 'rodPumpSpeedChange') &
					  (df['Action Type - No count'] != 'warmBootRTU'), :]

	df_short.loc[:, 'Action Date'] = pd.to_datetime(df_short.loc[:, 'Action Date'])
	days = (df_short['Action Date'].max() - df_short['Action Date'].min()).days

	grouped_df = df_short.groupby(['BusinessUnit', 'Action Type - No count'], as_index=False).sum()
	for bu in grouped_df['BusinessUnit'].unique():
		for event in grouped_df['Action Type - No count'].unique():
			if event not in grouped_df.loc[grouped_df['BusinessUnit'] == bu,
										   'Action Type - No count'].unique():
				grouped_df = grouped_df.append(pd.DataFrame([[bu, event, 0]],
											   columns = ['BusinessUnit',
											   			  'Action Type - No count',
														  'agg_dur']),
											   ignore_index=True)
	grouped_df.sort_values(['BusinessUnit', 'Action Type - No count'], inplace=True)

	events = sorted(df_short['Action Type - No count'].unique())

	i = 0
	index = np.arange(len(events))
	width = .225

	for bu in df['BusinessUnit'].unique():
		bu_df = grouped_df.loc[grouped_df['BusinessUnit'] == bu, :].sort_values('Action Type - No count')

		if graph_per == 'driver':
			divisor = drivers[bu.lower()] / 60
			graph_title = ' per Driver'
			scale = ''
			graph_save = 'driver'
		elif graph_per == 'well':
			divisor = wells[bu.lower()]
			graph_title = ' per Well'
			scale = ''
			graph_save = 'well'
		else:
			divisor = 1
			graph_title = ''
			scale = ''
			graph_save = 'total'

		ax.bar(index + (width * i),
			   bu_df['agg_dur'].values / divisor / 60 / 60 / days, width,
			   color=colors[bu.lower()], label=bu)
		ax.set_xticks(index + width / 2)
		ax.set_xticklabels(events)
		i += 1

	plt.title('Action Hours by BU (Excliding WM, Gauge, and SF)')
	plt.xlabel('Action')
	plt.ylabel('{}Minutes Spent per Event{} per Day'.format(scale, graph_title))
	plt.xticks(rotation='vertical')
	plt.legend()
	plt.tight_layout()
	plt.savefig('figures/action_hours_{}.png'.format(graph_save))

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

def action_count(df):
	plt.close()
	fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(10, 10))

	count_df = df.groupby(['BusinessUnit', 'Action Type - No count'], as_index=False).count()
	drivers = {'east': 42, 'midcon': 61, 'north': 69, 'west': 140}

	for bu, axis in zip(df['BusinessUnit'].unique(), [ax1, ax2, ax3, ax4]):
		bu_df = count_df.loc[count_df['BusinessUnit'] == bu, :]

		gauge = bu_df.loc[bu_df['Action Type - No count'] == 'Gauge', '_id'].values.sum()
		wm = bu_df.loc[bu_df['Action Type - No count'] == 'WM Completed', '_id'].values.sum()
		sf = bu_df.loc[bu_df['Action Type - No count'] == 'SF', '_id'].values.sum()
		other = bu_df.loc[(bu_df['Action Type - No count'] != 'Gauge') &
						  (bu_df['Action Type - No count'] != 'WM Completed') &
						  (bu_df['Action Type - No count'] != 'SF'), '_id'].values.sum()

		plot_dic = {'Gauge': gauge, 'WM': wm, 'SF': sf, 'Other': other}

		axis.bar(list(plot_dic.keys()),
				 np.array(list(plot_dic.values())) / drivers[bu.lower()],
				 .8, align='center',
				 color='#e5ca32', label='Action Type Counts')
		axis.set_title('{}'.format(bu))
		axis.xaxis.set_visible(True)
		axis.yaxis.set_visible(True)
		plt.setp(axis.xaxis.get_majorticklabels(), rotation=90)
		axis.set_ylabel('Count per Driver')

	plt.suptitle('Action Groupings by BU', y=.997)
	plt.tight_layout()
	plt.savefig('figures/bu_action_dist.png')

def stacked_actions(df_true, plot_type='count'):
	plt.close()
	fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(10, 10))
	df = df_true.copy()

	df.loc[(df['Action Type - No count'] == 'Safety 2.0') |
		   (df['Action Type - No count'] == 'Safety 3.0'),
		   'Action Type - No count'] = 'Safety'
	df.loc[(df['Action Type - No count'] == 'Plgr. Change') |
		   (df['Action Type - No count'] == 'Plgr. Insp.') |
		   (df['Action Type - No count'] == 'Plgr. Incomplete'),
		   'Action Type - No count'] = 'Plunger'
	df.loc[(df['Action Type - No count'] == 'WM Completed'),
		   'Action Type - No count'] = 'WM'
	df.loc[(df['Action Type - No count'] == 'soapSticks'),
		   'Action Type - No count'] = 'Soap Sticks'

	drivers = {'east': 42, 'midcon': 61, 'north': 69, 'west': 140}

	df['Action Date'] = pd.to_datetime(df['Action Date'])
	df['Action Date'] = df['Action Date'].map(lambda x:100*x.year + x.month)

	if plot_type == 'count':
		df = df.loc[:, ['BusinessUnit', 'Action Type - No count',
						'Action Date', 'Wellkey']]
		group_df = df.groupby(['BusinessUnit', 'Action Type - No count',
								 'Action Date'], as_index=False).count()
	elif plot_type == 'hours':
		df = df.loc[:, ['BusinessUnit', 'Action Type - No count',
						'Action Date', 'agg_dur']]
		group_df = df.groupby(['BusinessUnit', 'Action Type - No count',
								 'Action Date'], as_index=False).sum()
		group_df.rename(index=str, columns={'agg_dur': 'Wellkey'}, inplace=True)

	months = ['Mar 2017', 'Apr 2017', 'May 2017', 'Jun 2017', \
			  'Jul 2017', 'Aug 2017', 'Sep 2017', 'Oct 2017', 'Nov 2017', 'Dec 2017', \
			  'Jan 2018', 'Feb 2018', 'Mar 2018', 'Apr 2018', 'May 2018']

	month_vals = group_df['Action Date'].unique()

	for bu, axis in zip(df['BusinessUnit'].unique(), [ax1, ax2, ax3, ax4]):
		bu_df = group_df.loc[(group_df['BusinessUnit'] == bu) &
							 (group_df['Action Type - No count'].isin(
							  ['WM', 'Gauge', 'Site Report', 'SF',
							   'Safety', 'Plunger', 'Vent', 'Soap Sticks'])), :]

		colors = ['#09590d', '#0eb29f', '#0109a0', '#853cb2', '#ddd718',
				  '#dd0ef4', '#db1c1c', '#0fdbff']

		plot_x = {month: i for i, month in enumerate(sorted(month_vals))}

		bottoms = np.zeros(14)
		for i, action in enumerate(['WM', 'Gauge', 'Site Report', 'SF',
									'Safety', 'Plunger', 'Vent', 'Soap Sticks']):
			for month in month_vals:
				if month not in bu_df.loc[bu_df['Action Type - No count'] == action,
										  'Action Date'].unique():
					bu_df = bu_df.append(pd.DataFrame([[bu, action, month, 0]],
										 columns=['BusinessUnit',
										 		  'Action Type - No count',
												  'Action Date',
												  'Wellkey']))
			bu_df.sort_values(['Action Type - No count', 'Action Date'], inplace=True)
			plot_vals = bu_df.loc[bu_df['Action Type - No count'] == action,
								  'Wellkey'].values
			if plot_type == 'count':
				divisor = drivers[bu.lower()]
				height = 50
			elif plot_type == 'hours':
				divisor = (drivers[bu.lower()] * 60 * 60)
				height = 20

			if action == 'Gauge':
				this = bu_df.loc[bu_df['Action Type - No count'] == action, :]
				this['hours'] = this['Wellkey'] / divisor
				print(this)

			axis.bar(list(plot_x.values()),
					 plot_vals / divisor,
					 .8, bottom=bottoms, color=colors[i],
					 label=action)
			axis.set_xticks(list(plot_x.values()))
			axis.set_xticklabels(months)
			bottoms += (plot_vals / divisor)

		axis.set_title('{}'.format(bu))
		axis.set_ylim(0, height)
		# axis.set_ylim(0, max(bottoms) + height)
		plt.setp(axis.xaxis.get_majorticklabels(), rotation=90)
		axis.set_ylabel('{} per Driver'.format(plot_type.title()))

	plt.suptitle('Action {} by BU over Time'.format(plot_type.title()), y=.997)
	art = []
	lgd = pylab.legend(loc=9, bbox_to_anchor=(-0.1, -0.2), ncol=4)
	art.append(lgd)
	plt.tight_layout()
	pylab.savefig('figures/action_dist_{}.png'.format(plot_type),
				  additional_artists=art,
				  bbox_inches='tight')

def safety_plot(df_true):
	df = df_true.copy()

	plt.close()
	fig, ax1 = plt.subplots(1, 1, figsize=(6, 6))
	# ax2 = ax1.twinx()

	df['Action Date'] = pd.to_datetime(df['Action Date'])
	df['Action Date'] = df['Action Date'].map(lambda x:100*x.year + x.month)

	df.loc[(df['Action Type - No count'] == 'Safety 2.0') |
		   (df['Action Type - No count'] == 'Safety 3.0'),
		   'Action Type - No count'] = 'Safety'
	month_vals = df['Action Date'].unique()

	df = df.loc[df['Action Type - No count'] == 'Safety',
				['BusinessUnit', 'Action Type - No count', 'Action Date', 'agg_dur']]

	drivers = {'east': 42, 'midcon': 61, 'north': 69, 'west': 140}

	driver_df = pd.DataFrame(columns=df.columns)
	for bu in df['BusinessUnit'].unique():
		bu_df = df.loc[df['BusinessUnit'] == bu, :]
		bu_df.loc[:, 'agg_dur'] = bu_df.loc[:, 'agg_dur'] / drivers[bu.lower()]
		driver_df = driver_df.append(bu_df)

	months = ['March 2017', 'Apr 2017', 'May 2017', 'Jun 2017', \
			  'Jul 2017', 'Aug 2017', 'Sep 2017', 'Oct 2017', 'Nov 2017', 'Dec 2017', \
			  'Jan 2018', 'Feb 2018', 'Mar 2018', 'Apr 2018']

	plot_x = {month: i for i, month in enumerate(sorted(month_vals))}

	driver_df.drop('BusinessUnit', axis=1, inplace=True)

	for month in month_vals:
		if month not in driver_df['Action Date'].unique():
			driver_df = driver_df.append(pd.DataFrame([['Safety', month, 0]],
										 columns=['Action Type - No count', 'Action Date', 'agg_dur']))

	count_df = driver_df.groupby(['Action Type - No count',
						   		  'Action Date'], as_index=False).count()
	count_df.sort_values('Action Date', inplace=True)

	hours_df = driver_df.groupby(['Action Type - No count',
						   		  'Action Date'], as_index=False).sum()
	hours_df.sort_values('Action Date', inplace=True)

	ax1.plot(list(plot_x.values()), count_df['agg_dur'], 'o-',
			 color='#359333')

	ax1.set_xticks(list(plot_x.values()))
	ax1.set_xticklabels(months)
	ax1.set_ylabel('Count of Reports per Driver')
	plt.setp(ax1.xaxis.get_majorticklabels(), rotation=90)

	plt.title('Safety Events over Time')

	plt.tight_layout()
	plt.savefig('figures/safety_plot.png')

def venting(df):
	plt.close()
	fig, ax = plt.subplots(1, 1, figsize=(6, 6))

	df['Action Date'] = pd.to_datetime(df['Action Date'])
	df['Action Date'] = df['Action Date'].map(lambda x:100*x.year + x.month)

	month_vals = df['Action Date'].unique()
	months = ['March 2017', 'Apr 2017', 'May 2017', 'Jun 2017', \
			  'Jul 2017', 'Aug 2017', 'Sep 2017', 'Oct 2017', 'Nov 2017', 'Dec 2017', \
			  'Jan 2018', 'Feb 2018', 'Mar 2018', 'Apr 2018']
	plot_x = {month: i for i, month in enumerate(sorted(month_vals))}

	hours_df = df.groupby('Action Date', as_index=False).sum()
	hours_df.sort_values('Action Date', inplace=True)

	ax.plot(list(plot_x.values()), hours_df['agg_dur'] / 60 / 60, 'o-',
			color='#287acc')
	ax.set_xticks(list(plot_x.values()))
	ax.set_xticklabels(months)
	plt.setp(ax.xaxis.get_majorticklabels(), rotation=90)

	plt.title('North Venting over Time')
	# plt.xlabel('Business Unit')
	plt.ylabel('Total Hours Spent Venting')
	plt.tight_layout()
	plt.savefig('figures/north_venting.png')

def pie(df):
	plt.close()
	fig, ax = plt.subplots(1, 1, figsize=(8, 8))

	df.loc[(df['Action Type - No count'] == 'Safety 2.0') |
		   (df['Action Type - No count'] == 'Safety 3.0'),
		   'Action Type - No count'] = 'Safety'
	df.loc[(df['Action Type - No count'] == 'Plgr. Change') |
		   (df['Action Type - No count'] == 'Plgr. Insp.') |
		   (df['Action Type - No count'] == 'Plgr. Incomplete'),
		   'Action Type - No count'] = 'Plunger'
	df.loc[df['Action Type 1'] == 'Compressor - Gas Lift',
		   'Action Type - No count'] = 'Compressor'
	df.loc[(df['Action Type 1'] == 'Gas Scrubber') |
		   (df['Action Type 1'] == 'Gas Scrubber Separator') |
	  	   (df['Action Type 1'] == 'Sand Separator') |
		   (df['Action Type 1'] == 'Dehy') |
		   (df['Action Type 1'] == 'Fuel Gas/Start Gas') |
	  	   (df['Action Type 1'] == 'Heat Medium') |
	  	   (df['Action Type 1'] == 'Heater Treater') |
	  	   (df['Action Type 1'] == 'Instrument Air') |
	  	   (df['Action Type 1'] == 'Sales Valve') |
	  	   (df['Action Type 1'] == 'Sales Valve (PV)') |
	  	   (df['Action Type 1'] == 'Amine') |
	  	   (df['Action Type 1'] == 'Separator Inlet Valve (XV)'),
	  	   'Action Type - No count'] = 'Separator'
	df.loc[(df['Action Type 1'] == 'Tanks') |
		   (df['Action Type 1'] == 'Tanks/Pits') |
		   (df['Action Type 1'] == 'Water Transfer'),
	       'Action Type - No count'] = 'Liquids'
	df.loc[(df['Action Type 1'] == 'Pumping Unit') |
		   (df['Action Type 1'] == 'Recirc Pump'),
		   'Action Type - No count'] = 'Pumping System'
	df.loc[df['Action Type 1'] == 'Instrumentation',
		   'Action Type - No count'] = 'Instrumentation'
	df.loc[df['Action Type 1'] == 'Wellhead',
		   'Action Type - No count'] = 'Wellhead'
	df.loc[(df['Action Type - No count'] == 'WM Completed') &
		   (df['CommentAction'].str.contains('rtu')),
		   'Action Type - No count'] = 'RTU'
	df.loc[(df['Action Type - No count'] == 'WM Completed') &
		   (df['CommentAction'].str.contains('pump')),
		   'Action Type - No count'] = 'Pumping System'
	df.loc[(df['Action Type - No count'] == 'WM Completed') &
		   (df['CommentAction'].str.contains('pm')),
		   'Action Type - No count'] = 'PM'
	df.loc[(df['Action Type - No count'] == 'WM Completed') &
		   (df['CommentAction'].str.contains('plunger')),
		   'Action Type - No count'] = 'Plunger'
	df.loc[(df['Action Type - No count'] == 'WM Completed') &
		   (df['CommentAction'].str.contains('comp')),
		   'Action Type - No count'] = 'Compressor'

	df_short = df.loc[(df['Action Type - No count'] != 'Troubleshoot Charg. Sys.') &
					  (df['Action Type - No count'] != 'cIBatches') &
					  (df['Action Type - No count'] != 'documentSurfaceEquipment') &
					  (df['Action Type - No count'] != 'extBuildUp') &
					  (df['Action Type - No count'] != 'fluidShot') &
					  (df['Action Type - No count'] != 'pressureCheck') &
					  (df['Action Type - No count'] != 'snowRemoval') &
					  (df['Action Type - No count'] != 'soapSticks') &
					  (df['Action Type - No count'] != 'SF') &
					  (df['Action Type - No count'] != 'WM Completed') &
					  (df['Action Type - No count'] != 'rodPumpSpeedChange') &
					  (df['Action Type - No count'] != 'warmBootRTU'), :]

	hours_df = df_short.groupby('Action Type - No count', as_index=False).sum()
	hours_df.sort_values('agg_dur', inplace=True)

	plt.pie(hours_df['agg_dur'],
			labels=hours_df['Action Type - No count'],
			colors=['#5bd81c', '#ffb200', '#ba5600', '#7c20c1', '#6688e8',
					'#ad1362', '#edde8e', '#0c590b', '#008fa8', '#d68ffc',
					'#ff8282', '#ffe500', '#9cfc8f'],
			startangle=180)

	plt.title('Resource Allocation by Time Spent per Action', fontsize=20)
	plt.savefig('figures/pie.png')


if __name__ == '__main__':
	# action_df = action_pull()
	# action_df.to_csv('data/comment_action.csv')
	a_df = pd.read_csv('data/comment_action.csv', encoding='ISO-8859-1')
	hour_df = pd.read_csv('data/ws_hours.csv')
	wh_df = pd.merge(a_df, hour_df, left_on='_id', right_on='id')
	# stacked_actions(wh_df, plot_type='count')
	# stacked_actions(wh_df, plot_type='hours')
	# venting(wh_df.loc[(wh_df['BusinessUnit'] == 'North') &
	# 				  (wh_df['Action Type - No count'] == 'Vent'),
	# 				  ['Action Date', 'agg_dur']])
	# pie(wh_df[['Action Type - No count', 'Action Type 1', 'CommentAction', 'agg_dur']])

	# safety_plot(wh_df)

	# gauge_df = gauge_pull()
	# g_df = gauge_events(gauge_df)
	# gauge_counts(gauge_df[['BusinessUnit', 'Wellkey']])

	# pm_dist(a_df[a_df['Comment'].notnull()])

	# plunger_events(a_df)
	# plunger_insp(a_df)

	# dis_df = dispatch_pull()
	# work_dis_df = dis_df[['BusinessUnit', '_id', 'Action Type - No count']]
	# for g_type in ['driver']:
	# 	work_dist(work_dis_df, g_type)

	# hour_df = pd.read_csv('data/ws_hours.csv')
	# work_df = a_df.loc[(a_df['Action Type - No count'] != 'WM Completed') &
	# 				   (a_df['Action Type - No count'] != 'Gauge') &
	# 				   (a_df['Action Type - No count'] != 'SF'),
	# 				  ['BusinessUnit', '_id', 'Action Type - No count', 'Action Date']]
	# all_df = a_df.loc[:, ['BusinessUnit', '_id', 'Action Type - No count', 'Action Date']]
	# wh_df = pd.merge(work_df, hour_df, left_on='_id', right_on='id')
	site_report(wh_df[wh_df['Action Type - No count'] == 'Site Report'])

	# action_count(a_df)
	for g_type in ['driver']:
		work_dist(wh_df[['BusinessUnit', 'Action Type - No count',
						 'Action Type 1', 'CommentAction', 'agg_dur',
						 'Action Date']], g_type)

	# dispatch_wm(dis_df.loc[(dis_df['Action Type - No count'] == 'WM Completed') &
	# 				   	   (dis_df['CommentAction'].notnull()), :])
