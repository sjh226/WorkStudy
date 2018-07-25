import pandas as pd
import numpy as np
import pyodbc
import sys
import matplotlib.pyplot as plt


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
		SELECT	D.FacilityKey
				,D.LocationID
				,D.CalcDate
				,D.SiteName
				,D.PriorityLevel
				,D.Reason
				,D.Person_assigned
				,ALH.[Owner Nickname]
				,ALH._id AS id
				,ALH.DefermentGas
				,F.BusinessUnit
				,D.Job_Rank
				,ALH.[Action Type - No count]
				,ALH.[Action Type 1]
				,ALH.[CommentAction]
		  FROM (SELECT	FacilityKey
						,D.LocationID
						,CalcDate
						,SiteName
						,PriorityLevel
						,Reason
						,Person_assigned
						,Job_Rank
					FROM [TeamOptimizationEngineering].[dbo].[L48_Dispatch] D
				INNER JOIN
					(SELECT  LocationID
								,MAX(CalcDate) AS MDate
						FROM [TeamOptimizationEngineering].[dbo].[L48_Dispatch]
						GROUP BY LocationID, CAST(CalcDate AS DATE)) AS MD
				ON	MD.LocationID = D.LocationID
				AND MD.MDate = D.CalcDate) D
		  JOIN [OperationsDataMart].[Dimensions].[Facilities] F
			ON F.Facilitykey = D.Facilitykey
		  LEFT OUTER JOIN [TeamOptimizationEngineering].[Reporting].[ActionListHistory] ALH
			ON LEFT(ALH.assetAPI, 10) = LEFT(D.LocationID, 10)
			AND CAST(D.CalcDate AS DATE) = CAST(ALH.[Action Date] AS DATE)
		WHERE F.BusinessUnit IN ('West', 'North')
		AND LEN(D.Person_assigned) > 2
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

def dispatch_work(df):
	colors = {'west': '#59b7f9', 'north': '#7759f9'}

	for bu in df['BusinessUnit'].unique():
		plt.close()

		count_df = df[(df['BusinessUnit'] == bu) &
					  (df['Action Type - No count'].notnull())]\
					  .groupby('PriorityLevel', as_index=False).count()
		count_df.rename(index=str, columns={'LocationID': 'Completed'}, inplace=True)

		total_df = df[df['BusinessUnit'] == bu].groupby('PriorityLevel', as_index=False).count()
		total_df.rename(index=str, columns={'LocationID': 'Total'}, inplace=True)

		plot_df = total_df.merge(count_df, on=['PriorityLevel'])
		plot_df['perc'] = plot_df.loc[:,'Completed'] / plot_df.loc[:,'Total']

		plt.bar(plot_df['PriorityLevel'].values, plot_df['perc'].values, .8,
				color=colors[bu.lower()])

		plt.title('Completed Dispatch Actions by Priority Level for {}'.format(bu))
		plt.xlabel('Priority Level')
		plt.ylabel('Percentage of Completed Dispatch Entries')
		plt.savefig('figures/completed_dispatch_{}.png'.format(bu.lower()))

def missed_dispatch(df):
	colors = {'west': '#59b7f9', 'north': '#7759f9'}
	drivers = {'east': 42, 'midcon': 61, 'north': 69, 'west': 140}

	plt.close()
	fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 8), sharey=True)

	for ax, bu in zip((ax1, ax2), df['BusinessUnit'].unique()):
		bu_df = df.loc[(df['BusinessUnit'] == bu) &
					   (df['Action Type - No count'] != 'cIBatches') &
					   (df['Action Type - No count'] != 'extBuildUp'), :]
		bu_df.loc[(bu_df['Action Type - No count'] == 'Safety 2.0') |
			   (bu_df['Action Type - No count'] == 'Safety 3.0'),
			   'Action Type - No count'] = 'Safety'
		bu_df.loc[(bu_df['Action Type - No count'] == 'Plgr. Change') |
			   (bu_df['Action Type - No count'] == 'Plgr. Insp.') |
			   (bu_df['Action Type - No count'] == 'Plgr. Incomplete'),
			   'Action Type - No count'] = 'Plunger'
		bu_df.loc[bu_df['Action Type 1'] == 'Compressor - Gas Lift',
			   'Action Type - No count'] = 'Compressor'
		bu_df.loc[(bu_df['Action Type 1'] == 'Gas Scrubber') |
			   (bu_df['Action Type 1'] == 'Gas Scrubber Separator') |
		  	   (bu_df['Action Type 1'] == 'Sand Separator') |
			   (bu_df['Action Type 1'] == 'Dehy') |
			   (bu_df['Action Type 1'] == 'Fuel Gas/Start Gas') |
		  	   (bu_df['Action Type 1'] == 'Heat Medium') |
		  	   (bu_df['Action Type 1'] == 'Heater Treater') |
		  	   (bu_df['Action Type 1'] == 'Instrument Air') |
		  	   (bu_df['Action Type 1'] == 'Sales Valve') |
		  	   (bu_df['Action Type 1'] == 'Sales Valve (PV)') |
		  	   (bu_df['Action Type 1'] == 'Amine') |
		  	   (bu_df['Action Type 1'] == 'Separator Inlet Valve (XV)'),
		  	   'Action Type - No count'] = 'Separator'
		bu_df.loc[(bu_df['Action Type 1'] == 'Tanks') |
			   (bu_df['Action Type 1'] == 'Tanks/Pits') |
			   (bu_df['Action Type 1'] == 'Water Transfer'),
		       'Action Type - No count'] = 'Liquids'
		bu_df.loc[(bu_df['Action Type 1'] == 'Pumping Unit') |
			   (bu_df['Action Type 1'] == 'Recirc Pump'),
			   'Action Type - No count'] = 'Pumping System'
		bu_df.loc[bu_df['Action Type 1'] == 'Instrumentation',
			   'Action Type - No count'] = 'Instrumentation'
		bu_df.loc[bu_df['Action Type 1'] == 'Wellhead',
			   'Action Type - No count'] = 'Wellhead'
		bu_df.loc[(bu_df['Action Type - No count'] == 'WM Completed') &
			   (bu_df['CommentAction'].str.contains('rtu')),
			   'Action Type - No count'] = 'RTU'
		bu_df.loc[(bu_df['Action Type - No count'] == 'WM Completed') &
			   (bu_df['CommentAction'].str.contains('pump')),
			   'Action Type - No count'] = 'Pumping System'
		bu_df.loc[(bu_df['Action Type - No count'] == 'WM Completed') &
			   (bu_df['CommentAction'].str.contains('pm')),
			   'Action Type - No count'] = 'PM'
		bu_df.loc[(bu_df['Action Type - No count'] == 'WM Completed') &
			   (bu_df['CommentAction'].str.contains('plunger')),
			   'Action Type - No count'] = 'Plunger'
		bu_df.loc[(bu_df['Action Type - No count'] == 'WM Completed') &
			   (bu_df['CommentAction'].str.contains('comp')),
			   'Action Type - No count'] = 'Compressor'

		df_short = bu_df.loc[(bu_df['Action Type - No count'] != 'Troubleshoot Charg. Sys.') &
						  (bu_df['Action Type - No count'] != 'cIBatches') &
						  (bu_df['Action Type - No count'] != 'documentSurfaceEquipment') &
						  (bu_df['Action Type - No count'] != 'extBuildUp') &
						  (bu_df['Action Type - No count'] != 'fluidShot') &
						  (bu_df['Action Type - No count'] != 'pressureCheck') &
						  (bu_df['Action Type - No count'] != 'snowRemoval') &
						  (bu_df['Action Type - No count'] != 'soapSticks') &
						  (bu_df['Action Type - No count'] != 'SF') &
						  (bu_df['Action Type - No count'] != 'WM Completed') &
						  (bu_df['Action Type - No count'] != 'rodPumpSpeedChange') &
						  (bu_df['Action Type - No count'] != 'warmBootRTU'), :]

		dispatch_df = df_short.loc[df_short['id'].notnull(), :]\
					  .groupby(['BusinessUnit', 'Action Type - No count'],
								as_index=False).count()

		dispatch_df.sort_values('Action Type - No count', inplace=True)

		ax.barh(np.arange(len(dispatch_df['Action Type - No count'].unique())),
				dispatch_df['PriorityLevel'].values / drivers[bu.lower()] / 5.3, .8,
				color=colors[bu.lower()])
		ax.set_xlabel('Count of Action per Driver per Month')
		ax.set_title(bu)
		ax.set_xlim(0, 8)

	ax1.text(5.5, .9, 'West Gauge = 19.2', fontsize=9)
	ax1.set_yticks(np.arange(len(dispatch_df['Action Type - No count'].unique())))
	ax1.set_yticklabels(sorted(list(dispatch_df['Action Type - No count'].unique())))

	plt.suptitle('Completed Dispatch Events'.format(bu))
	plt.tight_layout()
	plt.savefig('figures/dispatch_complete.png'.format(bu.lower()))

def dispatch_deferment(df):
	colors = {'west': '#59b7f9', 'north': '#7759f9'}

	for bu in df['BusinessUnit'].unique():
		plt.close()

		def_df = df[(df['BusinessUnit'] == bu) &
				    (df['DefermentGas'].notnull())]\
				   .groupby('PriorityLevel', as_index=False).mean()
		def_df.rename(index=str, columns={'LocationID': 'Completed'}, inplace=True)

		# total_df = df[df['BusinessUnit'] == bu].groupby('PriorityLevel', as_index=False).count()
		# total_df.rename(index=str, columns={'LocationID': 'Total'}, inplace=True)
		#
		# plot_df = total_df.merge(count_df, on=['PriorityLevel'])
		# plot_df['perc'] = plot_df.loc[:,'Completed'] / plot_df.loc[:,'Total']

		plt.bar(def_df['PriorityLevel'].values, def_df['DefermentGas'].values, .8,
				color=colors[bu.lower()])

		plt.title('Average Deferment by Priority Level for {}'.format(bu))
		plt.xlabel('Priority Level')
		plt.ylabel('Average Deferment from Each Priority (mcf)')
		plt.savefig('figures/dispatch_deferment_{}.png'.format(bu.lower()))

def dispatch_hours(df):
	for bu in df['BusinessUnit'].unique():
		plt.close()

		def_df = df[(df['BusinessUnit'] == bu) &
				    (df['agg_dur'].notnull()) &
					(df['DefermendGas'] > 0)]\
				   .groupby('PriorityLevel', as_index=False).mean()
		def_df.rename(index=str, columns={'LocationID': 'Completed'}, inplace=True)

		# total_df = df[df['BusinessUnit'] == bu].groupby('PriorityLevel', as_index=False).count()
		# total_df.rename(index=str, columns={'LocationID': 'Total'}, inplace=True)
		#
		# plot_df = total_df.merge(count_df, on=['PriorityLevel'])
		# plot_df['perc'] = plot_df.loc[:,'Completed'] / plot_df.loc[:,'Total']

		plt.bar(def_df['PriorityLevel'].values, def_df['agg_dur'].values, .8,
				color='#ae53f4')

		plt.title('Average Hours Spent by Priority Level for {}'.format(bu))
		plt.xlabel('Priority Level')
		plt.ylabel('Average Hours Spent for Each Priority')
		plt.savefig('figures/dispatch_hours_{}.png'.format(bu.lower()))

def dispatch_rate(df):
	colors = {'west': '#59b7f9', 'north': '#7759f9'}

	df['CalcDate'] = pd.to_datetime(df['CalcDate'])
	df.loc[:, 'CalcDate'] = df.loc[:, 'CalcDate'].map(lambda x:100*x.year + x.month)

	plt.close()
	fig, ax = plt.subplots(1, 1, figsize=(6, 6))

	for bu in df['BusinessUnit'].unique():
		bu_df = df.loc[df['BusinessUnit'] == bu]
		bu_df.loc[bu_df['Action Type - No count'].notnull(),
				  'Action Type - No count'] = 'Completed'
		bu_df.loc[bu_df['Action Type - No count'].isnull(),
				  'Action Type - No count'] = 'Not Completed'

		dispatch_df = bu_df.groupby(['BusinessUnit', 'Action Type - No count',
									 'CalcDate'],
									as_index=False).count()

		dispatch_df.sort_values('Action Type - No count', inplace=True)

		plot_df = pd.DataFrame(columns=['BusinessUnit', 'CalcDate', 'Completed'])
		df_months = list(dispatch_df['CalcDate'].unique())
		df_months.remove(201807)

		for month in df_months:
			comp = dispatch_df.loc[(dispatch_df['Action Type - No count'] == 'Completed') &
								   (dispatch_df['CalcDate'] == month), 'Job_Rank'].values[0]
			not_comp = dispatch_df.loc[(dispatch_df['Action Type - No count'] == 'Not Completed') &
								   	   (dispatch_df['CalcDate'] == month), 'Job_Rank'].values[0]
			plot_df = plot_df.append(pd.DataFrame([[bu, month, (comp / (comp + not_comp)) * 100]],
						   			 columns=['BusinessUnit', 'CalcDate', 'Completed']))
		plot_df.sort_values('CalcDate', inplace=True)

		ax.plot(np.arange(len(df_months)), plot_df['Completed'].values,
				'o-', color=colors[bu.lower()], label=bu)

	months = ['Feb 2018', 'Mar 2018', 'Apr 2018', 'May 2018', 'Jun 2018']

	plt.setp(ax.xaxis.get_majorticklabels(), rotation=90)
	ax.set_xticks(np.arange(len(df_months)))
	ax.set_xticklabels(months)
	plt.ylim(ymin=0, ymax=100)
	plt.ylabel('Percent Dispatch Actions Completed')
	plt.title('Completed Dispatch Events')
	plt.legend()
	plt.tight_layout()
	plt.savefig('figures/dispatch_review.png')


if __name__ == '__main__':
	df = dispatch_pull()

	hour_df = pd.read_csv('data/ws_hours.csv')
	hour_dis_df = df.merge(hour_df, on='id')

	missed_dispatch(df[['BusinessUnit', 'PriorityLevel',
						'id', 'Action Type - No count',
						'Action Type 1', 'CommentAction']])

	dispatch_rate(df[['BusinessUnit', 'CalcDate',
					  'Job_Rank', 'Action Type - No count']])

	# dispatch_deferment(df.loc[df['PriorityLevel'] != 0,
	# 					 ['PriorityLevel', 'DefermentGas', 'BusinessUnit',
	# 					 'Action Type - No count']])
	# missed_dispatch(df.loc[(df['Action Type - No count'].isnull()) &
	# 					   (df['PriorityLevel'] != 0),
	# 					   ['PriorityLevel', 'LocationID', 'BusinessUnit']])
	# dispatch_hours(df.loc[(hour_dis_df['PriorityLevel'] != 0) &
	# 					  (hour_dis_df['id'].notnull()),
	# 					 ['PriorityLevel', 'LocationID', 'BusinessUnit',
	# 					 'agg_dur']])
