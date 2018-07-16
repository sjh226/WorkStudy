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

	for bu in df['BusinessUnit'].unique():
		plt.close()
		fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 6), sharey=True)

		bu_df = df.loc[df['BusinessUnit'] == bu]
		bu_df.loc[bu_df['Action Type - No count'].isnull(),
				  'Action Type - No count'] = 'Not Completed'

		dispatch_df = bu_df.groupby(['BusinessUnit', 'Action Type - No count'],
									as_index=False).count()
		completed_df = bu_df.loc[bu_df['id'].notnull(), :]\
					   .groupby(['BusinessUnit', 'Action Type - No count'],
								 as_index=False).count()

		dispatch_df.sort_values('Action Type - No count', inplace=True)
		completed_df.sort_values('Action Type - No count', inplace=True)

		ax1.barh(np.arange(len(bu_df['Action Type - No count'].unique())),
				 dispatch_df['PriorityLevel'].values, .8,
				 color=colors[bu.lower()])

		ax2.barh(np.arange(len(completed_df['Action Type - No count'].unique())),
				 completed_df['PriorityLevel'].values, .8,
				 color=colors[bu.lower()])

		ax1.set_yticks(np.arange(len(bu_df['Action Type - No count'].unique())))
		ax1.set_yticklabels(sorted(list(bu_df['Action Type - No count'].unique())))

		ax1.set_title('Total Actions Dispatched')
		ax2.set_title('Completed Dispatch Actions')
		ax1.set_xlabel('Count of Action')
		ax2.set_xlabel('Count of Action')

		plt.suptitle('Total and Completed Dispatch Events for {}'.format(bu))
		plt.savefig('figures/dispatch_review_{}.png'.format(bu.lower()))

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


if __name__ == '__main__':
	df = dispatch_pull()

	hour_df = pd.read_csv('data/ws_hours.csv')
	hour_dis_df = df.merge(hour_df, on='id')

	missed_dispatch(df[['BusinessUnit', 'PriorityLevel',
						'id', 'Action Type - No count']])

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
