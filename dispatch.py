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
			ON LEFT(ALH.assetAPI, 8) = LEFT(D.LocationID, 8)
			AND ALH.Person_assigned = D.Person_assigned
			AND CAST(D.CalcDate AS DATE) = CAST(ALH.[Action Date] AS DATE)
		WHERE F.BusinessUnit IN ('North', 'West')
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
	for bu in df['BusinessUnit'].unique():
		plt.close()

		count_df = df[df['BusinessUnit'] == bu].groupby('PriorityLevel', as_index=False).count()

		plt.bar(count_df['PriorityLevel'].values, count_df['LocationID'].values, .8,
				color='#ff754f')

		plt.title('Completed Dispatch Actions by Priority Level for {}'.format(bu))
		plt.xlabel('Priority Level')
		plt.ylabel('Count of Actions')
		plt.savefig('figures/completed_dispatch_{}.png'.format(bu.lower()))


if __name__ == '__main__':
	df = dispatch_pull()

	dispatch_work(df.loc[df['Action Type - No count'].notnull(),
						 ['PriorityLevel', 'LocationID', 'BusinessUnit']])
