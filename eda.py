import pandas as pd
import numpy as np
import pyodbc
import sys
import matplotlib.pyplot as plt


def enbase_pull():
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
        SELECT ALH. Wellkey
              ,[BusinessUnit]
              ,[Area]
              ,[WellName]
              ,[OwnerNTID]
              ,[createdby]
              ,[Owner Nickname]
              ,[_id]
              ,[assetAPI]
              ,[PriorityLevel]
              ,[PriorityType]
              ,[DispatchReason]
              ,[Person_assigned]
              ,[Action Date]
              ,[Action Type - No count]
              ,[Action Type]
              ,[Action Type 1]
              ,[Action Type 2]
              ,[Comment]
              ,ALH.DefermentDate
              ,[Action Status]
              ,ALH.DefermentGas
              ,ALH.CleanAvgGas
        	  ,CASE WHEN ALH.DefermentGas > 0 AND P.MeasuredGas > 0
        	   THEN (ALH.DefermentGas / PC.CleanAvgGas) * 100
        	   ELSE 0 END AS PercentDeferment
          FROM [TeamOptimizationEngineering].[Reporting].[ActionListHistory] AS ALH
          JOIN [OperationsDataMart].[Facts].[Production] AS P
        	ON P.Wellkey = ALH.Wellkey
        	AND ALH.DefermentDate = P.DateKey
          JOIN [OperationsDataMart].[Facts].[ProductionCalculations] AS PC
          	ON PC.Wellkey = ALH.Wellkey
        	AND ALH.DefermentDate = PC.DateKey
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

	df.columns = [col.lower().replace('-', '') for col in df.columns]
	df.columns = [col.replace('  ', ' ') for col in df.columns]
	df.columns = [col.replace(' ', '_') for col in df.columns]

	return df.drop_duplicates()

def deferment_stats(df):
	def_dic = {}
	for action in df['action_type_no_count'].unique():
		def_dic[action] = df[df['action_type_no_count'] == action]['percentdeferment'].mean()
	return def_dic

def deferment_plot(dic):
	plt.close()
	fig, ax = plt.subplots(1, 1, figsize=(10, 10))

	ax.bar(dic.keys(), dic.values(), .9)

	plt.xticks(rotation='vertical')
	plt.xlabel('Action Type')
	plt.ylabel('Average Deferment on Action (Percent)')
	plt.title('Comparison of Work Action to Average Deferment')
	plt.tight_layout()

	plt.savefig('figures/avg_deferment.png')


if __name__ == '__main__':
	# enb_df = enbase_pull()
	# enb_df.to_csv('data/enb_pull.csv', encoding='utf-8')
	enb_df = pd.read_csv('data/enb_pull.csv')

	def_dic = deferment_stats(enb_df)
	deferment_plot(def_dic)
