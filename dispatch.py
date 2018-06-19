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
        SELECT ALH.[Wellkey]
              ,ALH.[BusinessUnit]
              ,ALH.[Area]
              ,ALH.[WellName]
              ,[OwnerNTID]
              ,[assetAPI]
              ,ALH.[PriorityLevel]
              ,[PriorityType]
              ,[DispatchReason]
              ,ALH.[Person_assigned] AS ActionAssigned
        	  ,D.Person_assigned AS DispatchAssigned
              ,[Action Date]
              ,[Action Type - No count]
              ,[Action Type]
              ,[Action Type 1]
              ,[Action Type 2]
              ,[Comment]
              ,[CommentAction]
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
        						(SELECT  LocationID
        								 ,MAX(CalcDate) AS MDate
        						 FROM [TeamOptimizationEngineering].[dbo].[L48_Dispatch]
        						 GROUP BY LocationID, CAST(CalcDate AS DATE)) AS MD
        					ON	MD.LocationID = D.LocationID
        					AND MD.MDate = D.CalcDate) D
          ON D.FacilityKey = W.Facilitykey
          AND CAST(D.CalcDate AS DATE) = CAST(ALH.[Action Date] AS DATE)
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


if __name__ == '__main__':
    df = dispatch_pull()
