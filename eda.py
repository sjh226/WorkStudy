import pandas as pd
import numpy as np
import pyodbc
import sys
import matplotlib.pyplot as plt
from text_analysis import tokenize, vectorize
import time
import heapq
import operator


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

	df['comment'] = df['comment'].astype(str)

	return df.drop_duplicates()

def deferment_stats(df):
	def_dic = {}
	for action in df['action_type_no_count'].unique():
		def_dic[action] = df[df['action_type_no_count'] == action]['percentdeferment'].mean()
	return def_dic

def text_it(df):
	def clean_comment(val):
		comment = val.lower().replace('-', '')
		comment = comment.replace('null', '')
		comment = comment.replace('N/A', '')
		return comment

	df_clean = df.loc[:,:]
	df_clean['comment'] = df_clean['comment'].astype(str)
	df_clean['comment'] = df_clean['comment'].apply(clean_comment)

	X, count_vec = vectorize(df_clean)

	values = list(np.array(X.sum(axis=0))[0,:])
	top_vals = sorted(values)[-20:]
	top_idx = [values.index(val) for val in top_vals]

	top_words = []
	for word, idx in count_vec.vocabulary_.items():
		if idx in top_idx:
			top_words.append(word)

	X_phrase, count_vec_phrase = vectorize(df_clean, ngram_min=2, ngram_max=3)

	values_phrase = list(np.array(X_phrase.sum(axis=0))[0,:])
	top_vals_phrase = sorted(values_phrase)[-20:]
	top_idx_phrase = [values_phrase.index(val) for val in top_vals_phrase]

	top_phrase = []
	for phrase, idx in count_vec_phrase.vocabulary_.items():
		if idx in top_idx_phrase:
			top_phrase.append(phrase)

	for word in top_words:
		df_clean[word] = df_clean['comment'].apply(lambda x: int(word in x))
	for phrase in top_phrase:
		df_clean[phrase] = df_clean['comment'].apply(lambda x: int(phrase in x))

	phrases = top_words + top_phrase

	return df_clean, phrases

def nlp_plot(df, phrases):
	plt.close()
	fig, ax = plt.subplots(1, 1, figsize=(10, 10))

	dic = {}
	for phrase in phrases:
		dic[phrase] = df[phrase].sum()

	ax.bar(sort_dic.keys(), sort_dic.values(), .9)

	plt.xticks(rotation='vertical')
	plt.xlabel('Action Type')
	plt.ylabel('Count of Actions in Comment')
	plt.title('Distribution of Action Words in Comment Section')
	plt.tight_layout()

	plt.savefig('figures/phrase_count.png')

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

	# def_dic = deferment_stats(enb_df)
	# deferment_plot(def_dic)

	df_clean, phrases = text_it(enb_df)
	df_clean.to_csv('data/phrase_df.csv', encoding='utf-8')
	df_clean = pd.read_csv('data/phrase_df.csv')

	nlp_plot(df_clean, phrases)
