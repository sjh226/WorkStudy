import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from analyze import action_pull


def sf_dist(df, graph_per='total'):
	plt.close()
	fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(10, 10))

	drivers = {'east': 42, 'midcon': 61, 'north': 69, 'west': 140}
	wells = {'east': 880, 'midcon': 2853, 'north': 2003, 'west': 3834}

	grouped_df = df.groupby(['BusinessUnit', 'Action Type 1'], as_index=False).sum()
	return_df = pd.DataFrame(columns=grouped_df.columns)

	for bu, axis in zip(df['BusinessUnit'].unique(), [ax1, ax2, ax3, ax4]):
		bu_df = df.loc[df['BusinessUnit'] == bu, :]

		if graph_per == 'driver':
			divisor = drivers[bu.lower()]
			graph_title = ' per Driver'
			graph_save = 'driver'
		elif graph_per == 'well':
			divisor = wells[bu.lower()]
			graph_title = ' per Well'
			graph_save = 'well'
		else:
			divisor = 100
			graph_title = ''
			graph_save = 'total'

		bu_df.loc[bu_df['Action Type 1'] == 'Compressor - Gas Lift',
				  'Action Type 1'] = 'Compressor'
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
				  'Action Type 1'] = 'Separator'
		bu_df.loc[(bu_df['Action Type 1'] == 'Tanks') |
				  (bu_df['Action Type 1'] == 'Tanks/Pits') |
				  (bu_df['Action Type 1'] == 'Water Transfer'),
				  'Action Type 1'] = 'Liquids'
		bu_df.loc[(bu_df['Action Type 1'] == 'Generator') |
				  (bu_df['Action Type 1'] == 'Site Power'),
				  'Action Type 1'] = 'Power System'
		bu_df.loc[(bu_df['Action Type 1'] == 'Pumping Unit') |
				  (bu_df['Action Type 1'] == 'Recirc Pump'),
				  'Action Type 1'] = 'Pumping System'

		bu_df = bu_df.loc[(bu_df['Action Type 1'] == 'Automation') |
		 				  (bu_df['Action Type 1'] == 'Compressor') |
						  (bu_df['Action Type 1'] == 'Instrumentation') |
						  (bu_df['Action Type 1'] == 'Liquids') |
						  (bu_df['Action Type 1'] == 'Plunger System') |
						  (bu_df['Action Type 1'] == 'Separator') |
						  (bu_df['Action Type 1'] == 'Wellhead'), :]
		return_df = return_df.append(bu_df)

		datas = []
		labels = []
		for act in sorted(bu_df['Action Type 1'].unique()):
			data = bu_df.loc[bu_df['Action Type 1'] == act, 'agg_dur'].values
			datas.append(list(data / 60 / 60))
			labels.append(act)

		axis.boxplot(datas, labels=labels)
		axis.set_title('{}'.format(bu))
		axis.xaxis.set_visible(True)
		axis.yaxis.set_visible(True)
		axis.set_ylim(top=6.25)
		plt.setp(axis.xaxis.get_majorticklabels(), rotation=90)
		axis.set_ylabel('Hours Spent per Event'.format(graph_title))

	plt.suptitle('SF Hours by BU', y=.997)
	plt.tight_layout()
	plt.savefig('figures/sf_hours.png'.format(graph_save))

	return return_df.groupby(['BusinessUnit', 'Action Type 1'], as_index=False).mean()


if __name__ == '__main__':
	# action_df = action_pull()
	# action_df.to_csv('data/comment_action.csv')
	a_df = pd.read_csv('data/comment_action.csv', encoding='ISO-8859-1')

	hour_df = pd.read_csv('data/ws_hours.csv')
	hour_df = a_df.merge(hour_df, left_on='_id', right_on='id')

	sf_df = hour_df.loc[(hour_df['Action Type - No count'] == 'SF') &
						(hour_df['agg_dur'].notnull()) &
						(hour_df['Action Type 1'].notnull()),
						['BusinessUnit', '_id', 'Action Date',
						'Action Type - No count', 'Action Type 1',
						'DefermentGas', 'agg_dur']]

	for label in ['driver']:
		sff_df = sf_dist(sf_df[['BusinessUnit', 'Action Type 1', 'agg_dur']], label)
