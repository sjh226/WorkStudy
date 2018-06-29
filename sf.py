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

	for bu, axis in zip(df['BusinessUnit'].unique(), [ax1, ax2, ax3, ax4]):
		bu_df = grouped_df.loc[grouped_df['BusinessUnit'] == bu, :]

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

		axis.bar(bu_df['Action Type 1'].values,
				 bu_df['agg_dur'].values / divisor / 60 / 60, .8,
				 color='#00b232', label='Action Type Counts')
		axis.set_title('{}'.format(bu))
		axis.xaxis.set_visible(True)
		axis.yaxis.set_visible(True)
		plt.setp(axis.xaxis.get_majorticklabels(), rotation=90)
		# axis.set_xlabel('SF Event')
		axis.set_ylabel('Hours Spent per Event{}'.format(graph_title))

	plt.suptitle('SF Hours by BU', y=.997)
	plt.tight_layout()
	plt.savefig('figures/sf_hours_{}.png'.format(graph_save))

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

	for label in ['total', 'driver', 'well']:
		sf_dist(sf_df[['BusinessUnit', 'Action Type 1', 'agg_dur']], label)
