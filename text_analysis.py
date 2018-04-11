import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.cross_validation import train_test_split
import nltk
from nltk.stem.porter import PorterStemmer
import string
from heapq import nlargest


def tokenize(text):
	text = "".join([ch for ch in text if ch not in string.punctuation])
	tokens = nltk.word_tokenize(text)
	stems = []
	for item in tokens:
		stems.append(PorterStemmer().stem(item))
	return stems

def vectorize(df):
	vect = TfidfVectorizer(max_df=1.0, min_df=0.0, lowercase=True, \
						   stop_words='english', tokenizer=tokenize, \
						   ngram_range=(1,3))
	X = vect.fit_transform(df['comment'].values)
	idf = vect.idf_
	return idf, X
