import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
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

def vectorize(df, ngram_min=1, ngram_max=3):
	count_vec = CountVectorizer(stop_words='english', tokenizer=tokenize, \
								ngram_range=(ngram_min,ngram_max))
	X = count_vec.fit_transform(df['comment'].values)

	return X, count_vec
