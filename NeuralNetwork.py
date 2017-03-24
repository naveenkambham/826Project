import numpy
import pandas
from keras.models import Sequential
from keras.layers import Dense
from keras.wrappers.scikit_learn import KerasRegressor
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

# Scaling Input and only input layers and output layers
# load dataset
dataframe = pandas.read_csv("Finalinput.csv")
# split into input (X) and output (Y) variables
X = dataframe.iloc[:,2:7].values
Y = dataframe.iloc[:,[9,11]].values

def larger_model():
	# create model
	model = Sequential()
	model.add(Dense(20, input_dim=5, init='normal', activation='relu'))
	model.add(Dense(2, init='normal'))
	# Compile model
	model.compile(loss='mean_squared_error', optimizer='adam')
	return model
seed=7
numpy.random.seed(seed)
estimators = []
estimators.append(('standardize', StandardScaler()))
#nb_epoch=50, batch_size=5
estimators.append(('mlp', KerasRegressor(build_fn=larger_model, nb_epoch=50, batch_size=20, verbose=0)))
pipeline = Pipeline(estimators)
kfold = KFold(n_splits=10, random_state=seed)
results = cross_val_score(pipeline, X, Y, cv=kfold)
print("Larger: %.2f (%.2f) MSE" % (results.mean(), results.std()))
