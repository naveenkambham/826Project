from sklearn.ensemble import RandomForestRegressor
# import numpy as np
import  pandas
import  numpy
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
# from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
# from memory_profiler import profile
import cProfile
# import os
import psutil

# @profile(precision=4)
def RunModel():
    df = pandas.read_csv(r'Finalinput.csv')
    # df = pandas.read_csv(r'Synth_generated_29K.csv')

    X = df.iloc[:,2:7]
    Y = df.iloc[:,9:11]
    rf = RandomForestRegressor(n_estimators=20,criterion='mse',max_depth=10)
    seed=7
    numpy.random.seed(seed)
    estimators = []
    # estimators.append(('standardize', StandardScaler()))

    estimators.append(('mlp', rf))
    pipeline = Pipeline(estimators)
    kfold = KFold(n_splits=10, random_state=seed)
    results = cross_val_score(pipeline, X, Y, cv=kfold,scoring='neg_mean_squared_error')
    print(results)
    print("Larger: %.2f (%.2f) MSE" % (results.mean(), results.std()))

# pr = cProfile.Profile()
# pr.enable()
RunModel()
# pr.disable()

# pr.print_stats()

# myProcess = psutil.Process(os.getpid())
# myProcess.get_memory_info()


p = psutil.Process()
print(p.cpu_percent(interval=1))