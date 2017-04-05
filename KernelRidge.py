import numpy as np
from sklearn.kernel_ridge import KernelRidge
from sklearn.model_selection import GridSearchCV
import  pandas
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
# from sklearn.preprocessing import StandardScaler
import numpy
from sklearn.pipeline import Pipeline
from memory_profiler import profile
# import cProfile
# import psutil


@profile(precision=4)
def RunModel():
    df = pandas.read_csv(r'Finalinput.csv')
    X = df.iloc[:,2:7]
    Y = df.iloc[:,9:11]
    kr = GridSearchCV(KernelRidge(kernel='rbf', gamma=0.1), cv=5,
                      param_grid={"alpha": [1e0, 0.1, 1e-2, 1e-3],
                                  "gamma": np.logspace(-2, 2, 5)})

    seed=7
    numpy.random.seed(seed)
    estimators = []
    # estimators.append(('standardize', StandardScaler()))
    #nb_epoch=50, batch_size=5
    estimators.append(('mlp', kr))
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
# p = psutil.Process()
# print(p.cpu_times())
# print(p.cpu_percent(interval=1))