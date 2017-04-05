import numpy as np
from sklearn.svm import SVR
import  pandas
import matplotlib.pyplot as plt

df = pandas.read_csv(r'Finalinput.csv')
X = df.iloc[:,2:7]
Y = df.iloc[:,9:11]
clf = SVR(kernel='poly')
clf.fit(X,Y)
print(clf.predict(X.iloc[230]))
print(Y.iloc[230])
