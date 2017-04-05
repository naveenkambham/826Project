print(__doc__)


# Code source: Jaques Grobler
# License: BSD 3 clause


import matplotlib.pyplot as plt
import numpy as np
import pandas
from sklearn import datasets, linear_model

# Load the diabetes dataset
dataframe = pandas.read_csv(r'Finalinput.csv')
# print((dataframe))

X = dataframe.iloc[:,2:7]
Y = dataframe.iloc[:,9]
# print(len(X))
# print(len(Y))
# Split the data into training/testing sets
diabetes_X_train = X[:-20]
diabetes_X_test = X[-20:]


# Split the targets into training/testing sets
diabetes_y_train = X[:-20]
diabetes_y_test = Y[-20:]

# Create linear regression object
regr = linear_model.LinearRegression()
# print((diabetes_X_test))
# print((diabetes_y_test))
# Train the model using the training sets
regr.fit(diabetes_X_train, diabetes_y_train)
print(regr.predict(diabetes_X_test))
# The coefficients
print('Coefficients: \n', regr.coef_)
# The mean squared error
print("Mean squared error: %.2f"
      % np.mean((regr.predict(diabetes_X_test) - diabetes_y_test) ** 2))
# Explained variance score: 1 is perfect prediction
print('Variance score: %.2f' % regr.score(diabetes_X_test, diabetes_y_test))

# Plot outputs
plt.scatter(diabetes_X_test, diabetes_y_test,  color='black')
plt.plot(diabetes_X_test, regr.predict(diabetes_X_test), color='blue',
         linewidth=3)

plt.xticks(())
plt.yticks(())

plt.show()
