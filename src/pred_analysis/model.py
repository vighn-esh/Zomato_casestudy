# modeling.py

from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn import metrics

def train_linear_regression(X_train, y_train):
    """Train a Linear Regression model."""
    print("Training Linear Regression model...")
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    return lr

def train_decision_tree(X_train, y_train, config):
    """Train a Decision Tree model."""
    dt = DecisionTreeRegressor(max_depth=config['DT']['max_depth'], criterion=config['DT']['criterion'])
    dt.fit(X_train, y_train)
    return dt

def train_random_forest(X_train, y_train, config):
    """Train a Random Forest model."""
    rf = RandomForestRegressor(max_depth=config['RF']['max_depth'], n_estimators=config['RF']['n_estimators'], criterion=config['RF']['criterion'])
    rf.fit(X_train, y_train)
    return rf

def evaluate_model(model, X_test, y_test):
    """Evaluate the model with MAE and RMSE metrics."""
    y_pred = model.predict(X_test)
    mae = metrics.mean_absolute_error(y_test, y_pred)
    rmse = metrics.mean_squared_error(y_test, y_pred, squared=False)
    return mae, rmse
