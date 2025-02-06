import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# Load historical data
data = pd.read_csv('historical_weather_data')

# Assume the data has columns: 'GHI', 'DNI', 'Temperature', 'Energy_Production'
X = data[['Cloud coverage', 'DNI', 'Temperature']]  # Features
y = data['Energy_Production']  # Target variable (energy production)

# Split into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train the model (Random Forest in this case)
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Evaluate the model
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse}")

# Now, you can use the model to predict energy production based on forecast data
forecast_data = pd.read_csv('forecast_solar_data.csv')  # Forecasted GHI, DNI, etc.
X_forecast = forecast_data[['GHI', 'DNI', 'Temperature']]
energy_predictions = model.predict(X_forecast)

# Output energy predictions
print(energy_predictions)
