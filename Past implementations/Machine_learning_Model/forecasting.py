import pandas as pd
import numpy as np
from xgboost import XGBRegressor
import sqlite3
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib

class RenewableEnergyForecaster:
    def __init__(self, database_paths):
        self.database_paths = database_paths
        self.models = {
            'solar': None,
            'wind': None
        }
        self.scalers = {
            'solar': StandardScaler(),
            'wind': StandardScaler()
        }
        self.table_names = {
            'solar': 'SolarProduction',
            'wind': 'WindProduction'
        }
    
    def load_and_merge_data(self, source_type):
        """Load and merge relevant data for either 'solar' or 'wind'"""
        try:
            # Weather data loading and processing
            weather_conn = sqlite3.connect(self.database_paths['weather'])
            weather_data = pd.read_sql('SELECT * FROM weather', weather_conn)
            
            # Convert weather timestamps and create monthly aggregation
            weather_data['timestamp'] = pd.to_datetime(weather_data['time'])
            weather_monthly = weather_data.groupby(weather_data['timestamp'].dt.to_period('M')).agg({
                'temperature': 'mean',
                'humidity': 'mean',
                'precipitation': 'sum',
                'windspeed': 'mean',
                'cloudcover': 'mean'
            }).reset_index()
            
            weather_monthly['date'] = weather_monthly['timestamp'].dt.to_timestamp()
            
            # Production data loading and processing
            prod_conn = sqlite3.connect(self.database_paths[source_type])
            table_name = self.table_names[source_type]
            production_data = pd.read_sql(f'SELECT * FROM {table_name}', prod_conn)
            
            if production_data.empty:
                print(f"No data available for {source_type}")
                return None
            
            production_data['date'] = pd.to_datetime(production_data['date'])
            
            # Merge datasets
            merged_data = pd.merge(weather_monthly, production_data, 
                                 on='date', how='inner')
            
            weather_conn.close()
            prod_conn.close()
            return merged_data
            
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            raise

    def prepare_features(self, data, source_type):
        """Prepare feature set from merged data"""
        if data is None or data.empty:
            return None, None
            
        print("\nPreparing features for", source_type)
        print("Available columns:", data.columns.tolist())
            
        if source_type == 'solar':
            target_col = 'ac_monthly'
            additional_features = ['solar_radiation', 'capacity_factor']
        else:  # wind
            target_col = 'power'
            additional_features = ['wind_direction', 'pressure']
        
        # Base weather features
        base_features = ['temperature', 'humidity', 'precipitation', 'windspeed', 'cloudcover']
        
        # Add month as a feature
        data['month'] = pd.to_datetime(data['date']).dt.month
        data['month_sin'] = np.sin(2 * np.pi * data['month']/12)
        data['month_cos'] = np.cos(2 * np.pi * data['month']/12)
        
        # Combine all available features
        all_features = base_features + additional_features + ['month_sin', 'month_cos']
        available_features = [col for col in all_features if col in data.columns]
        
        print(f"Selected features: {available_features}")
        print(f"Target variable: {target_col}")
        
        X = data[available_features]
        y = data[target_col]
        
        return X, y

    def train(self, X, y, source_type):
        """Train the model and return performance metrics"""
        if X is None or y is None:
            return None
            
        print(f"\nTraining {source_type} model...")
        print(f"Number of training samples: {len(X)}")
        
        # Split the data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Scale features
        X_train_scaled = self.scalers[source_type].fit_transform(X_train)
        X_test_scaled = self.scalers[source_type].transform(X_test)
        
        # Initialize model
        self.models[source_type] = XGBRegressor(
            n_estimators=100,
            learning_rate=0.1,
            max_depth=5,
            random_state=42,
            enable_categorical=True  # Enable categorical feature support
        )
        
        # Train model
        self.models[source_type].fit(
            X_train_scaled, 
            y_train,
            eval_set=[(X_test_scaled, y_test)],
            verbose=True
        )
        
        # Make predictions
        y_pred = self.models[source_type].predict(X_test_scaled)
        
        # Calculate metrics
        metrics = {
            'mse': mean_squared_error(y_test, y_pred),
            'rmse': np.sqrt(mean_squared_error(y_test, y_pred)),
            'mae': mean_absolute_error(y_test, y_pred),
            'r2': r2_score(y_test, y_pred)
        }
        
        # Feature importance
        importance = pd.DataFrame({
            'feature': X.columns,
            'importance': self.models[source_type].feature_importances_
        }).sort_values('importance', ascending=False)
        
        # Save model and scaler
        joblib.dump(self.models[source_type], f'{source_type}_model.joblib')
        joblib.dump(self.scalers[source_type], f'{source_type}_scaler.joblib')
        
        return metrics, importance, (y_test, y_pred)

def plot_results(y_test, y_pred, source_type):
    """Plot actual vs predicted values and residuals"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))
    
    # Actual vs Predicted
    ax1.scatter(y_test, y_pred, alpha=0.5)
    ax1.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
    ax1.set_xlabel('Actual Values')
    ax1.set_ylabel('Predicted Values')
    ax1.set_title('Actual vs Predicted')
    
    # Residuals
    residuals = y_pred - y_test
    ax2.scatter(y_pred, residuals, alpha=0.5)
    ax2.axhline(y=0, color='r', linestyle='--')
    ax2.set_xlabel('Predicted Values')
    ax2.set_ylabel('Residuals')
    ax2.set_title('Residual Plot')
    
    plt.tight_layout()
    plt.savefig(f'{source_type}_results.png')
    plt.close()

def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    
    database_paths = {
        'weather': os.path.join(project_root, 'data_collection', 'weather', 'historical_weather.db'),
        'solar': os.path.join(project_root, 'data_collection', 'solar', 'solar_production.db'),
        'wind': os.path.join(project_root, 'data_collection', 'wind', 'wind_production.db'),
    }

    forecaster = RenewableEnergyForecaster(database_paths)

    for source_type in ['solar', 'wind']:
        print(f"\nProcessing {source_type} data...")
        try:
            # Load and prepare data
            merged_data = forecaster.load_and_merge_data(source_type)
            if merged_data is None:
                continue
                
            X, y = forecaster.prepare_features(merged_data, source_type)
            if X is None:
                continue
            
            # Train model and get metrics
            training_results = forecaster.train(X, y, source_type)
            
            if training_results is not None:
                metrics, importance, (y_test, y_pred) = training_results
                
                # Print results
                print(f"\n{source_type.capitalize()} Model Performance:")
                print(f"MSE: {metrics['mse']:.2f}")
                print(f"RMSE: {metrics['rmse']:.2f}")
                print(f"MAE: {metrics['mae']:.2f}")
                print(f"R2 Score: {metrics['r2']:.2f}")
                
                print("\nFeature Importance:")
                print(importance)
                
                # Plot results
                plot_results(y_test, y_pred, source_type)
                print(f"\nResults plotted and saved as {source_type}_results.png")
            
        except Exception as e:
            print(f"Error processing {source_type} data: {str(e)}")
            import traceback
            print("Traceback:", traceback.format_exc())

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error running forecasting script: {str(e)}")