import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import logging

# Setup logging (addresses AI feedback)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def train_dropout_model(df):
    try:
        logging.info("Starting ML Training Pipeline...")
        X = df[['academic_score', 'coding_score', 'attendance_rate']]
        y = df['dropped_out'] # Target variable

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
        
        model = RandomForestClassifier(n_estimators=100)
        model.fit(X_train, y_train)
        
        logging.info(f"Model trained successfully. Accuracy: {model.score(X_test, y_test)}")
        return model
    except Exception as e:
        logging.error(f"Error in ML pipeline: {e}")

if __name__ == "__main__":
    # In production, this would load from Snowflake
    logging.info("ML Module Initialized")
