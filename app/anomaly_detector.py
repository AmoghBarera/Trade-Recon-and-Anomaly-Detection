import os
import pandas as pd
import numpy as np
import joblib
from datetime import datetime
from sklearn.ensemble import IsolationForest

MODEL_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'reports', 'anomaly_model.pkl')

class AnomalyDetector:
    """
    Anomaly Detector for trades using Isolation Forest.
    
    Why Isolation Forest?
    Isolation Forest is an unsupervised machine learning algorithm that is highly effective 
    for anomaly detection, especially when the rate of anomalies is low. It works by 
    randomly selecting a feature and then randomly selecting a split value between the 
    maximum and minimum values of the selected feature. Since anomalies are "few and different", 
    they are isolated closer to the root of the tree (fewer splits needed). 
    Crucially, it does not require labeled anomalous (fraud) data to train.
    """
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.model = None
        self.features = ['price_deviation_pct', 'quantity', 'time_of_day', 'timestamp_drift_ms']
        self._load_or_train_model()

    def _extract_features_from_row(self, exec_price, exec_qty, exec_ts, conf_price, conf_qty, conf_ts):
        """Extracts numerical features from raw trade data."""
        try:
            # 1. Price deviation %
            price_dev_pct = 0.0
            if exec_price and conf_price and float(exec_price) > 0:
                price_dev_pct = abs(float(exec_price) - float(conf_price)) / float(exec_price)

            # 2. Quantity
            qty = float(exec_qty) if exec_qty else 0.0

            # 3. Time of day (hour)
            time_of_day = 0.0
            if exec_ts:
                dt = datetime.fromisoformat(exec_ts.replace('Z', '+00:00'))
                time_of_day = dt.hour + (dt.minute / 60.0)

            # 4. Timestamp drift (ms)
            drift_ms = 0.0
            if exec_ts and conf_ts:
                dt_exec = datetime.fromisoformat(exec_ts.replace('Z', '+00:00'))
                dt_conf = datetime.fromisoformat(conf_ts.replace('Z', '+00:00'))
                drift_ms = abs((dt_exec - dt_conf).total_seconds() * 1000.0)

            return [price_dev_pct, qty, time_of_day, drift_ms]
        except Exception:
            # Fallback to zeros on error to prevent crashing the pipeline
            return [0.0, 0.0, 0.0, 0.0]

    def _load_or_train_model(self):
        """Loads the saved model if it exists, otherwise trains a new one."""
        if os.path.exists(MODEL_PATH):
            print("Loading existing Anomaly Detection model...")
            self.model = joblib.load(MODEL_PATH)
        else:
            print("Training new Anomaly Detection model from historical data...")
            self._train_model()

    def _train_model(self):
        """Trains the Isolation Forest on the CSV data."""
        exec_file = os.path.join(self.data_dir, 'executions.csv')
        conf_file = os.path.join(self.data_dir, 'broker_confirmations.csv')

        if not os.path.exists(exec_file) or not os.path.exists(conf_file):
            print("Warning: Training data not found. Anomaly detector will be untrained.")
            # Initialize a dummy model that always predicts 1 (normal)
            self.model = IsolationForest(contamination=0.01, random_state=42)
            # Fit on dummy data just to initialize the tree structure
            self.model.fit(np.zeros((10, len(self.features))))
            return

        df_exec = pd.read_csv(exec_file)
        df_conf = pd.read_csv(conf_file)

        # Merge on trade_id
        df = pd.merge(df_exec, df_conf, on='trade_id', suffixes=('_exec', '_conf'))

        feature_rows = []
        for _, row in df.iterrows():
            features = self._extract_features_from_row(
                row.get('price_exec'), row.get('quantity_exec'), row.get('timestamp_exec'),
                row.get('price_conf'), row.get('quantity_conf'), row.get('timestamp_conf')
            )
            feature_rows.append(features)

        X = pd.DataFrame(feature_rows, columns=self.features)

        # Train Isolation Forest
        self.model = IsolationForest(contamination=0.01, random_state=42, n_estimators=100)
        self.model.fit(X)

        # Ensure the reports directory exists
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        joblib.dump(self.model, MODEL_PATH)
        print(f"Anomaly Detection model trained and saved to {MODEL_PATH}")

    def score_trade(self, execution: dict, confirmation: dict) -> tuple[bool, float]:
        """
        Scores an incoming trade against the trained Isolation Forest model.
        Returns:
            is_anomalous (bool): True if suspicious.
            anomaly_score (float): Lower is more anomalous (negative values typically indicate anomalies).
        """
        if not self.model:
            return False, 0.0

        features = self._extract_features_from_row(
            execution.get('price'), execution.get('quantity'), execution.get('timestamp'),
            confirmation.get('price'), confirmation.get('quantity'), confirmation.get('timestamp')
        )
        
        X = pd.DataFrame([features], columns=self.features)
        
        # Predict returns 1 for inliers (normal) and -1 for outliers (anomalous)
        prediction = self.model.predict(X)[0]
        score = self.model.score_samples(X)[0]

        is_anomalous = (prediction == -1)
        return is_anomalous, float(score)
