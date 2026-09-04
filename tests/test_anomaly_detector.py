import pytest
import os
import numpy as np
from datetime import datetime, timedelta
from app.anomaly_detector import AnomalyDetector

@pytest.fixture(scope="module")
def anomaly_detector():
    """
    Returns an AnomalyDetector initialized pointing to a non-existent dir 
    so it falls back to a dummy trained model for testing, OR 
    if the actual data exists, it uses it.
    We will use the actual test data directory if possible to be realistic.
    """
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
    return AnomalyDetector(data_dir=data_dir)

def test_anomaly_detector_normal_trade(anomaly_detector):
    # Construct a perfectly normal trade
    execution = {
        'price': 150.0,
        'quantity': 100,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    confirmation = {
        'price': 150.0,
        'quantity': 100,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    
    is_anomalous, score = anomaly_detector.score_trade(execution, confirmation)
    assert not is_anomalous, f"Normal trade flagged as anomalous! Score: {score}"

def test_anomaly_detector_anomalous_trade(anomaly_detector):
    # Construct a highly anomalous trade (price deviation is massive)
    execution = {
        'price': 150.0,
        'quantity': 100,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    confirmation = {
        'price': 1500.0, # 10x deviation
        'quantity': 10000, # Massive quantity
        'timestamp': (datetime.utcnow() - timedelta(hours=5)).isoformat() + 'Z' # Massive drift
    }
    
    is_anomalous, score = anomaly_detector.score_trade(execution, confirmation)
    assert is_anomalous, f"Highly anomalous trade was NOT flagged! Score: {score}"
    assert score < 0, "Anomalous trade should have a negative score from Isolation Forest"

def test_anomaly_detector_missing_features(anomaly_detector):
    # Edge case: missing price or timestamp entirely
    execution = {
        'price': None,
        'quantity': 100
        # missing timestamp
    }
    confirmation = {
        'price': 150.0,
        'quantity': None,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    
    # Should not crash, should just return a safe score
    is_anomalous, score = anomaly_detector.score_trade(execution, confirmation)
    # The outcome doesn't strictly matter as long as it handles the exception and doesn't crash
    assert isinstance(is_anomalous, (bool, np.bool_))
    assert isinstance(score, float)
