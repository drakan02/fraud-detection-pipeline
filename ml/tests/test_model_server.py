import pytest
from unittest.mock import MagicMock, patch
import numpy as np
from fastapi.testclient import TestClient
from ml.model_server import app

@pytest.fixture(autouse=True)
def mock_model_loading():
    mock_model = MagicMock()
    def mock_predict_proba(X):
        if X[0, 0] < -5.0:
            return np.array([[0.1, 0.9]])
        return np.array([[0.9, 0.1]])
    mock_model.predict_proba = mock_predict_proba

    mock_scaler = MagicMock()
    mock_scaler.transform.return_value = np.array([[1.0]])

    dummy_state = {
        "model": mock_model,
        "amount_scaler": mock_scaler,
        "time_scaler": mock_scaler,
        "version": "latest",
        "baseline_mean": {},
        "baseline_std": {},
    }

    with patch("ml.model_server._load_version") as mock_load, \
         patch("ml.model_server.REGISTRY_PATH") as mock_reg, \
         patch("ml.model_server.sync_ch_write") as mock_write, \
         patch("ml.model_server.async_ch_query") as mock_query:
        
        def load_fake_version(version):
            import ml.model_server
            ml.model_server._state = dummy_state
        mock_load.side_effect = load_fake_version
        
        mock_reg.exists.return_value = True
        mock_reg.read_text.return_value = '[{"version": "latest", "auroc": 0.9}]'
        
        mock_write.return_value = None
        mock_query.return_value = {"data": [{"model_version": "latest"}]}
        
        yield

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

def test_list_models(client):
    response = client.get("/models")
    assert response.status_code == 200
    data = response.json()
    assert "versions" in data
    assert "active_version" in data

def test_predict_legit(client):
    # Payload matching PredictRequest schema
    payload = {
        "transaction_id": "test-legit-tx-id",
        "features": {
            "V1": -1.3, "V2": 0.5, "V3": 1.1, "V4": -0.2, "V5": 0.1,
            "V6": 0.2, "V7": 0.1, "V8": 0.5, "V9": 0.1, "V10": -0.1,
            "V11": 0.0, "V12": 0.1, "V13": 0.0, "V14": 0.1, "V15": 0.1,
            "V16": 0.0, "V17": 0.1, "V18": 0.0, "V19": 0.1, "V20": 0.0,
            "V21": 0.1, "V22": 0.2, "V23": -0.1, "V24": 0.0, "V25": 0.1,
            "V26": 0.1, "V27": 0.0, "V28": 0.0,
            "Time": 0.0, "Amount": 15.0
        }
    }
    
    response = client.post("/predict", json=payload)
    if response.status_code == 422:
        print(response.json())
    assert response.status_code == 200
    data = response.json()
    assert "fraud_probability" in data
    # Legit transaction probability should be very low
    assert data["fraud_probability"] < 0.2

def test_predict_fraud(client):
    # A heavily anomalous transaction resembling fraud
    payload = {
        "transaction_id": "test-fraud-tx-id",
        "features": {
            "V1": -15.3, "V2": 10.5, "V3": -21.1, "V4": 6.2, "V5": -10.1,
            "V6": -4.2, "V7": -18.1, "V8": 10.5, "V9": -5.1, "V10": -15.1,
            "V11": 7.0, "V12": -12.1, "V13": 0.0, "V14": -12.1, "V15": -0.5,
            "V16": -9.0, "V17": -15.1, "V18": -5.0, "V19": 2.1, "V20": 1.5,
            "V21": 1.5, "V22": -0.5, "V23": -1.5, "V24": 0.2, "V25": 1.0,
            "V26": 0.5, "V27": 1.5, "V28": 0.5,
            "Time": 406.0, "Amount": 5000.0
        }
    }
    
    response = client.post("/predict", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "fraud_probability" in data
    # Fraud transaction probability should be high
    assert data["fraud_probability"] > 0.8

def test_hot_swap_model(client):
    # 1. Get the current active version
    resp = client.get("/models")
    data = resp.json()
    active_version = data["active_version"]
    versions = data["versions"]
    
    if len(versions) < 1 or active_version is None:
        pytest.skip("No models available to test hot-swapping.")
        
    # We'll just hot-swap to the same active version for testing the API
    response = client.post(f"/models/{active_version}/activate")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
