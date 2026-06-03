import React, { useState, useEffect, useRef } from 'react';
import { 
  Activity, 
  ShieldAlert, 
  ShieldCheck, 
  Cpu, 
  Settings, 
  BarChart3, 
  History, 
  RefreshCw, 
  CheckCircle2, 
  XCircle, 
  ArrowRightLeft 
} from 'lucide-react';

function App() {
  const [status, setStatus] = useState('connecting');
  const [activeVersion, setActiveVersion] = useState('latest');
  const [availableModels, setAvailableModels] = useState([]);
  const [stats, setStats] = useState({
    models: {},
    active_version: 'latest'
  });
  const [predictions, setPredictions] = useState([]);
  const [loadingModelId, setLoadingModelId] = useState(null);
  
  const wsRef = useRef(null);

  // 1. Establish WebSocket Connection
  useEffect(() => {
    const connectWS = () => {
      setStatus('connecting');
      // Determine WebSocket protocol and host dynamically
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const host = window.location.host || 'localhost:30001';
      // In development if port is 5173 (Vite), target the backend at 30001
      const wsHost = host.includes('5173') ? 'localhost:30001' : host;
      
      const wsUrl = `${protocol}//${wsHost}/ws`;
      console.log('Connecting to WebSocket:', wsUrl);
      
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        setStatus('connected');
        console.log('WebSocket Connected');
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          console.log('Received WebSocket Data:', data);
          setStats(data);
          if (data.active_version) {
            setActiveVersion(data.active_version);
          }
        } catch (err) {
          console.error('Error parsing WS message:', err);
        }
      };

      ws.onclose = () => {
        setStatus('disconnected');
        console.log('WebSocket Disconnected, retrying in 3s...');
        setTimeout(connectWS, 3000);
      };

      ws.onerror = (err) => {
        console.error('WebSocket Error:', err);
        ws.close();
      };
    };

    connectWS();

    return () => {
      if (wsRef.current) wsRef.current.close();
    };
  }, []);

  // 2. Fetch Available Models and Recent Predictions
  const fetchModels = async () => {
    const host = window.location.host.includes('5173') ? 'http://localhost:30001' : '';
    try {
      const res = await fetch(`${host}/models`);
      if (res.ok) {
        const data = await res.json();
        setAvailableModels(data.versions || []);
      }
    } catch (err) {
      console.error('Error fetching models:', err);
    }
  };

  const fetchPredictions = async () => {
    const host = window.location.host.includes('5173') ? 'http://localhost:30001' : '';
    try {
      const res = await fetch(`${host}/api/predictions?limit=15`);
      if (res.ok) {
        const data = await res.json();
        setPredictions(data.data || []);
      }
    } catch (err) {
      console.error('Error fetching predictions:', err);
    }
  };

  useEffect(() => {
    fetchModels();
    fetchPredictions();
    // Poll recent transactions every 2.5s for dynamic update
    const interval = setInterval(fetchPredictions, 2500);
    return () => clearInterval(interval);
  }, []);

  // 3. Hot-Swap Model
  const handleActivateModel = async (version) => {
    setLoadingModelId(version);
    const host = window.location.host.includes('5173') ? 'http://localhost:30001' : '';
    try {
      const res = await fetch(`${host}/models/${version}/activate`, {
        method: 'POST'
      });
      if (res.ok) {
        setActiveVersion(version);
        // Refresh models list
        fetchModels();
      } else {
        alert('Failed to swap model version');
      }
    } catch (err) {
      console.error('Error activating model:', err);
      alert('Error connecting to ML Server');
    } finally {
      setLoadingModelId(null);
    }
  };

  // 4. Calculate Current Model Metrics
  const currentModelStats = stats.models[activeVersion] || {
    total: 0,
    accuracy: 0.0,
    precision: 0.0,
    recall: 0.0,
    f1: 0.0,
    confusion_matrix: { tp: 0, fp: 0, tn: 0, fn: 0 }
  };

  const cm = currentModelStats.confusion_matrix;

  return (
    <div className="app-container">
      {/* Header */}
      <header>
        <div className="logo-container">
          <ShieldAlert size={28} className="logo-icon" />
          <h1 className="logo-title">FraudShield Control Room</h1>
        </div>
        <div className="status-badge">
          <span className={`status-dot ${status === 'connected' ? '' : 'disconnected'}`}></span>
          <span>{status === 'connected' ? 'Live Stream Active' : 'Connecting to Stream...'}</span>
        </div>
      </header>

      {/* Main Grid Layout */}
      <div className="dashboard-grid">
        
        {/* Left Side: KPIs & Charts */}
        <div className="main-content-column">
          
          {/* KPI Cards Row */}
          <div className="kpis-row">
            <div className="card kpi-card">
              <span className="kpi-label">Transactions Scored</span>
              <span className="kpi-value">{currentModelStats.total.toLocaleString()}</span>
              <span className="kpi-subtext">Cumulative evaluated count</span>
            </div>
            
            <div className="card kpi-card">
              <span className="kpi-label">Detected Fraud</span>
              <span className="kpi-value" style={{ color: 'var(--accent-red)' }}>
                {(cm.tp + cm.fp).toLocaleString()}
              </span>
              <span className="kpi-subtext">True Positives + False Positives</span>
            </div>

            <div className="card kpi-card">
              <span className="kpi-label">Model Precision</span>
              <span className="kpi-value" style={{ color: 'var(--accent-blue)' }}>
                {Math.round(currentModelStats.precision * 100)}%
              </span>
              <span className="kpi-subtext">Accuracy of fraud alerts</span>
            </div>

            <div className="card kpi-card">
              <span className="kpi-label">Model Recall</span>
              <span className="kpi-value" style={{ color: 'var(--accent-amber)' }}>
                {Math.round(currentModelStats.recall * 100)}%
              </span>
              <span className="kpi-subtext">Percentage of fraud captured</span>
            </div>
          </div>

          {/* Performance & Confusion Matrix Grid */}
          <div className="dual-layout">
            
            {/* Performance Indicators */}
            <div className="card">
              <h2 className="card-title">
                <BarChart3 size={18} /> Model Performance Metrics
              </h2>
              
              <div className="metric-row">
                <div className="metric-header">
                  <span>Accuracy (Độ chính xác tổng quan)</span>
                  <span style={{ fontWeight: 600 }}>{Math.round(currentModelStats.accuracy * 100)}%</span>
                </div>
                <div className="metric-bar-bg">
                  <div className="metric-bar-fill" style={{ width: `${currentModelStats.accuracy * 100}%` }}></div>
                </div>
              </div>

              <div className="metric-row">
                <div className="metric-header">
                  <span>Precision (Độ chuẩn xác cảnh báo)</span>
                  <span style={{ fontWeight: 600 }}>{Math.round(currentModelStats.precision * 100)}%</span>
                </div>
                <div className="metric-bar-bg">
                  <div className="metric-bar-fill" style={{ width: `${currentModelStats.precision * 100}%`, background: 'var(--accent-blue)' }}></div>
                </div>
              </div>

              <div className="metric-row">
                <div className="metric-header">
                  <span>Recall (Tỷ lệ phát hiện đầy đủ)</span>
                  <span style={{ fontWeight: 600 }}>{Math.round(currentModelStats.recall * 100)}%</span>
                </div>
                <div className="metric-bar-bg">
                  <div className="metric-bar-fill" style={{ width: `${currentModelStats.recall * 100}%`, background: 'var(--accent-amber)' }}></div>
                </div>
              </div>

              <div className="metric-row">
                <div className="metric-header">
                  <span>F1-Score (Trung bình điều hòa)</span>
                  <span style={{ fontWeight: 600 }}>{Math.round(currentModelStats.f1 * 100)}%</span>
                </div>
                <div className="metric-bar-bg">
                  <div className="metric-bar-fill" style={{ width: `${currentModelStats.f1 * 100}%`, background: 'linear-gradient(90deg, var(--accent-purple), var(--accent-red))' }}></div>
                </div>
              </div>
            </div>

            {/* Confusion Matrix Block */}
            <div className="card">
              <h2 className="card-title">
                <ArrowRightLeft size={18} /> Confusion Matrix (Ma Trận Nhầm Lẫn)
              </h2>
              
              <div className="confusion-matrix">
                <div className="cm-cell correct">
                  <div className="cm-value">{cm.tn.toLocaleString()}</div>
                  <div className="cm-label">True Negative (TN)<br/>Hợp lệ classified Hợp lệ</div>
                </div>
                <div className="cm-cell incorrect">
                  <div className="cm-value">{cm.fp.toLocaleString()}</div>
                  <div className="cm-label">False Positive (FP)<br/>Hợp lệ classified G.Lận</div>
                </div>
                <div className="cm-cell incorrect">
                  <div className="cm-value">{cm.fn.toLocaleString()}</div>
                  <div className="cm-label">False Negative (FN)<br/>G.Lận classified Hợp lệ</div>
                </div>
                <div className="cm-cell correct">
                  <div className="cm-value">{cm.tp.toLocaleString()}</div>
                  <div className="cm-label">True Positive (TP)<br/>G.Lận classified G.Lận</div>
                </div>
              </div>
            </div>

          </div>

        </div>

        {/* Right Side: Model Version Registry & Active Swapper */}
        <div className="card">
          <h2 className="card-title">
            <Cpu size={18} /> Active Model Control
          </h2>
          <div className="model-list">
            {availableModels.length === 0 ? (
              <div style={{ color: 'var(--text-muted)', fontSize: '0.85rem', textAlign: 'center', padding: '1rem' }}>
                No versioned models loaded in registry.
              </div>
            ) : (
              availableModels.map((model) => {
                const isActive = activeVersion === model.version;
                const isLoading = loadingModelId === model.version;
                return (
                  <div key={model.version} className={`model-item ${isActive ? 'active' : ''}`}>
                    <div className="model-info-left">
                      <span className="model-name">Model Version {model.version.slice(-4) || 'Default'}</span>
                      <span className="model-meta">AUROC: {model.auroc || '0.97'} | Trained: {model.trained_at ? new Date(model.trained_at).toLocaleDateString() : 'N/A'}</span>
                    </div>
                    <button 
                      className="btn-activate"
                      disabled={isActive || isLoading}
                      onClick={() => handleActivateModel(model.version)}
                    >
                      {isActive ? 'Active' : isLoading ? 'Activating...' : 'Activate'}
                    </button>
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* Recent Transactions Table spanning both columns */}
        <div className="card table-card">
          <h2 className="card-title">
            <History size={18} /> Real-Time Transactions & Predictions (Bảng đối chiếu Luồng Giao Dịch)
          </h2>
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Transaction ID</th>
                  <th>Model ID</th>
                  <th>ML Probability</th>
                  <th>Model Decision</th>
                  <th>Actual Outcome</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {predictions.length === 0 ? (
                  <tr>
                    <td colSpan="6" style={{ textAlign: 'center', color: 'var(--text-muted)', padding: '2rem' }}>
                      Waiting for stream replayer data to arrive...
                    </td>
                  </tr>
                ) : (
                  predictions.map((p, idx) => {
                    const isPending = !p.actual_status;
                    const isModelCorrect = isPending ? false : ((p.prediction === 1 && p.actual_status === 'FRAUD') || (p.prediction === 0 && p.actual_status === 'SUCCESS'));
                    return (
                      <tr key={p.transaction_id || idx}>
                        <td style={{ fontFamily: 'monospace' }}>{p.transaction_id}</td>
                        <td>{p.model_version.slice(-4)}</td>
                        <td>{p.probability !== null ? `${Math.round(p.probability * 100)}%` : '0%'}</td>
                        <td>
                          <span className={`badge ${p.prediction === 1 ? 'fraud' : 'success'}`}>
                            {p.prediction === 1 ? 'FRAUD' : 'SUCCESS'}
                          </span>
                        </td>
                        <td>
                          {isPending ? (
                            <span className="badge" style={{ background: 'rgba(255,255,255,0.05)', color: 'var(--text-muted)' }}>
                              PENDING
                            </span>
                          ) : (
                            <span className={`badge ${p.actual_status === 'FRAUD' ? 'fraud' : 'success'}`}>
                              {p.actual_status}
                            </span>
                          )}
                        </td>
                        <td style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
                          {isPending ? (
                            <>
                              <RefreshCw size={16} className="spin" style={{ color: 'var(--text-muted)' }} />
                              <span style={{ color: 'var(--text-muted)', fontWeight: 500 }}>Processing</span>
                            </>
                          ) : isModelCorrect ? (
                            <>
                              <CheckCircle2 size={16} style={{ color: 'var(--accent-green)' }} />
                              <span style={{ color: 'var(--accent-green)', fontWeight: 500 }}>Correct</span>
                            </>
                          ) : (
                            <>
                              <XCircle size={16} style={{ color: 'var(--accent-red)' }} />
                              <span style={{ color: 'var(--accent-red)', fontWeight: 500 }}>Missed</span>
                            </>
                          )}
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>

      </div>
    </div>
  );
}

export default App;
