import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Activity,
  ShieldAlert,
  ShieldCheck,
  Cpu,
  BarChart3,
  History,
  RefreshCw,
  CheckCircle2,
  XCircle,
  ArrowRightLeft,
  ExternalLink,
  Percent,
  ArrowUpRight
} from 'lucide-react';

const API_BASE =
  window.location.host.includes('5173') ? 'http://localhost:30001' : '';

// Helper to render Circular SVG progress gauge
function CircularGauge({ label, value, colorClass, subtitle }) {
  const radius = 36;
  const circumference = 2 * Math.PI * radius; // ~226.19
  const offset = circumference - (value * circumference);
  
  return (
    <div className="gauge-item">
      <div className="gauge-svg-container">
        <svg width="90" height="90" viewBox="0 0 90 90">
          <circle cx="45" cy="45" r={radius} className="gauge-bg" />
          <circle
            cx="45"
            cy="45"
            r={radius}
            className={`gauge-val ${colorClass}`}
            strokeDasharray={circumference}
            strokeDashoffset={offset}
          />
        </svg>
        <span className="gauge-text">{Math.round(value * 100)}%</span>
      </div>
      <span className="gauge-label">{label}</span>
      <span className="gauge-desc">{subtitle}</span>
    </div>
  );
}

function App() {
  const [status, setStatus] = useState('connecting');
  const [activeVersion, setActiveVersion] = useState('latest');
  const [availableModels, setAvailableModels] = useState([]);
  const [stats, setStats] = useState({ models: {}, active_version: 'latest' });
  const [predictions, setPredictions] = useState([]);
  const [loadingModelId, setLoadingModelId] = useState(null);

  const wsRef = useRef(null);

  useEffect(() => {
    let timeoutId = null;
    const connectWS = () => {
      setStatus('connecting');
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const host = window.location.host || 'localhost:30001';
      const wsHost = host.includes('5173') ? 'localhost:30001' : host;
      const wsUrl = `${protocol}//${wsHost}/ws`;
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onopen  = () => setStatus('connected');
      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          setStats(data);
          if (data.active_version) setActiveVersion(data.active_version);
        } catch (_) {}
      };
      ws.onclose = () => {
        setStatus('disconnected');
        timeoutId = setTimeout(connectWS, 3000);
      };
      ws.onerror = () => ws.close();
    };
    connectWS();
    return () => {
      if (timeoutId) clearTimeout(timeoutId);
      if (wsRef.current) {
        wsRef.current.onclose = null;
        wsRef.current.close();
      }
    };
  }, []);

  // ── 2. Fetch available models & recent predictions ────────────────────────
  const fetchModels = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/models`);
      if (res.ok) {
        const data = await res.json();
        setAvailableModels(data.versions || []);
      }
    } catch (_) {}
  }, []);

  const fetchPredictions = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/predictions?limit=15`);
      if (res.ok) {
        const data = await res.json();
        setPredictions(data.data || []);
      }
    } catch (_) {}
  }, []);

  useEffect(() => {
    fetchModels();
    fetchPredictions();
    const iv = setInterval(fetchPredictions, 2500);
    return () => clearInterval(iv);
  }, [fetchModels, fetchPredictions]);

  // ── 3. Hot-swap model ─────────────────────────────────────────────────────
  const handleActivateModel = async (version) => {
    setLoadingModelId(version);
    try {
      const res = await fetch(`${API_BASE}/models/${version}/activate`, { method: 'POST' });
      if (res.ok) {
        setActiveVersion(version);
        fetchModels();
      } else {
        alert('Failed to swap model version');
      }
    } catch (_) {
      alert('Error connecting to ML Server');
    } finally {
      setLoadingModelId(null);
    }
  };

  // ── 4. Derived metrics ────────────────────────────────────────────────────
  const currentModelStats = stats.models[activeVersion] || {
    total: 0, accuracy: 0.0, precision: 0.0, recall: 0.0, f1: 0.0,
    confusion_matrix: { tp: 0, fp: 0, tn: 0, fn: 0 },
  };
  const cm = currentModelStats.confusion_matrix;

  return (
    <div className="app-container">
      {/* Header */}
      <header>
        <div className="logo-container">
          <ShieldAlert size={30} className="logo-icon" />
          <h1 className="logo-title">FraudShield Control Room</h1>
        </div>
        <div className="status-badge">
          <span className={`status-dot ${status === 'connected' ? '' : 'disconnected'}`} />
          <span>{status === 'connected' ? 'Live Stream Active' : 'Connecting to Stream...'}</span>
        </div>
      </header>

      {/* Main Grid */}
      <div className="dashboard-grid">

        {/* Left Column: Metrics and Tables */}
        <div className="main-content-column">

          {/* KPI Cards */}
          <div className="kpis-row">
            <div className="card kpi-card kpi-scored">
              <div className="kpi-header">
                <span className="kpi-label">Transactions Scored</span>
                <div className="kpi-icon-wrapper">
                  <Activity size={20} />
                </div>
              </div>
              <span className="kpi-value">{currentModelStats.total.toLocaleString()}</span>
              <span className="kpi-subtext">Cumulative evaluated count</span>
            </div>
            <div className="card kpi-card kpi-fraud">
              <div className="kpi-header">
                <span className="kpi-label">Detected Fraud</span>
                <div className="kpi-icon-wrapper">
                  <ShieldAlert size={20} />
                </div>
              </div>
              <span className="kpi-value">
                {(cm.tp + cm.fp).toLocaleString()}
              </span>
              <span className="kpi-subtext">True Positives + False Positives</span>
            </div>
            <div className="card kpi-card kpi-precision">
              <div className="kpi-header">
                <span className="kpi-label">Model Precision</span>
                <div className="kpi-icon-wrapper">
                  <ShieldCheck size={20} />
                </div>
              </div>
              <span className="kpi-value">
                {Math.round(currentModelStats.precision * 100)}%
              </span>
              <span className="kpi-subtext">Accuracy of fraud alerts</span>
            </div>
            <div className="card kpi-card kpi-recall">
              <div className="kpi-header">
                <span className="kpi-label">Model Recall</span>
                <div className="kpi-icon-wrapper">
                  <Percent size={18} />
                </div>
              </div>
              <span className="kpi-value">
                {Math.round(currentModelStats.recall * 100)}%
              </span>
              <span className="kpi-subtext">Percentage of fraud captured</span>
            </div>
          </div>

          {/* Gauges & Confusion Matrix */}
          <div className="dual-layout">
            
            {/* Circular Gauges Performance Card */}
            <div className="card">
              <h2 className="card-title">
                <BarChart3 size={18} /> Real-Time Model Performance
              </h2>
              <div className="gauge-grid">
                <CircularGauge
                  label="Accuracy"
                  value={currentModelStats.accuracy}
                  colorClass="val-success"
                  subtitle="Overall correctness"
                />
                <CircularGauge
                  label="Precision"
                  value={currentModelStats.precision}
                  colorClass="val-secondary"
                  subtitle="Alert correctness"
                />
                <CircularGauge
                  label="Recall"
                  value={currentModelStats.recall}
                  colorClass="val-warning"
                  subtitle="Capture rate"
                />
                <CircularGauge
                  label="F1-Score"
                  value={currentModelStats.f1}
                  colorClass="val-primary"
                  subtitle="Harmonic mean"
                />
              </div>
            </div>

            {/* Scientific 2x2 Confusion Matrix Grid */}
            <div className="card">
              <h2 className="card-title">
                <ArrowRightLeft size={18} /> Confusion Matrix (Đối Chiếu)
              </h2>
              
              <div className="matrix-container">
                <div className="matrix-grid">
                  {/* Row 1 Headers */}
                  <div className="matrix-header-cell"></div>
                  <div className="matrix-header-cell">Predicted LEGIT</div>
                  <div className="matrix-header-cell" style={{color: 'hsl(var(--accent-danger))'}}>Predicted FRAUD</div>

                  {/* Row 2: Actual Success */}
                  <div className="matrix-header-cell" style={{fontWeight: 700}}>Actual LEGIT</div>
                  
                  {/* TN */}
                  <div className="matrix-cell cell-correct">
                    <span className="matrix-value">{cm.tn.toLocaleString()}</span>
                    <span className="matrix-label">True Negative (TN)</span>
                    <span className="matrix-sublabel">Legit as Legit</span>
                  </div>

                  {/* FP */}
                  <div className="matrix-cell cell-incorrect">
                    <span className="matrix-value">{cm.fp.toLocaleString()}</span>
                    <span className="matrix-label">False Positive (FP)</span>
                    <span className="matrix-sublabel">Legit as Fraud</span>
                  </div>

                  {/* Row 3: Actual Fraud */}
                  <div className="matrix-header-cell" style={{color: 'hsl(var(--accent-danger))', fontWeight: 700}}>Actual FRAUD</div>

                  {/* FN */}
                  <div className="matrix-cell cell-incorrect">
                    <span className="matrix-value">{cm.fn.toLocaleString()}</span>
                    <span className="matrix-label">False Negative (FN)</span>
                    <span className="matrix-sublabel">Fraud as Legit</span>
                  </div>

                  {/* TP */}
                  <div className="matrix-cell cell-correct">
                    <span className="matrix-value">{cm.tp.toLocaleString()}</span>
                    <span className="matrix-label">True Positive (TP)</span>
                    <span className="matrix-sublabel">Fraud as Fraud</span>
                  </div>
                </div>
              </div>

            </div>
          </div>

        </div>

        {/* Right Column: Sidebar (Controls & Links) */}
        <div className="sidebar-column">

          {/* Active Model Control */}
          <div className="card">
            <h2 className="card-title"><Cpu size={18} /> Active Model Control</h2>
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
                        <span className="model-name">Version {model.version.slice(-4) || 'Default'}</span>
                        <span className="model-meta">
                          AUROC: {model.auroc || 'N/A'}
                        </span>
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

          {/* Infrastructure Observability Gateway */}
          <div className="card">
            <h2 className="card-title"><ExternalLink size={18} /> Infrastructure Observability</h2>
            <div className="gateway-links">
              
              <a href="http://localhost:30081" target="_blank" rel="noopener noreferrer" className="gateway-item gw-flink">
                <div className="gateway-item-left">
                  <Activity size={18} className="gateway-icon" />
                  <div>
                    <div className="gateway-title">Flink Dashboard</div>
                    <div className="gateway-desc">Stream graph and tasks status</div>
                  </div>
                </div>
                <ArrowUpRight size={14} className="gateway-arrow" />
              </a>

              <a href="http://localhost:30000" target="_blank" rel="noopener noreferrer" className="gateway-item gw-grafana">
                <div className="gateway-item-left">
                  <BarChart3 size={18} className="gateway-icon" />
                  <div>
                    <div className="gateway-title">Grafana Panels</div>
                    <div className="gateway-desc">Drift detection & tech metrics</div>
                  </div>
                </div>
                <ArrowUpRight size={14} className="gateway-arrow" />
              </a>

              <a href="http://localhost:30090" target="_blank" rel="noopener noreferrer" className="gateway-item gw-prometheus">
                <div className="gateway-item-left">
                  <History size={18} className="gateway-icon" />
                  <div>
                    <div className="gateway-title">Prometheus DB</div>
                    <div className="gateway-desc">Raw metric scrape & rules</div>
                  </div>
                </div>
                <ArrowUpRight size={14} className="gateway-arrow" />
              </a>

            </div>
          </div>

        </div>

        {/* Transactions Table */}
        <div className="card table-card">
          <div className="table-header-row">
            <h2 className="card-title">
              <History size={18} /> Real-Time Transactions & Predictions
            </h2>
          </div>
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Transaction ID</th>
                  <th>Model Version</th>
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
                    const isCorrect = !isPending && (
                      (p.prediction === 1 && p.actual_status === 'FRAUD') ||
                      (p.prediction === 0 && p.actual_status === 'SUCCESS')
                    );
                    return (
                      <tr key={p.transaction_id || idx}>
                        <td className="td-id">
                          {p.transaction_id?.slice(0, 16)}…
                        </td>
                        <td>{p.model_version?.slice(-4) || 'Default'}</td>
                        <td className="td-probability">
                          {p.probability != null ? `${Math.round(p.probability * 100)}%` : '0%'}
                        </td>
                        <td>
                          <span className={`badge ${p.prediction === 1 ? 'fraud' : 'success'}`}>
                            {p.prediction === 1 ? 'FRAUD' : 'SUCCESS'}
                          </span>
                        </td>
                        <td>
                          {isPending ? (
                            <span className="badge" style={{ background: 'rgba(255,255,255,0.03)', color: 'var(--text-muted)', border: '1px solid var(--border-light)' }}>
                              PENDING
                            </span>
                          ) : (
                            <span className={`badge ${p.actual_status === 'FRAUD' ? 'fraud' : 'success'}`}>
                              {p.actual_status}
                            </span>
                          )}
                        </td>
                        <td>
                          {isPending ? (
                            <div className="status-cell processing">
                              <RefreshCw size={12} className="spin" />
                              <span>Scoring</span>
                            </div>
                          ) : isCorrect ? (
                            <div className="status-cell correct">
                              <CheckCircle2 size={13} />
                              <span>Correct</span>
                            </div>
                          ) : (
                            <div className="status-cell missed">
                              <XCircle size={13} />
                              <span>Missed</span>
                            </div>
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
