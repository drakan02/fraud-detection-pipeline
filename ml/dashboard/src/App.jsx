import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  AlertTriangle,
  BarChart3,
  CheckCircle2,
  Cpu,
  Database,
  ExternalLink,
  Gauge,
  History,
  RefreshCw,
  ShieldAlert,
  ShieldCheck,
  XCircle,
} from 'lucide-react';

const API_BASE = window.location.host.includes('5173') ? 'http://localhost:30001' : '';

const formatPercent = (value) => `${((value || 0) * 100).toFixed(1)}%`;
const formatProb = (value) => (value == null ? '-' : `${(value * 100).toFixed(value < 0.01 ? 3 : 1)}%`);
const shortId = (value, len = 12) => (value ? `${value.slice(0, len)}...` : '-');
const shortVersion = (value) => (value ? value.slice(-8) : 'unknown');

function Sparkline({ data, color = '#3b82f6' }) {
  if (!data || data.length < 2) return null;
  const width = 120;
  const height = 28;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min === 0 ? 1 : max - min;
  
  const points = data.map((val, index) => {
    const x = (index / (data.length - 1)) * width;
    const y = height - ((val - min) / range) * (height - 4) - 2;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });

  const pathData = `M ${points.join(' L ')}`;
  const fillPoints = `${points.join(' ')} ${width},${height} 0,${height}`;
  const gradId = `grad-${color.replace('#', '')}`;

  return (
    <div className="sparkline-container">
      <svg width="100%" height="100%" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
        <defs>
          <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.25" />
            <stop offset="100%" stopColor={color} stopOpacity="0" />
          </linearGradient>
        </defs>
        <polygon points={fillPoints} fill={`url(#${gradId})`} />
        <polyline fill="none" stroke={color} strokeWidth="1.5" points={points.join(' ')} strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    </div>
  );
}

function AccuracyGauge({ value }) {
  const safeValue = Math.max(0, Math.min(1, Number(value) || 0));
  const radius = 55;
  const strokeWidth = 10;
  const circumference = Math.PI * radius;
  const strokeDashoffset = circumference - (safeValue * circumference);
  
  return (
    <div className="accuracy-container">
      <div className="gauge-wrapper">
        <svg width="150" height="85" viewBox="0 0 150 85" className="gauge-svg">
          <defs>
            <linearGradient id="gauge-grad" x1="0" y1="0" x2="1" y2="0">
              <stop offset="0%" stopColor="#2563eb" />
              <stop offset="100%" stopColor="#3b82f6" />
            </linearGradient>
            <filter id="glow">
              <feGaussianBlur stdDeviation="2.5" result="coloredBlur"/>
              <feMerge>
                <feMergeNode in="coloredBlur"/>
                <feMergeNode in="SourceGraphic"/>
              </feMerge>
            </filter>
          </defs>
          <path
            d="M 20 75 A 55 55 0 0 1 130 75"
            fill="none"
            stroke="rgba(255, 255, 255, 0.05)"
            strokeWidth={strokeWidth}
            strokeLinecap="round"
          />
          <path
            d="M 20 75 A 55 55 0 0 1 130 75"
            fill="none"
            stroke="url(#gauge-grad)"
            strokeWidth={strokeWidth}
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            filter="url(#glow)"
            style={{ transition: 'stroke-dashoffset 0.8s ease-out' }}
          />
        </svg>
        <div className="gauge-val-text">{formatPercent(safeValue)}</div>
      </div>
      <div className="gauge-sub-text">
        <span className="live-matrix-dot" /> Live Model Accuracy
      </div>
    </div>
  );
}

function KpiTile({ icon: Icon, label, value, detail, tone = 'neutral' }) {
  return (
    <section className={`kpi-tile tone-${tone}`}>
      <div className="tile-top">
        <span className="tile-label">{label}</span>
        <Icon size={18} />
      </div>
      <div className="tile-value">{value}</div>
      <div className="tile-detail">{detail}</div>
    </section>
  );
}

function MetricBar({ label, value, tone, historyData, colorCode }) {
  const safe = Math.max(0, Math.min(1, Number(value) || 0));
  return (
    <div className="metric-row">
      <div className="metric-row-head">
        <span>{label}</span>
        <strong>{formatPercent(safe)}</strong>
      </div>
      <div className="metric-track">
        <span className={`metric-fill tone-${tone}`} style={{ width: `${safe * 100}%` }} />
      </div>
      {historyData && historyData.length > 1 && (
        <Sparkline data={historyData} color={colorCode} />
      )}
    </div>
  );
}

function StatusPill({ status }) {
  const connected = status === 'connected';
  return (
    <div className={`status-pill ${connected ? 'online' : 'offline'}`}>
      <span className="status-dot" />
      <span>{connected ? 'Live Data Feed' : status === 'connecting' ? 'Connecting...' : 'Offline'}</span>
    </div>
  );
}

function DecisionBadge({ value }) {
  const fraud = value === 'FRAUD' || value === 1;
  return <span className={`badge ${fraud ? 'fraud' : 'success'}`}>{fraud ? 'FRAUD' : 'SUCCESS'}</span>;
}

function App() {
  const [status, setStatus] = useState('connecting');
  const [activeVersion, setActiveVersion] = useState('latest');
  const [availableModels, setAvailableModels] = useState([]);
  const [stats, setStats] = useState({ models: {}, active_version: 'latest' });
  const [predictions, setPredictions] = useState([]);
  const [loadingModelId, setLoadingModelId] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  
  const [metricHistory, setMetricHistory] = useState({
    accuracy: [],
    precision: [],
    recall: [],
    f1: []
  });

  const wsRef = useRef(null);

  useEffect(() => {
    let timeoutId = null;

    const connectWS = () => {
      setStatus('connecting');
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const host = window.location.host || 'localhost:30001';
      const wsHost = host.includes('5173') ? 'localhost:30001' : host;
      const ws = new WebSocket(`${protocol}//${wsHost}/ws`);
      wsRef.current = ws;

      ws.onopen = () => setStatus('connected');
      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          setStats(data);
          setLastUpdated(new Date());
          
          const activeVer = data.active_version || 'latest';
          setActiveVersion(activeVer);

          const modelStats = data.models?.[activeVer];
          if (modelStats) {
            setMetricHistory(prev => {
              const limit = 15;
              const acc = modelStats.accuracy || 0;
              const prec = modelStats.precision || 0;
              const rec = modelStats.recall || 0;
              const f1Val = modelStats.f1 || 0;
              
              return {
                accuracy: prev.accuracy.length === 0 
                  ? Array(8).fill(acc) 
                  : [...prev.accuracy, acc].slice(-limit),
                precision: prev.precision.length === 0 
                  ? Array(8).fill(prec) 
                  : [...prev.precision, prec].slice(-limit),
                recall: prev.recall.length === 0 
                  ? Array(8).fill(rec) 
                  : [...prev.recall, rec].slice(-limit),
                f1: prev.f1.length === 0 
                  ? Array(8).fill(f1Val) 
                  : [...prev.f1, f1Val].slice(-limit),
              };
            });
          }
        } catch (_) {
          // Ignore malformed websocket frames.
        }
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

  const fetchModels = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/models`);
      if (res.ok) {
        const data = await res.json();
        setAvailableModels(data.versions || []);
      }
    } catch (_) {
      // Keep previous model list on transient failures.
    }
  }, []);

  const fetchPredictions = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/predictions?limit=20`);
      if (res.ok) {
        const data = await res.json();
        setPredictions(data.data || []);
      }
    } catch (_) {
      // Keep previous table rows on transient failures.
    }
  }, []);

  useEffect(() => {
    fetchModels();
    fetchPredictions();
    const iv = setInterval(fetchPredictions, 2500);
    return () => clearInterval(iv);
  }, [fetchModels, fetchPredictions]);

  const handleActivateModel = async (version) => {
    setLoadingModelId(version);
    try {
      let adminKey = sessionStorage.getItem('modelAdminApiKey') || '';
      const headers = adminKey ? { 'X-API-Key': adminKey } : {};
      let res = await fetch(`${API_BASE}/models/${version}/activate`, { method: 'POST', headers });

      if (res.status === 401) {
        adminKey = window.prompt('Model admin API key');
        if (!adminKey) return;
        sessionStorage.setItem('modelAdminApiKey', adminKey);
        res = await fetch(`${API_BASE}/models/${version}/activate`, {
          method: 'POST',
          headers: { 'X-API-Key': adminKey },
        });
      }

      if (res.ok) {
        setActiveVersion(version);
        fetchModels();
      } else {
        alert('Model activation failed');
      }
    } catch (_) {
      alert('ML server is not reachable');
    } finally {
      setLoadingModelId(null);
    }
  };

  const current = stats.models?.[activeVersion] || {
    total: 0,
    accuracy: 0,
    precision: 0,
    recall: 0,
    f1: 0,
    confusion_matrix: { tp: 0, fp: 0, tn: 0, fn: 0 },
  };

  const cm = current.confusion_matrix || { tp: 0, fp: 0, tn: 0, fn: 0 };
  const detected = (cm.tp || 0) + (cm.fp || 0);
  const missed = cm.fn || 0;
  const lastRun = predictions.find((p) => p.run_id)?.run_id || 'unknown';

  const newestPredictions = useMemo(
    () => predictions.map((p) => {
      const pending = !p.actual_status;
      const correct = !pending && (
        (p.prediction === 1 && p.actual_status === 'FRAUD') ||
        (p.prediction === 0 && p.actual_status === 'SUCCESS')
      );
      return { ...p, pending, correct };
    }),
    [predictions],
  );

  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand-block">
          <div className="brand-mark"><ShieldAlert size={24} /></div>
          <div>
            <h1>GUARDLINK // FRAUD DETECTION</h1>
            <p>Active Model: {shortVersion(activeVersion)} · Run ID: {lastRun}</p>
          </div>
        </div>
        <div className="topbar-actions">
          <StatusPill status={status} />
          <button className="icon-button" onClick={() => { fetchModels(); fetchPredictions(); }} title="Refresh data">
            <RefreshCw size={16} />
          </button>
        </div>
      </header>

      <section className="kpi-grid">
        <KpiTile icon={Activity} label="Total Scored" value={current.total.toLocaleString()} detail="Ground truth matching" tone="blue" />
        <KpiTile icon={ShieldAlert} label="Alerts Triggered" value={detected.toLocaleString()} detail={`${cm.tp || 0} True · ${cm.fp || 0} False`} tone="purple" />
        <KpiTile icon={ShieldCheck} label="Precision Rate" value={formatPercent(current.precision)} detail="Alert positive correctness" tone="green" />
        <KpiTile icon={AlertTriangle} label="Missed Incidents" value={missed.toLocaleString()} detail={`Recall rate: ${formatPercent(current.recall)}`} tone="red" />
      </section>

      <section className="workbench">
        <div className="panel performance-panel">
          <div className="panel-header">
            <div>
              <h2><Gauge size={18} /> Real-Time Accuracy</h2>
              <p>{lastUpdated ? `Telemetry sync: ${lastUpdated.toLocaleTimeString()}` : 'Waiting for telemetry'}</p>
            </div>
          </div>
          
          <AccuracyGauge value={current.accuracy} />
          
          <div className="metric-stack">
            <MetricBar label="Precision" value={current.precision} tone="green" historyData={metricHistory.precision} colorCode="#10b981" />
            <MetricBar label="Recall" value={current.recall} tone="amber" historyData={metricHistory.recall} colorCode="#f59e0b" />
            <MetricBar label="F1 Score" value={current.f1} tone="purple" historyData={metricHistory.f1} colorCode="#a855f7" />
          </div>
        </div>

        <div className="panel matrix-panel">
          <div className="panel-header">
            <div>
              <h2><BarChart3 size={18} /> Confusion Matrix</h2>
              <p>Direct classification metrics join from ClickHouse</p>
            </div>
          </div>
          
          <div className="live-matrix-header">
            <span className="live-matrix-dot" /> Live-updating results (Last 24h)
          </div>
          
          <div className="confusion-grid-2x2">
            <div className="matrix-cell-premium tp">
              <span className="matrix-label-premium">True Positives</span>
              <strong className="matrix-value-premium">{(cm.tp || 0).toLocaleString()}</strong>
              <span className="matrix-subtext-premium">Fraud Detected</span>
            </div>
            
            <div className="matrix-cell-premium fp">
              <span className="matrix-label-premium">False Positives</span>
              <strong className="matrix-value-premium">{(cm.fp || 0).toLocaleString()}</strong>
              <span className="matrix-subtext-premium">Legitimate Flagged</span>
            </div>
            
            <div className="matrix-cell-premium fn">
              <span className="matrix-label-premium">False Negatives</span>
              <strong className="matrix-value-premium">{(cm.fn || 0).toLocaleString()}</strong>
              <span className="matrix-subtext-premium">Fraud Missed</span>
            </div>
            
            <div className="matrix-cell-premium tn">
              <span className="matrix-label-premium">True Negatives</span>
              <strong className="matrix-value-premium">{(cm.tn || 0).toLocaleString()}</strong>
              <span className="matrix-subtext-premium">Legitimate Approved</span>
            </div>
          </div>
        </div>

        <aside className="side-stack">
          <div className="panel">
            <div className="panel-header compact">
              <h2><Cpu size={18} /> Model Registry</h2>
            </div>
            <div className="model-list">
              {availableModels.length === 0 ? (
                <div className="empty-state">No registered versions found</div>
              ) : availableModels.map((model) => {
                const active = activeVersion === model.version;
                const loading = loadingModelId === model.version;
                return (
                  <div key={model.version} className={`model-row ${active ? 'active' : ''}`}>
                    <div className="model-row-info">
                      <div className="model-row-title-block">
                        <strong>{shortVersion(model.version)}</strong>
                        {active && (
                          <span className="live-model-badge">
                            <span className="live-model-dot" /> Live
                          </span>
                        )}
                      </div>
                      <span>{model.algorithm || 'Model'} · AUPRC {model.auprc || 'N/A'} · T {model.threshold || 'N/A'}</span>
                    </div>
                    <button className="small-button" disabled={active || loading} onClick={() => handleActivateModel(model.version)}>
                      {active ? 'Active' : loading ? '...' : 'Swap'}
                    </button>
                  </div>
                );
              })}
            </div>
          </div>

          <div className="panel">
            <div className="panel-header compact">
              <h2><ExternalLink size={18} /> Console Links</h2>
            </div>
            <div className="ops-links">
              <a href="http://localhost:30081" target="_blank" rel="noopener noreferrer"><Activity size={18} /> Flink</a>
              <a href="http://localhost:30000" target="_blank" rel="noopener noreferrer"><BarChart3 size={18} /> Grafana</a>
              <a href="http://localhost:30090" target="_blank" rel="noopener noreferrer"><Database size={18} /> Prometheus</a>
            </div>
          </div>
        </aside>
      </section>

      <section className="panel table-panel">
        <div className="panel-header">
          <div>
            <h2><History size={18} /> Recent Pipeline Predictions</h2>
            <p>Live stream of transaction scoring, model outputs, and verified labels</p>
          </div>
        </div>
        <div className="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Timestamp</th>
                <th>Run ID</th>
                <th>Transaction ID</th>
                <th>Model</th>
                <th>Fraud Probability</th>
                <th>Prediction</th>
                <th>Ground Truth</th>
                <th>Validation Result</th>
              </tr>
            </thead>
            <tbody>
              {newestPredictions.length === 0 ? (
                <tr><td colSpan="8" className="empty-cell">No predictions flowing in current window</td></tr>
              ) : newestPredictions.map((p, idx) => (
                <tr key={`${p.transaction_id || idx}-${p.predicted_at || idx}`}>
                  <td className="mono">{p.predicted_at ? String(p.predicted_at).slice(11, 19) : '-'}</td>
                  <td className="mono">{p.run_id || 'unknown'}</td>
                  <td className="mono">{shortId(p.transaction_id, 14)}</td>
                  <td className="mono">{shortVersion(p.model_version)}</td>
                  <td className="mono">{formatProb(p.probability)}</td>
                  <td><DecisionBadge value={p.prediction} /></td>
                  <td>{p.pending ? <span className="badge neutral">PENDING</span> : <DecisionBadge value={p.actual_status} />}</td>
                  <td>
                    {p.pending ? (
                      <span className="result neutral"><RefreshCw size={13} /> Verification Pending</span>
                    ) : p.correct ? (
                      <span className="result good"><CheckCircle2 size={13} /> Correct Class</span>
                    ) : (
                      <span className="result bad"><XCircle size={13} /> Classification Error</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}

export default App;
