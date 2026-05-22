import { useState, useEffect } from 'react';
import { apiCall } from '../hooks/useApi';
import { useToast } from '../hooks/useToast';
import { FiClock, FiCheckCircle, FiAlertCircle, FiEye, FiRefreshCw, FiArrowLeft, FiActivity, FiLayers } from 'react-icons/fi';
import { motion, AnimatePresence } from 'framer-motion';

export default function HistoryView({ isEmbedded = false }) {
  const [runs, setRuns] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedRun, setSelectedRun] = useState(null);
  const toast = useToast();

  const loadHistory = async () => {
    setLoading(true);
    try {
      // Fixed the endpoint path to be consistent with the backend router
      const r = await apiCall('/orchestrate/history?limit=50');
      setRuns(r.runs || []);
    } catch (e) {
      toast('Failed to load history: ' + e.message, 'error');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadHistory();
  }, []);

  const getStatusBadge = (status) => {
    const s = String(status).toUpperCase();
    let color = 'var(--text3)';
    let bg = 'var(--surface2)';
    let icon = null;

    if (s === 'SUCCESS') { color = '#16a34a'; bg = '#f0fdf4'; icon = <FiCheckCircle style={{ marginRight: 4 }} />; }
    else if (s === 'FAILURE') { color = '#ef4444'; bg = '#fef2f2'; icon = <FiAlertCircle style={{ marginRight: 4 }} />; }
    else if (s === 'PARTIAL') { color = '#f59e0b'; bg = '#fffbeb'; icon = <FiActivity style={{ marginRight: 4 }} />; }
    else if (s === 'RUNNING') { color = 'var(--accent)'; bg = 'var(--blue-bg)'; icon = <FiRefreshCw className="spin" style={{ marginRight: 4 }} />; }

    return (
      <span style={{ 
        display: 'inline-flex', alignItems: 'center', padding: '4px 10px', 
        borderRadius: 8, fontSize: 11, fontWeight: 800, color, background: bg,
        border: `1px solid ${color}20`
      }}>
        {icon} {s}
      </span>
    );
  };

  const formatDuration = (start, end) => {
    if (!start || !end) return '—';
    const s = new Date(start);
    const e = new Date(end);
    const diff = Math.floor((e - s) / 1000);
    if (diff < 60) return `${diff}s`;
    return `${Math.floor(diff / 60)}m ${diff % 60}s`;
  };

  return (
    <div className="history-beta-container">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 22, fontWeight: 900, color: 'var(--text1)' }}>History / Monitoring</h2>
          <p style={{ margin: '4px 0 0', fontSize: 13, color: 'var(--text3)' }}>Review Fabric deployments, orchestration runs, strategy usage, and execution outcomes.</p>
        </div>
        <button className="orch-btn ghost tiny" onClick={loadHistory} disabled={loading}>
          <FiRefreshCw className={loading ? 'spin' : ''} style={{ marginRight: 6 }} /> 
          {loading ? 'Refreshing...' : 'Refresh'}
        </button>
      </div>

      <div className="orch-card" style={{ padding: 0, overflow: 'hidden', border: '1px solid var(--border)' }}>
        <div className="premium-table-wrapper" style={{ overflowX: 'auto' }}>
          <table className="preview-table" style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr>
                <th>Batch ID</th>
                <th>Client</th>
                <th>Source</th>
                <th>Strategy</th>
                <th>Workspace</th>
                <th>Status</th>
                <th>Start Time</th>
                <th>Duration</th>
                <th>Progress</th>
                <th style={{ textAlign: 'right' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <tr key={i}>
                    <td colSpan={10}><div className="skeleton" style={{ height: 40, margin: '8px 0' }} /></td>
                  </tr>
                ))
              ) : runs.length === 0 ? (
                <tr>
                  <td colSpan={10} style={{ textAlign: 'center', padding: 40, color: 'var(--text3)' }}>
                    No execution history found.
                  </td>
                </tr>
              ) : runs.map(r => (
                <tr key={r.run_id} className="history-row">
                  <td style={{ fontWeight: 800, color: 'var(--text1)' }}>{r.batch_id}</td>
                  <td>{r.client_name}</td>
                  <td>
                    <span className="source-badge" style={{ background: 'var(--surface2)', color: 'var(--text2)', padding: '2px 8px', borderRadius: 6, fontSize: 10, fontWeight: 700 }}>
                      {r.source_type}
                    </span>
                  </td>
                  <td style={{ fontSize: 12 }}>{r.deployment_strategy ? r.deployment_strategy.replace(/_/g, ' ') : 'Not captured'}</td>
                  <td style={{ fontFamily: 'monospace', fontSize: 11 }}>{r.workspace_id || 'Not captured'}</td>
                  <td>{getStatusBadge(r.status)}</td>
                  <td style={{ fontSize: 12, color: 'var(--text3)' }}>{new Date(r.start_time).toLocaleString()}</td>
                  <td>{formatDuration(r.start_time, r.end_time)}</td>
                  <td>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <div style={{ flex: 1, height: 6, background: 'var(--surface2)', borderRadius: 3, overflow: 'hidden', minWidth: 60 }}>
                        <div style={{ 
                          height: '100%', 
                          width: `${(r.success_count / (r.total_datasets || 1)) * 100}%`, 
                          background: 'var(--green)',
                          borderRadius: 3
                        }} />
                      </div>
                      <span style={{ fontSize: 11, fontWeight: 700 }}>{r.success_count}/{r.total_datasets}</span>
                    </div>
                  </td>
                  <td style={{ textAlign: 'right' }}>
                    <button className="orch-btn ghost tiny" onClick={() => setSelectedRun(r)}>
                      <FiEye style={{ marginRight: 6 }} /> Details
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <AnimatePresence>
        {selectedRun && (
          <div className="mode-modal-overlay" style={{ zIndex: 3000 }} onClick={() => setSelectedRun(null)}>
            <motion.div 
              initial={{ scale: 0.9, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.9, opacity: 0 }}
              className="mode-modal-card"
              style={{ width: 800, padding: 0, overflow: 'hidden' }}
              onClick={e => e.stopPropagation()}
            >
              <div className="dq-editor-header" style={{ padding: '24px 32px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
                  <div style={{ 
                    width: 48, height: 48, borderRadius: 12, 
                    background: selectedRun.status === 'SUCCESS' ? '#f0fdf4' : '#fef2f2',
                    display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 24
                  }}>
                    {selectedRun.status === 'SUCCESS' ? <FiCheckCircle color="#16a34a" /> : <FiAlertCircle color="#ef4444" />}
                  </div>
                  <div>
                    <h3 style={{ margin: 0 }}>Run {selectedRun.batch_id}</h3>
                    <div style={{ fontSize: 13, color: 'var(--text2)', marginTop: 4 }}>
                      {selectedRun.client_name} • {selectedRun.source_type} • {new Date(selectedRun.start_time).toLocaleString()}
                    </div>
                  </div>
                </div>
                <button className="orch-btn ghost tiny" onClick={() => setSelectedRun(null)}><FiArrowLeft /> Back</button>
              </div>

              <div style={{ padding: '0 32px 32px', maxHeight: '70vh', overflowY: 'auto' }}>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, marginBottom: 32 }}>
                   <div style={{ padding: 16, background: 'var(--surface2)', borderRadius: 16, border: '1px solid var(--border)' }}>
                      <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text3)', textTransform: 'uppercase', marginBottom: 8 }}>Status</div>
                      <div>{getStatusBadge(selectedRun.status)}</div>
                   </div>
                   <div style={{ padding: 16, background: 'var(--surface2)', borderRadius: 16, border: '1px solid var(--border)' }}>
                      <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text3)', textTransform: 'uppercase', marginBottom: 8 }}>Datasets</div>
                      <div style={{ fontSize: 24, fontWeight: 900 }}>{selectedRun.total_datasets}</div>
                   </div>
                   <div style={{ padding: 16, background: 'var(--surface2)', borderRadius: 16, border: '1px solid var(--border)' }}>
                      <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text3)', textTransform: 'uppercase', marginBottom: 8 }}>Success</div>
                      <div style={{ fontSize: 24, fontWeight: 900, color: 'var(--green)' }}>{selectedRun.success_count}</div>
                   </div>
                   <div style={{ padding: 16, background: 'var(--surface2)', borderRadius: 16, border: '1px solid var(--border)' }}>
                      <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text3)', textTransform: 'uppercase', marginBottom: 8 }}>Duration</div>
                      <div style={{ fontSize: 24, fontWeight: 900 }}>{formatDuration(selectedRun.start_time, selectedRun.end_time)}</div>
                   </div>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 28 }}>
                  <div className="config-chip"><strong>Strategy:</strong> {selectedRun.deployment_strategy ? selectedRun.deployment_strategy.replace(/_/g, ' ') : 'Not captured'}</div>
                  <div className="config-chip"><strong>Workspace:</strong> {selectedRun.workspace_id || 'Not captured'}</div>
                  <div className="config-chip"><strong>Pipeline:</strong> {selectedRun.pipeline_id || 'Not captured'}</div>
                  <div className="config-chip"><strong>Platform:</strong> {selectedRun.platform || 'Not captured'}</div>
                </div>

                <h4 style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 10 }}>
                  <FiLayers style={{ color: 'var(--accent)' }} /> 
                  Dataset Results Breakdown
                </h4>
                
                <div style={{ border: '1px solid var(--border)', borderRadius: 12, overflow: 'hidden' }}>
                  <table className="preview-table" style={{ width: '100%', marginBottom: 0 }}>
                    <thead>
                      <tr>
                        <th>Dataset</th>
                        <th>Status</th>
                        <th>Bronze Rows</th>
                        <th>Silver Rows</th>
                        <th>Details</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(selectedRun.pipeline_results || []).map((res, i) => (
                        <tr key={i}>
                          <td style={{ fontWeight: 800 }}>{res.dataset_name || res.dataset_id}</td>
                          <td>
                            <span style={{ 
                              fontSize: 10, fontWeight: 800, padding: '2px 8px', borderRadius: 4,
                              background: res.status === 'SUCCESS' ? '#f0fdf4' : '#fef2f2',
                              color: res.status === 'SUCCESS' ? '#16a34a' : '#ef4444'
                            }}>
                              {res.status}
                            </span>
                          </td>
                          <td>{res.metrics?.bronze?.rows_written?.toLocaleString() || '—'}</td>
                          <td>{res.metrics?.silver?.rows_written?.toLocaleString() || '—'}</td>
                          <td style={{ fontSize: 12, color: 'var(--text3)' }}>
                             {res.status === 'FAILURE' ? <span style={{ color: 'var(--red)' }}>{res.reason}</span> : 'Completed successfully'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                {selectedRun.error_message && (
                  <div style={{ marginTop: 24, padding: 20, background: '#fef2f2', borderRadius: 16, border: '1px solid #fecaca', color: '#ef4444' }}>
                    <div style={{ fontWeight: 900, marginBottom: 4, display: 'flex', alignItems: 'center', gap: 8 }}>
                      <FiAlertCircle /> Execution System Error
                    </div>
                    <div style={{ fontSize: 13, opacity: 0.9 }}>{selectedRun.error_message}</div>
                  </div>
                )}
              </div>

              <div className="step-footer" style={{ padding: '20px 32px', background: 'var(--surface2)', borderRadius: 0 }}>
                <div style={{ flex: 1 }} />
                <button className="orch-btn primary" onClick={() => setSelectedRun(null)}>Understood</button>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>
      
      <style dangerouslySetInnerHTML={{ __html: `
        .history-row:hover { background: var(--surface2) !important; }
        .preview-table th { text-align: left; background: var(--surface2); padding: 12px 20px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text3); border-bottom: 1px solid var(--border); }
        .preview-table td { padding: 14px 20px; border-bottom: 1px solid var(--border); font-size: 13px; color: var(--text2); }
        .history-beta-container { animation: fadeIn 0.4s ease-out; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
      `}} />
    </div>
  );
}
