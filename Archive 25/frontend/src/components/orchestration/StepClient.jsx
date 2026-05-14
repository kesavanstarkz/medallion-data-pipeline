import { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { FiCheck, FiSearch, FiPlus, FiRefreshCw, FiTrash2, FiClock, FiAlertTriangle, FiX, FiLink, FiBox, FiCloud, FiFolder, FiDatabase, FiServer, FiCpu, FiHardDrive, FiShare2, FiShield, FiInfo, FiChevronRight, FiEdit2, FiUploadCloud } from 'react-icons/fi';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import FluentSelect from '../FluentSelect';
import logo from '../../assets/images/image.png';

const itemVariants = {
  initial: { opacity: 0, y: 10 },
  animate: { opacity: 1, y: 0 }
};

export default function StepClient({ 
  clients, clientLoading, selectedClient, setSelectedClient, fetchClients, onNext, call, toast, 
  sourceForm, setSourceForm, registerSource, savingSource, testConnection, testingConnection, 
  connectionVerified, setConnectionVerified, testResult, setTestResult, extractedFabricData, setExtractedFabricData, 
  onDeploySuccess, targets, setTargets, selectedTarget, setSelectedTarget, fetchTargets,
  setShowUploadModal
}) {
  const navigate = useNavigate();
  const [tab, setTab] = useState('existing'); // 'existing' | 'register' | 'target'
  const [searchQuery, setSearchQuery] = useState('');
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [clientToDelete, setClientToDelete] = useState(null);

  // Target Setup State
  const [targetType, setTargetType] = useState('None');
  const [targetFields, setTargetFields] = useState({});
  const [testingTarget, setTestingTarget] = useState(false);
  const [registeringTarget, setRegisteringTarget] = useState(false);
  const [targetTestResult, setTargetTestResult] = useState(null);
  const [showDeleteTargetConfirm, setShowDeleteTargetConfirm] = useState(false);
  const [targetToDelete, setTargetToDelete] = useState(null);

  const TARGET_TYPES = [
    "None", "Fabric Warehouse", "SQL Server", "PostgreSQL", "MySQL", "Snowflake", 
    "Redshift", "BigQuery", "Azure Synapse", "MongoDB", "Cosmos DB", "AWS S3", 
    "Azure ADLS", "OneLake", "Parquet", "CSV", "JSON"
  ];

  const TARGET_FIELD_MAP = {
    "Fabric Warehouse": ["Workspace ID", "Warehouse ID", "SQL Endpoint", "Database Name", "Username", "Password / Token", "Schema", "Table", "Write Mode"],
    "SQL Server": ["Host", "Port", "Database", "Username", "Password", "Schema", "Table", "Encrypt Toggle", "Write Mode"],
    "PostgreSQL": ["Host", "Port", "Database", "Username", "Password", "Schema", "Table", "SSL Mode", "Write Mode"],
    "MySQL": ["Host", "Port", "Database", "Username", "Password", "Table", "Charset", "SSL", "Write Mode"],
    "Snowflake": ["Account", "Username", "Password", "Warehouse", "Database", "Schema", "Role", "Table", "Write Mode"],
    "Redshift": ["Cluster Endpoint", "Port", "Database", "Username", "Password", "Schema", "Table", "SSL", "Write Mode"],
    "BigQuery": ["Project ID", "Dataset", "Table", "Service Account JSON Upload", "Location", "Write Mode"],
    "Azure Synapse": ["Server", "Database", "Username", "Password", "Schema", "Table", "Auth Type", "Write Mode"],
    "MongoDB": ["Connection URI", "Database", "Collection", "Auth Database", "Write Mode"],
    "Cosmos DB": ["Endpoint URL", "Primary Key", "Database", "Container", "API Type", "Write Mode"],
    "AWS S3": ["Bucket", "Folder Path", "Region", "Access Key", "Secret Key", "File Format", "Compression", "Partition Strategy"],
    "Azure ADLS": ["Storage Account", "Container", "Folder Path", "Account Key / SAS", "File Format", "Compression"],
    "OneLake": ["Workspace ID", "Lakehouse", "Folder Path", "Access Token", "File Format", "Write Mode"],
    "Parquet": ["Output Path", "Partition Strategy", "Compression"],
    "CSV": ["Output Path", "Delimiter", "Encoding", "Header Toggle"],
    "JSON": ["Output Path", "JSON Mode", "Encoding", "Pretty Print Toggle"]
  };


  const filteredClients = (clients || []).filter(c =>
    c.toLowerCase().includes(searchQuery.toLowerCase())
  );
  const apiHasScanDetails = sourceForm.source_type === 'API' && !!sourceForm.base_url && !!String(sourceForm.endpoints || '').trim();
  const apiCanSkipConnection = sourceForm.source_type === 'API' && !apiHasScanDetails;

  const handleSelectClient = (c) => {
    setSelectedClient(c);
    localStorage.setItem('client_name', c);
    fetchTargets(c);
  };

  const deleteClient = (e, c) => {
    e.stopPropagation();
    setClientToDelete(c);
    setShowDeleteConfirm(true);
  };

  const handleConfirmDelete = async () => {
    if (!clientToDelete) return;
    try {
      await call(`/config/clients/${clientToDelete}`, 'DELETE');
      toast(`Successfully deleted client "${clientToDelete}"`, 'success');
      if (selectedClient === clientToDelete) {
        setSelectedClient(null);
      }
      setShowDeleteConfirm(false);
      setClientToDelete(null);
      fetchClients();
    } catch (err) {
      toast(`Deletion failed: ${err.message}`, 'error');
    }
  };

  const handleTestTarget = async () => {
    if (!selectedClient) return;
    setTestingTarget(true);
    setTargetTestResult(null);
    try {
      const res = await call('/config/targets/test', 'POST', {
        client_name: selectedClient,
        target_type: targetType,
        target_name: targetFields["target_name"] || `${selectedClient}_${targetType.replace(/\s/g, '_')}`,
        credential_config: targetFields
      });
      setTargetTestResult({ type: res.status === 'SUCCESS' ? 'success' : 'error', message: res.message });
      if (res.status === 'SUCCESS') toast('Target connection verified', 'success');
    } catch (err) {
      setTargetTestResult({ type: 'error', message: err.message });
      toast('Test failed: ' + err.message, 'error');
    } finally {
      setTestingTarget(false);
    }
  };

  const handleRegisterTarget = async () => {
    if (!selectedClient) return;
    setRegisteringTarget(true);
    try {
      const name = targetFields["target_name"] || `${selectedClient}_${targetType.replace(/\s/g, '_')}`;
      await call('/config/targets', 'POST', {
        client_name: selectedClient,
        target_type: targetType,
        target_name: name,
        credential_config: targetFields
      });
      toast(`Target "${name}" registered successfully`, 'success');
      fetchTargets(selectedClient);
      setTab('existing');
    } catch (err) {
      toast('Registration failed: ' + err.message, 'error');
    } finally {
      setRegisteringTarget(false);
    }
  };

  const handleEditTarget = (t) => {
    setTargetType(t.target_type);
    setTargetFields({ ...t.credential_config, target_name: t.target_name });
    setSelectedTarget(t);
    setTab('target');
  };

  const handleDeleteTarget = (e, t) => {
    e.stopPropagation();
    setTargetToDelete(t);
    setShowDeleteTargetConfirm(true);
  };

  const handleConfirmDeleteTarget = async () => {
    if (!targetToDelete) return;
    try {
      await call(`/config/targets/${targetToDelete.target_id}`, 'DELETE');
      toast('Target deleted successfully', 'success');
      fetchTargets(selectedClient);
      if (selectedTarget?.target_id === targetToDelete.target_id) {
        setSelectedTarget(null);
      }
      setShowDeleteTargetConfirm(false);
      setTargetToDelete(null);
    } catch (err) {
      toast('Deletion failed: ' + err.message, 'error');
    }
  };

  // 4. Source Type Switching Bug - Reset stale fields
  useEffect(() => {
    const { source_type } = sourceForm;
    // Keep client_name but reset others if not switching back to same type
    setSourceForm(prev => ({
      ...prev,
      source_name: '',
      base_url: '',
      auth_type: 'none',
      auth_token: '',
      endpoints: '',
      aws_access_key_id: '',
      aws_secret_access_key: '',
      region: '',
      bucket_name: '',
      azure_account_name: '',
      azure_account_key: '',
      azure_container_name: '',
      source_type // preserve current selection
    }));
    setConnectionVerified(false);
    setTestResult(null);
  }, [sourceForm.source_type]);

  const canRegister = () => {
    if (!sourceForm.client_name) return false;
    if (sourceForm.source_type === 'LOCAL') return true;
    
    const hasSourceName = !!sourceForm.source_name;
    if (!hasSourceName) return false;

    // For cloud/API, connection must be verified OR it's an API that can skip
    return connectionVerified || apiCanSkipConnection;
  };

  return (
    <motion.div
      key="step1"
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
      exit={{ opacity: 0, x: -20 }}
      className="orch-step-panel"
    >
      <div className="step-header-responsive">
        <div className="step-header-text">
          <h2 className="step-title">DEA Agent — Client Setup</h2>
          <p className="step-sub">Register a new client or choose an existing one to begin.</p>
        </div>

        <div className="step-header-actions">
          <div className="step-tabs">
            <button
              className={`step-tab ${tab === 'existing' ? 'active' : ''}`}
              onClick={() => setTab('existing')}
            >
              Choose Existing
            </button>
            <button
              className={`step-tab ${tab === 'register' ? 'active' : ''}`}
              onClick={() => setTab('register')}
            >
              <FiPlus /> Register Client
            </button>
            <button
              className={`step-tab ${tab === 'target' ? 'active' : ''}`}
              onClick={() => {
                if (!selectedClient) {
                  toast('Please select or register a client before configuring a target.', 'warning');
                  return;
                }
                setTab('target');
              }}
            >
              <FiShare2 /> Targets
            </button>
          </div>

          <div className="header-logo-divider" />
          <img src={logo} alt="Agilisium" className="step-header-logo" />
        </div>
      </div>

      <div className="step-body">
        {tab === 'existing' && (
          <div className="existing-client-container">
            <div className="client-search-bar">
              <div className="search-input-wrapper">
                <FiSearch className="search-icon" />
                <input
                  type="text"
                  placeholder="Search clients..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="search-input"
                />
              </div>

              <button className="orch-btn ghost tiny" onClick={fetchClients} title="Refresh Client List">
                <FiRefreshCw className="spin-on-hover" />
              </button>
            </div>

            <div className="client-grid-responsive">
              {clientLoading ? (
                Array.from({ length: 8 }).map((_, i) => (
                  <div key={i} className="client-card skeleton" aria-hidden>
                    <div className="skeleton-circle" style={{ width: 44, height: 44, borderRadius: 12 }} />
                    <div style={{ flex: 1 }}>
                      <div className="skeleton-line" style={{ width: '70%', height: 14 }} />
                      <div className="skeleton-line" style={{ width: '50%', height: 10, marginTop: 6 }} />
                    </div>
                  </div>
                ))
              ) : filteredClients.length === 0 ? (
                <div className="empty-client-state">
                  <p>{searchQuery ? `No clients matching "${searchQuery}"` : 'No clients found. Register one first.'}</p>
                </div>
              ) : (
                filteredClients.map((c) => (
                  <motion.div
                    key={c}
                    variants={itemVariants}
                    initial="initial"
                    animate="animate"
                    className={`client-card ${selectedClient === c ? 'selected' : ''}`}
                    onClick={() => handleSelectClient(c)}
                    role="button"
                  >
                    <div className="client-avatar">
                      {c.split(/[-_ ]/).map(p => p[0]).slice(0, 2).join('').toUpperCase()}
                    </div>
                    <div className="client-info">
                      <div className="client-name">{c}</div>
                      <div className="client-env">Active Infrastructure</div>
                    </div>
                    <div className="client-card-status">
                      <div className="client-card-actions">
                        <button
                          className="delete-client-btn"
                          onClick={(e) => deleteClient(e, c)}
                          title="Delete Client"
                        >
                          <FiTrash2 size={14} />
                        </button>
                      </div>
                      {selectedClient === c && (
                        <div className="selection-badge">
                          <FiCheck size={12} />
                        </div>
                      )}
                    </div>
                  </motion.div>
                ))
              )}
            </div>

            {selectedClient && (
              <div className="registered-targets-section">
                <div className="section-header">
                  <h4 className="section-title">
                    <FiShare2 /> Registered Targets for {selectedClient}
                  </h4>
                  <button 
                    className="orch-btn ghost tiny" 
                    onClick={() => { setTargetType('None'); setTargetFields({}); setTab('target'); }}
                  >
                    <FiPlus /> New Target
                  </button>
                </div>
                
                <div className="target-grid-responsive">
                  {(targets || []).map(t => (
                    <div 
                      key={t.target_id} 
                      className={`target-card-premium ${selectedTarget?.target_id === t.target_id ? 'selected' : ''}`}
                      onClick={() => setSelectedTarget(t)}
                    >
                      <div className="target-card-header">
                        <div className="target-icon-box">
                          <FiDatabase size={18} />
                        </div>
                        <div className="target-card-actions">
                          <button className="orch-btn ghost tiny" onClick={(e) => { e.stopPropagation(); handleEditTarget(t); }}><FiEdit2 size={14} /></button>
                          <button className="orch-btn ghost tiny" onClick={(e) => handleDeleteTarget(e, t)}><FiTrash2 size={14} /></button>
                        </div>
                      </div>
                      <div className="target-info">
                        <div className="target-name">{t.target_name}</div>
                        <div className="target-type">{t.target_type}</div>
                      </div>
                      {selectedTarget?.target_id === t.target_id && (
                        <div className="target-selected-mark">
                          <FiCheck size={10} /> Selected
                        </div>
                      )}
                    </div>
                  ))}
                  {(!targets || targets.length === 0) && (
                    <div className="empty-target-state">
                      No targets configured for this client.
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        )}

        {tab === 'register' && (
          <div className="register-client-container">
            <div className="register-field full">
              <label>Select Source Type</label>
              <div className="source-type-grid-responsive">
                {[
                  { id: 'LOCAL', label: 'Local Files', icon: <FiFolder />, color: '#10b981', desc: 'Direct upload' },
                  { id: 'API', label: 'REST API', icon: <FiLink />, color: '#3b82f6', desc: 'Secure endpoints' },
                  { id: 'S3', label: 'AWS S3', icon: <FiBox />, color: '#f59e0b', desc: 'Cloud buckets' },
                  { id: 'ADLS', label: 'Azure ADLS', icon: <FiCloud />, color: '#0078d4', desc: 'Azure storage' }
                ].map(t => (
                  <motion.div
                    key={t.id}
                    whileHover={{ y: -4, scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                    onClick={() => {
                      setSourceForm({ ...sourceForm, source_type: t.id });
                    }}
                    className={`source-type-card-premium ${(sourceForm.source_type || 'API') === t.id ? 'active' : ''}`}
                    style={{ '--theme-color': t.color }}
                  >
                    <div className="source-type-icon-wrapper">
                      {t.icon}
                    </div>
                    <div className="source-type-info">
                      <div className="source-type-label">{t.label}</div>
                      <div className="source-type-desc">{t.desc}</div>
                    </div>
                  </motion.div>
                ))}
              </div>
            </div>

            <div className="register-form-grid">
              <div className="register-field full">
                <label>Client Name</label>
                <input
                  value={sourceForm.client_name}
                  onChange={e => {
                    setSourceForm({ ...sourceForm, client_name: e.target.value });
                    setConnectionVerified(false);
                  }}
                  placeholder="e.g. AMGEN"
                  className="orch-input"
                />
              </div>

              {(sourceForm.source_type === 'API' || !sourceForm.source_type) && (
                <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="api-config-grid">
                  <div className="register-field">
                    <label>Source Name</label>
                    <input
                      value={sourceForm.source_name}
                      onChange={e => {
                        setSourceForm({ ...sourceForm, source_name: e.target.value });
                        setConnectionVerified(false);
                      }}
                      placeholder="e.g. disease-api"
                      className="orch-input"
                    />
                  </div>
                  <div className="register-field">
                    <label>Base URL</label>
                    <input
                      value={sourceForm.base_url}
                      onChange={e => {
                        setSourceForm({ ...sourceForm, base_url: e.target.value });
                        setConnectionVerified(false);
                      }}
                      placeholder="https://api.example.com"
                      className="orch-input"
                    />
                  </div>
                  <div className="register-field full">
                    <label>Authentication</label>
                    <FluentSelect
                      value={sourceForm.auth_type}
                      onChange={e => {
                        setSourceForm({ ...sourceForm, auth_type: e.target.value });
                        setConnectionVerified(false);
                      }}
                      options={[
                        { value: 'none', label: '🔓 No auth (public API)' },
                        { value: 'bearer', label: '🔑 Bearer token' },
                        { value: 'api_key', label: '🗝️ API key header' },
                        { value: 'basic', label: '🔒 Basic auth' }
                      ]}
                    />
                  </div>
                  {sourceForm.auth_type !== 'none' && (
                    <div className="register-field full">
                      <label>{sourceForm.auth_type === 'api_key' ? 'API Key' : 'Token / Password'}</label>
                      <input
                        type="password"
                        value={sourceForm.auth_token || ''}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, auth_token: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="Enter credentials..."
                        className="orch-input"
                      />
                    </div>
                  )}
                  <div className="register-field full">
                    <label>Endpoints (comma-separated)</label>
                    <input
                      value={sourceForm.endpoints}
                      onChange={e => {
                        setSourceForm({ ...sourceForm, endpoints: e.target.value });
                        setConnectionVerified(false);
                      }}
                      placeholder="users,posts,todos"
                      className="orch-input"
                    />
                  </div>
                </motion.div>
              )}

              {sourceForm.source_type === 'S3' && (
                <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="api-config-grid">
                  <div className="register-field">
                    <label>Source Name</label>
                    <input
                      value={sourceForm.source_name}
                      onChange={e => setSourceForm({ ...sourceForm, source_name: e.target.value })}
                      placeholder="e.g. clinical-data-s3"
                      className="orch-input"
                    />
                  </div>
                  <div className="register-field">
                    <label>AWS Region</label>
                    <input
                      value={sourceForm.region}
                      onChange={e => setSourceForm({ ...sourceForm, region: e.target.value })}
                      placeholder="us-east-1"
                      className="orch-input"
                    />
                  </div>
                  <div className="register-field">
                    <label>S3 Bucket Name</label>
                    <input
                      value={sourceForm.bucket_name}
                      onChange={e => setSourceForm({ ...sourceForm, bucket_name: e.target.value })}
                      placeholder="my-data-bucket"
                      className="orch-input"
                    />
                  </div>
                  <div className="register-field">
                    <label>Access Key ID</label>
                    <input
                      value={sourceForm.aws_access_key_id}
                      onChange={e => setSourceForm({ ...sourceForm, aws_access_key_id: e.target.value })}
                      placeholder="AKIA..."
                      className="orch-input"
                    />
                  </div>
                  <div className="register-field full">
                    <label>Secret Access Key</label>
                    <input
                      type="password"
                      value={sourceForm.aws_secret_access_key}
                      onChange={e => setSourceForm({ ...sourceForm, aws_secret_access_key: e.target.value })}
                      placeholder="Secret Key"
                      className="orch-input"
                    />
                  </div>
                </motion.div>
              )}

              {sourceForm.source_type === 'ADLS' && (
                <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="api-config-grid">
                  <div className="register-field">
                    <label>Source Name</label>
                    <input
                      value={sourceForm.source_name}
                      onChange={e => setSourceForm({ ...sourceForm, source_name: e.target.value })}
                      placeholder="e.g. adls-raw-zone"
                      className="orch-input"
                    />
                  </div>
                  <div className="register-field">
                    <label>Account Name</label>
                    <input
                      value={sourceForm.azure_account_name}
                      onChange={e => setSourceForm({ ...sourceForm, azure_account_name: e.target.value })}
                      placeholder="mystorageaccount"
                      className="orch-input"
                    />
                  </div>
                  <div className="register-field">
                    <label>Container Name</label>
                    <input
                      value={sourceForm.azure_container_name}
                      onChange={e => setSourceForm({ ...sourceForm, azure_container_name: e.target.value })}
                      placeholder="raw-data"
                      className="orch-input"
                    />
                  </div>
                  <div className="register-field full">
                    <label>Account Key</label>
                    <input
                      type="password"
                      value={sourceForm.azure_account_key}
                      onChange={e => setSourceForm({ ...sourceForm, azure_account_key: e.target.value })}
                      placeholder="Account Key"
                      className="orch-input"
                    />
                  </div>
                </motion.div>
              )}

              {sourceForm.source_type === 'LOCAL' && (
                <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="local-info-box">
                  <FiUploadCloud size={24} color="var(--blue)" />
                  <div className="info-content">
                    <div className="info-title">Local Files Selection</div>
                    <div className="info-sub">Provide the Client Name above, then click "Continue to Upload" to open the secure upload portal.</div>
                  </div>
                </motion.div>
              )}
            </div>

            {testResult && (
              <div className={`test-result-box ${testResult.type}`}>
                {testResult.type === 'success' ? '✅' : '❌'} {testResult.message}
              </div>
            )}

            <div className="register-actions">
              {sourceForm.source_type !== 'LOCAL' && (
                <button
                  className="orch-btn ghost"
                  onClick={testConnection}
                  disabled={testingConnection || !sourceForm.client_name || !sourceForm.source_name}
                >
                  {testingConnection ? 'Testing...' : '⚡ Test Connection'}
                </button>
              )}

              <button
                className={`orch-btn primary premium-btn ${canRegister() ? '' : 'disabled-btn'}`}
                onClick={() => {
                  if (sourceForm.source_type === 'LOCAL') {
                    setSelectedClient(sourceForm.client_name);
                    setShowUploadModal(true);
                  } else {
                    registerSource();
                  }
                }}
                disabled={savingSource || !canRegister()}
              >
                {savingSource ? 'Registering...' :
                  sourceForm.source_type === 'LOCAL' ? 'Continue to Upload →' :
                    `🚀 Register Source`}
              </button>
            </div>
          </div>
        )}


        {tab === 'target' && (
          <div className="target-config-container">
            <div className="target-config-header">
              <div className="client-badge">Selected: {selectedClient}</div>
              <h3 className="target-title">Target System Setup</h3>
              <p className="target-sub">Configure destination systems for data extraction.</p>
            </div>

            <div className="target-form-wrapper">
              <div className="register-field full">
                <label>TARGET TYPE</label>
                <FluentSelect
                  value={targetType}
                  onChange={(e) => {
                    setTargetType(e.target.value);
                    setTargetFields({});
                    setTargetTestResult(null);
                  }}
                  options={TARGET_TYPES.map(t => ({ value: t, label: t }))}
                />
              </div>

              {targetType === 'None' ? (
                <div className="local-info-box">
                  <FiInfo size={20} color="var(--blue)" />
                  <div className="info-content">
                    <div className="info-title">No Target Selected</div>
                    <div className="info-sub">Intelligence will continue using source-only context.</div>
                  </div>
                </div>
              ) : (
                <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="target-fields-grid">
                  <div className="register-field full">
                    <label>Target Name</label>
                    <input 
                      value={targetFields["target_name"] || ''} 
                      onChange={(e) => setTargetFields({...targetFields, target_name: e.target.value})}
                      placeholder="e.g. Fabric_Warehouse_Gold"
                      className="orch-input"
                    />
                  </div>
                  {TARGET_FIELD_MAP[targetType].map(field => (
                    <div key={field} className={`register-field ${["SQL Endpoint", "Connection URI", "Endpoint URL", "Service Account JSON Upload", "Folder Path", "Output Path"].includes(field) ? 'full' : ''}`}>
                      <label>{field}</label>
                      {field === 'Write Mode' ? (
                        <FluentSelect 
                          value={targetFields[field] || 'Append'}
                          onChange={(e) => setTargetFields({...targetFields, [field]: e.target.value})}
                          options={[
                            { value: 'Append', label: 'Append' },
                            { value: 'Overwrite', label: 'Overwrite' },
                            { value: 'Upsert', label: 'Upsert' }
                          ]}
                        />
                      ) : (
                        <input 
                          type={field.includes('Password') || field.includes('Key') || field.includes('Token') ? 'password' : 'text'}
                          value={targetFields[field] || ''} 
                          onChange={(e) => setTargetFields({...targetFields, [field]: e.target.value})}
                          placeholder={field}
                          className="orch-input"
                        />
                      )}
                    </div>
                  ))}
                </motion.div>
              )}

              {targetTestResult && (
                <div className={`test-result-box ${targetTestResult.type}`}>
                  {targetTestResult.type === 'success' ? '✅' : '❌'} {targetTestResult.message}
                </div>
              )}

              <div className="register-actions">
                {targetType !== 'None' && (
                  <button
                    className="orch-btn ghost"
                    onClick={handleTestTarget}
                    disabled={testingTarget}
                  >
                    {testingTarget ? 'Testing...' : '⚡ Test Target'}
                  </button>
                )}

                <button
                  className="orch-btn primary premium-btn"
                  onClick={handleRegisterTarget}
                  disabled={registeringTarget || targetType === 'None'}
                >
                  {registeringTarget ? 'Saving...' : '💾 Save Target'}
                </button>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Main Footer for Step 1 */}
      <AnimatePresence>
        {selectedClient && tab !== 'register' && (
          <motion.div
            initial={{ opacity: 0, scale: 0.98, y: 10 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.98, y: 10 }}
            className="step-footer-actions-container"
          >
            <div className="selected-info">
              Ready to proceed with: <strong>{selectedClient}</strong>
            </div>
            <button
              className="orch-btn primary premium-btn"
              onClick={onNext}
            >
              Continue <FiChevronRight />
            </button>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Delete Modals */}
      <AnimatePresence>
        {showDeleteConfirm && (
          <div className="pi-modal-overlay">
            <motion.div initial={{ scale: 0.9, opacity: 0 }} animate={{ scale: 1, opacity: 1 }} className="pi-modal-content" style={{ maxWidth: 400 }}>
              <div className="pi-modal-header">
                <h3>Delete Client</h3>
                <button className="pi-modal-close" onClick={() => setShowDeleteConfirm(false)}><FiX /></button>
              </div>
              <div className="pi-modal-body">
                <p>Are you sure you want to delete client <strong>{clientToDelete}</strong>? This action cannot be undone.</p>
              </div>
              <div className="pi-modal-footer">
                <button className="orch-btn ghost" onClick={() => setShowDeleteConfirm(false)}>Cancel</button>
                <button className="orch-btn primary" onClick={handleConfirmDelete} style={{ background: 'var(--red)' }}>Delete</button>
              </div>
            </motion.div>
          </div>
        )}
        
        {showDeleteTargetConfirm && (
          <div className="pi-modal-overlay">
            <motion.div initial={{ scale: 0.9, opacity: 0 }} animate={{ scale: 1, opacity: 1 }} className="pi-modal-content" style={{ maxWidth: 400 }}>
              <div className="pi-modal-header">
                <h3>Delete Target</h3>
                <button className="pi-modal-close" onClick={() => setShowDeleteTargetConfirm(false)}><FiX /></button>
              </div>
              <div className="pi-modal-body">
                <p>Are you sure you want to delete target <strong>{targetToDelete?.target_name}</strong>?</p>
              </div>
              <div className="pi-modal-footer">
                <button className="orch-btn ghost" onClick={() => setShowDeleteTargetConfirm(false)}>Cancel</button>
                <button className="orch-btn primary" onClick={handleConfirmDeleteTarget} style={{ background: 'var(--red)' }}>Delete</button>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
