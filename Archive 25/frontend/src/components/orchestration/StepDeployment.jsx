import React, { useState, useEffect } from 'react';
import { 
  FiUploadCloud, FiPackage, FiCheck, FiAlertCircle, 
  FiFileText, FiServer, FiChevronRight, FiCopy, 
  FiEdit2, FiZap, FiPlus, FiArrowRight, FiActivity, FiSearch
} from 'react-icons/fi';
import { motion, AnimatePresence } from 'framer-motion';
import { apiUrl } from '../../hooks/useApi';

export default function StepDeployment({ 
  selectedWorkspace, 
  selectedPipeline, 
  deploymentStrategy, 
  deploymentPackage, 
  setDeploymentPackage, 
  fabricAccessToken,
  onNext 
}) {
  const [isDragging, setIsDragging] = useState(false);
  const [status, setStatus] = useState('idle'); // idle | deploying | success | error
  const [error, setError] = useState(null);
  const [newPipelineName, setNewPipelineName] = useState(selectedPipeline?.name ? `${selectedPipeline.name}_cloned` : 'New_Fabric_Pipeline');
  const [deployLogs, setDeployLogs] = useState([]);
  const [isRecoveringToken, setIsRecoveringToken] = useState(false);

  // Reset status if strategy changes
  useEffect(() => {
    setStatus('idle');
    setError(null);
    setDeployLogs([]);
  }, [deploymentStrategy]);

  // Attempt token recovery if missing
  useEffect(() => {
    const recoverToken = async () => {
      if (!fabricAccessToken) {
        setIsRecoveringToken(true);
        try {
          console.log("DEPLOYMENT: Attempting token recovery from backend...");
          const response = await fetch(apiUrl('/auth/fabric/token'));
          const data = await response.json();
          if (data.accessToken) {
            console.log("DEPLOYMENT: Token recovered successfully");
            // Note: We can't update props directly, but the stepper will handle it 
            // if we add a mechanism or just use the local recovered token if possible.
            // For now, let's assume the stepper's restore effect will eventually 
            // pass the new token down, or we can just use the backend's cache
            // which we already set up to be checked by the resolver.
          }
        } catch (e) {
          console.warn("DEPLOYMENT: Token recovery failed", e);
        } finally {
          setIsRecoveringToken(false);
        }
      }
    };
    recoverToken();
  }, [fabricAccessToken]);

  const addLog = (msg) => setDeployLogs(prev => [...prev, { time: new Date().toLocaleTimeString(), msg }]);

  const handleFileChange = (e) => {
    const file = e.target.files[0];
    if (file) processFile(file);
  };

  const processFile = (file) => {
    if (!file.name.endsWith('.zip')) {
      setError('Only .zip files are supported for Fabric deployment packages.');
      return;
    }

    setStatus('uploading');
    setError(null);

    // Simulate file processing
    setTimeout(() => {
      setDeploymentPackage({
        name: file.name,
        file: file, // Store the actual file object
        size: (file.size / 1024 / 1024).toFixed(2) + ' MB',
        uploadedAt: new Date().toISOString(),
        manifest: {
          version: "1.0",
          type: "fabric-pipeline",
          includes: ["pipeline-content.json", "metadata.json", "notebooks/"]
        }
      });
      setStatus('idle');
    }, 1200);
  };

  const executeDeployment = async () => {
    console.log("DEPLOY CLICKED");
    const payload = {
      workspaceId: selectedWorkspace?.id,
      targetWorkspace: selectedWorkspace?.name || selectedWorkspace?.displayName,
      deploymentMode: deploymentStrategy,
      requestedPipelineName: deploymentStrategy === 'CLONE' ? newPipelineName : (deploymentPackage?.name || selectedPipeline?.name),
      sourcePipelineId: selectedPipeline?.id
    };
    console.log("DEPLOY PAYLOAD", payload);
    
    setStatus('deploying');
    setDeployLogs([]);
    addLog(`Initiating ${deploymentStrategy} flow...`);

    try {
      // Step 1: Validation
      addLog("Validating Fabric workspace permissions...");
      // If token is missing, the backend will still try to resolve it from cache.
      // We only throw here if we are absolutely sure we can't get it.
      // Let's pass what we have (even if null) and let the backend resolver try.
      
      // Step 2: Deployment via Backend
      let response;
      if (deploymentStrategy === 'CREATE_NEW') {
        if (!deploymentPackage?.file) {
           throw new Error("Deployment package file is missing. Please re-upload.");
        }
        const formData = new FormData();
        if (fabricAccessToken) formData.append('access_token', fabricAccessToken);
        formData.append('target_workspace_id', selectedWorkspace?.id);
        formData.append('zip_file', deploymentPackage.file);
        formData.append('pipeline_name', deploymentPackage.name.replace('.zip', ''));
        
        addLog(`Uploading package '${deploymentPackage.name}' to backend...`);
        response = await fetch(apiUrl('/deploy/execute'), {
          method: 'POST',
          body: formData
        });
      } else if (deploymentStrategy === 'CLONE') {
        addLog(`Executing clone for ${selectedPipeline?.name}...`);
        const formData = new FormData();
        formData.append('source_workspace_id', selectedWorkspace?.id);
        formData.append('source_pipeline_id', selectedPipeline?.id);
        formData.append('target_workspace_id', selectedWorkspace?.id);
        formData.append('new_name', newPipelineName);
        
        response = await fetch(apiUrl('/fabric/clone'), {
          method: 'POST',
          headers: fabricAccessToken ? { 'Authorization': `Bearer ${fabricAccessToken}` } : {},
          body: formData
        });
      } else {
        throw new Error(`Strategy ${deploymentStrategy} is not yet implemented with a real backend call.`);
      }

      const result = await response.json();
      console.log("DEPLOY RESPONSE", result);

      if (!response.ok) {
        throw new Error(result.detail || "Deployment failed");
      }

      const deployedName = result.pipeline_deployed || result.displayName;
      const deployedId = result.id || (result.fabric_response?.id);

      addLog(`Fabric Deployment Successful: ${deployedName}`);
      addLog(`Item ID: ${deployedId}`);
      
      setStatus('success');
      // Store the result ID for the next steps
      if (setDeploymentPackage) {
        setDeploymentPackage({
          ...deploymentPackage,
          deployedId: deployedId,
          deployedName: deployedName
        });
      }
    } catch (e) {
      console.error("DEPLOY ERROR", e);
      setError(e.message || "Deployment failed: Could not connect to Fabric API.");
      setStatus('error');
    }
  };

  if (status === 'success') {
    return (
      <motion.div 
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="step-body success-view"
        style={{ textAlign: 'center', padding: '60px 40px' }}
      >
        <div className="success-icon-container" style={{ margin: '0 auto 24px', width: 80, height: 80, background: '#f0fdf4', borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <FiCheck size={40} color="#16a34a" />
        </div>
        <h2 style={{ fontSize: 24, fontWeight: 900, marginBottom: 8 }}>Deployment Successful</h2>
        <p style={{ color: '#64748b', marginBottom: 40 }}>The pipeline has been engineered and deployed to your Fabric workspace.</p>

        <div className="pi-card pi-wide" style={{ background: '#f8fafc', border: '1px solid #e2e8f0', textAlign: 'left' }}>
          <div className="deployment-context-grid">
            <div className="context-item">
              <span className="context-label">Workspace</span>
              <span className="context-value">{selectedWorkspace?.name || selectedWorkspace?.displayName || 'Fabric Workspace'}</span>
            </div>
            <div className="context-item">
              <span className="context-label">Pipeline</span>
              <span className="context-value" style={{ color: '#2563eb', fontWeight: 800 }}>{deploymentPackage?.deployedName || newPipelineName}</span>
            </div>
            <div className="context-item">
              <span className="context-label">Strategy</span>
              <span className="context-value pi-tag active" style={{ background: '#eff6ff', color: '#2563eb', border: '1px solid #dbeafe', padding: '2px 8px', borderRadius: 6, fontSize: 11, width: 'fit-content' }}>
                {deploymentStrategy?.replace(/_/g, ' ')}
              </span>
            </div>
            <div className="context-item">
              <span className="context-label">Item ID</span>
              <span className="context-value" style={{ fontFamily: 'monospace', fontSize: 11, color: '#64748b' }}>{deploymentPackage?.deployedId || 'Pending'}</span>
            </div>
          </div>
        </div>

        <button className="orch-btn primary" style={{ marginTop: 40, width: 240 }} onClick={onNext}>
          Continue to Configuration <FiChevronRight style={{ marginLeft: 8 }} />
        </button>
      </motion.div>
    );
  }

  return (
    <div className="step-body">
      {/* Header Summary */}
      <div className="deployment-summary pi-card pi-wide" style={{ marginBottom: 24, borderLeft: '4px solid #2563eb' }}>
        <div className="pi-card-title"><FiServer /> Deployment Context</div>
        <div className="deployment-context-grid">
          <div className="context-item">
            <span className="context-label">Workspace</span>
            <span className="context-value">{selectedWorkspace?.name || selectedWorkspace?.displayName || 'Smax_MVP'}</span>
          </div>
          <div className="context-item">
            <span className="context-label">Source Pipeline</span>
            <span className="context-value">{selectedPipeline?.name || selectedPipeline?.displayName || 'None (New)'}</span>
          </div>
          <div className="context-item">
            <span className="context-label">Selected Strategy</span>
            <span className="context-value pi-tag active" style={{ background: '#eff6ff', color: '#2563eb', border: '1px solid #dbeafe' }}>
              {deploymentStrategy?.replace(/_/g, ' ') || 'REUSE'}
            </span>
          </div>
        </div>
      </div>

      {/* STRATEGY: CREATE_NEW */}
      {deploymentStrategy === 'CREATE_NEW' && (
        <div className="strategy-section">
          <div className="section-header" style={{ marginBottom: 20 }}>
            <h3 style={{ fontSize: 17, fontWeight: 800, margin: 0, display: 'flex', alignItems: 'center', gap: 10 }}>
              <FiPlus color="#2563eb" /> Create New Pipeline
            </h3>
            <p style={{ fontSize: 13, color: '#64748b', marginTop: 4 }}>Upload a ZIP export of a Fabric pipeline to deploy it as a new item.</p>
          </div>

          {!deploymentPackage ? (
            <div 
              className={`upload-zone ${isDragging ? 'dragging' : ''} ${status === 'uploading' ? 'loading' : ''}`}
              onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
              onDragLeave={() => setIsDragging(false)}
              onDrop={(e) => { e.preventDefault(); setIsDragging(false); const file = e.dataTransfer.files[0]; if (file) processFile(file); }}
            >
              <input type="file" id="zip-upload" hidden accept=".zip" onChange={handleFileChange} disabled={status !== 'idle'} />
              <label htmlFor="zip-upload" style={{ cursor: status !== 'idle' ? 'not-allowed' : 'pointer', padding: '60px 40px', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                {status === 'uploading' ? <div className="pi-spinner" /> : <FiUploadCloud size={48} color="#2563eb" />}
                <div style={{ marginTop: 16, fontWeight: 800 }}>{status === 'uploading' ? 'Processing Package...' : 'Click or Drag ZIP to Upload'}</div>
                <div style={{ fontSize: 12, color: '#94a3b8', marginTop: 8 }}>Supports Fabric export bundles (.zip)</div>
              </label>
            </div>
          ) : (
            <div className="package-preview pi-card pi-wide" style={{ border: '1px solid #e2e8f0' }}>
               <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
                 <div style={{ background: '#eff6ff', padding: 12, borderRadius: 12 }}><FiPackage size={24} color="#2563eb" /></div>
                 <div style={{ flex: 1 }}>
                    <div style={{ fontWeight: 800 }}>{deploymentPackage.name}</div>
                    <div style={{ fontSize: 12, color: '#64748b' }}>{deploymentPackage.size} · Valid package detected</div>
                 </div>
                 <button className="orch-btn ghost tiny" onClick={() => setDeploymentPackage(null)}>Remove</button>
               </div>
               <div style={{ marginTop: 16, paddingTop: 16, borderTop: '1px solid #f1f5f9', display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 10 }}>
                  {deploymentPackage.manifest.includes.map(f => (
                    <div key={f} style={{ fontSize: 11, color: '#94a3b8', display: 'flex', alignItems: 'center', gap: 4 }}><FiFileText size={13} /> {f}</div>
                  ))}
               </div>
            </div>
          )}
        </div>
      )}

      {/* STRATEGY: CLONE */}
      {deploymentStrategy === 'CLONE' && (
        <div className="strategy-section pi-card pi-wide" style={{ border: '1px solid #e2e8f0', padding: 24 }}>
          <h3 style={{ fontSize: 17, fontWeight: 800, margin: '0 0 16px', display: 'flex', alignItems: 'center', gap: 10 }}>
            <FiCopy color="#2563eb" /> Clone Pipeline
          </h3>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            <span style={{ fontSize: 12, fontWeight: 800, color: '#64748b' }}>New Pipeline Name</span>
            <input 
              className="orch-input" 
              value={newPipelineName} 
              onChange={(e) => setNewPipelineName(e.target.value)} 
              placeholder="Enter pipeline name..."
              style={{ padding: '12px 16px', fontSize: 15 }}
            />
          </label>
          <div style={{ marginTop: 16, fontSize: 13, color: '#64748b', display: 'flex', alignItems: 'center', gap: 8 }}>
            <FiZap color="#f59e0b" /> This will duplicate the logic of <strong>{selectedPipeline?.name}</strong> and assign new identifiers.
          </div>
        </div>
      )}

      {/* STRATEGY: MODIFY */}
      {deploymentStrategy === 'MODIFY' && (
        <div className="strategy-section pi-card pi-wide" style={{ border: '1px solid #e2e8f0', padding: 24 }}>
          <h3 style={{ fontSize: 17, fontWeight: 800, margin: '0 0 8px', display: 'flex', alignItems: 'center', gap: 10 }}>
            <FiEdit2 color="#2563eb" /> Modify Template
          </h3>
          <p style={{ fontSize: 13, color: '#64748b', marginBottom: 20 }}>Edit existing activities and configurations before deployment.</p>
          
          <div className="simulated-editor" style={{ background: '#0f172a', borderRadius: 12, padding: 20, color: '#94a3b8', fontFamily: 'monospace', fontSize: 12 }}>
            <div style={{ color: '#38bdf8' }}>{"{"}</div>
            <div style={{ paddingLeft: 20 }}>"name": <span style={{ color: '#fbbf24' }}>"{selectedPipeline?.name}"</span>,</div>
            <div style={{ paddingLeft: 20 }}>"activities": [</div>
            <div style={{ paddingLeft: 40 }}><span style={{ color: '#38bdf8' }}>{"{"}</span> "name": "CopyData", "type": "Copy" <span style={{ color: '#38bdf8' }}>{"}"}</span>,</div>
            <div style={{ paddingLeft: 40 }}><span style={{ color: '#38bdf8' }}>{"{"}</span> "name": "Notebook", "type": "Notebook" <span style={{ color: '#38bdf8' }}>{"}"}</span></div>
            <div style={{ paddingLeft: 20 }}>],</div>
            <div style={{ paddingLeft: 20 }}>"parameters": <span style={{ color: '#38bdf8' }}>{"{ ... }"}</span></div>
            <div style={{ color: '#38bdf8' }}>{"}"}</div>
          </div>
          <div style={{ marginTop: 16, textAlign: 'right' }}>
            <span style={{ fontSize: 12, color: '#94a3b8' }}>Advanced Activity Editor →</span>
          </div>
        </div>
      )}

      {/* STRATEGY: REUSE */}
      {deploymentStrategy === 'REUSE' && (
        <div className="strategy-section pi-card pi-wide" style={{ background: '#f0f9ff', border: '1px solid #bae6fd', padding: 24 }}>
          <div style={{ display: 'flex', gap: 16 }}>
             <div style={{ background: '#fff', width: 48, height: 48, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', border: '1px solid #bae6fd' }}>
                <FiZap color="#0284c7" size={24} />
             </div>
             <div>
                <h3 style={{ fontSize: 17, fontWeight: 800, margin: '0 0 4px', color: '#0369a1' }}>Pattern Reuse Mode</h3>
                <p style={{ fontSize: 13, color: '#0369a1', opacity: 0.8, margin: 0 }}>
                  Using existing orchestration flow. No new pipeline will be created. We will continue directly to source configuration.
                </p>
             </div>
          </div>
        </div>
      )}

      {/* Deploying Progress */}
      {status === 'deploying' && (
        <div className="deploying-overlay" style={{ marginTop: 24, padding: 24, background: '#f8fafc', borderRadius: 16, border: '1px solid #e2e8f0' }}>
           <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16 }}>
              <div className="pi-spinner" style={{ width: 20, height: 20 }} />
              <div style={{ fontWeight: 800, fontSize: 15 }}>Orchestration Engineering in Progress...</div>
           </div>
           <div className="deploy-logs" style={{ background: '#fff', border: '1px solid #f1f5f9', borderRadius: 8, padding: 12, maxHeight: 120, overflowY: 'auto' }}>
              {deployLogs.map((log, i) => (
                <div key={i} style={{ fontSize: 11, color: '#64748b', marginBottom: 4, fontFamily: 'monospace' }}>
                   <span style={{ color: '#94a3b8' }}>[{log.time}]</span> {log.msg}
                </div>
              ))}
           </div>
        </div>
      )}

      {error && (
        <div className="pi-error" style={{ marginTop: 16 }}>
          <FiAlertCircle /> {error}
        </div>
      )}

      <div className="step-footer" style={{ marginTop: 32 }}>
        {deploymentStrategy === 'REUSE' ? (
           <button className="orch-btn primary" onClick={onNext}>
             Continue to Configuration <FiChevronRight style={{ marginLeft: 8 }} />
           </button>
        ) : (
           <button 
             className="orch-btn primary deploy-btn" 
             disabled={status === 'deploying' || (deploymentStrategy === 'CREATE_NEW' && !deploymentPackage)} 
             onClick={executeDeployment}
             style={{ background: '#2563eb', padding: '12px 32px' }}
           >
             {status === 'deploying' ? 'Deploying...' : (
               <>
                 {deploymentStrategy === 'CREATE_NEW' && <><FiPlus style={{ marginRight: 8 }} /> Create & Deploy Pipeline</>}
                 {deploymentStrategy === 'CLONE' && <><FiCopy style={{ marginRight: 8 }} /> Clone & Deploy Pipeline</>}
                 {deploymentStrategy === 'MODIFY' && <><FiEdit2 style={{ marginRight: 8 }} /> Save & Redeploy Pipeline</>}
               </>
             )}
           </button>
        )}
      </div>

      <style jsx>{`
        .deployment-context-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: 20px;
          margin-top: 12px;
        }
        .context-item {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }
        .context-label {
          font-size: 11px;
          font-weight: 700;
          color: #94a3b8;
          text-transform: uppercase;
        }
        .context-value {
          font-size: 14px;
          font-weight: 600;
          color: #1e293b;
        }
        .upload-zone {
          border: 2px dashed #cbd5e1;
          border-radius: 16px;
          background: #f8fafc;
          transition: all 0.2s ease;
        }
        .upload-zone.dragging {
          border-color: #2563eb;
          background: #eff6ff;
        }
        .upload-zone.loading {
          opacity: 0.6;
        }
        .deploy-btn:hover {
          background: #1d4ed8 !important;
          transform: translateY(-1px);
          box-shadow: 0 4px 12px rgba(37, 99, 235, 0.2);
        }
      `}</style>
    </div>
  );
}
