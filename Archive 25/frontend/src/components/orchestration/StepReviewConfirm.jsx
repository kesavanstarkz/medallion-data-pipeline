import { useEffect } from 'react';
import { FiCheck, FiDatabase, FiFile, FiSettings, FiZap, FiCpu, FiActivity, FiArrowRight, FiCloud } from 'react-icons/fi';
import { motion } from 'framer-motion';
import logo from '../../assets/images/image.png';
import '../PipelineIntelligence.css';

function Tag({ active, children }) {
  return (
    <span
      className={`pi-tag ${active === false ? 'inactive' : 'active'}`}
      style={{ fontSize: 11, padding: '4px 10px', borderRadius: 8 }}
    >
      {children}
    </span>
  );
}

function SummaryChip({ label, value, color }) {
  return (
    <div className="config-chip" style={color ? { borderColor: `${color}30`, background: `${color}08` } : {}}>
      <strong>{label}:</strong> {value || 'Not selected'}
    </div>
  );
}

function IntelCard({ icon, title, children, style }) {
  return (
    <div className="pi-card" style={{ background: '#fff', border: '1px solid rgba(0,0,0,0.06)', borderRadius: 16, padding: '20px 24px', ...style }}>
      <div className="pi-card-title" style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13, fontWeight: 800, marginBottom: 12, color: 'var(--text1)' }}>
        {icon} {title}
      </div>
      {children}
    </div>
  );
}

const PLATFORM_META = {
  FABRIC: { label: 'Microsoft Fabric', color: '#6366f1', icon: <FiZap /> },
  AZURE: { label: 'Azure', color: '#0078d4', icon: <FiCloud /> },
  AWS: { label: 'AWS', color: '#f59e0b', icon: <FiCloud /> },
  DATABRICKS: { label: 'Databricks', color: '#ef4444', icon: <FiCpu /> },
};

export default function StepReviewConfirm({
  selectedClient,
  sourceType,
  folderPath,
  intelligenceData,
  configPersisted,
  requiresRealScan = false,
  onBack,
  onConfirm,
  isOrchestrating,
  fabricMode = 'DISCOVERY',
  pipelineDeployed = false,
  selectedPlatform = '',
  deploymentStrategy = null,
  deploymentPackage = null,
  selectedWorkspace = null,
  selectedPipeline = null,
  masterConfig = null,
}) {
  const isFabricDeploy = fabricMode === 'DEPLOY';
  const isFabricReady = isFabricDeploy && !!configPersisted;
  const platform = PLATFORM_META[selectedPlatform] || null;
  const support = intelligenceData?.ingestion_support || {};
  const capabilities = intelligenceData?.pipeline_capabilities || {};
  const flow = intelligenceData?.interactive_flow || intelligenceData?.loading_flow || [];

  const executionPayload = {
    endpoint: '/orchestrate/run',
    method: 'POST',
    query: {
      source_path: masterConfig?.source_folder || intelligenceData?.reformatted_config?.source_path || folderPath || '',
      source_type: masterConfig?.source_type || (isFabricDeploy ? 'FABRIC' : sourceType),
      folder_path: masterConfig?.source_folder || intelligenceData?.reformatted_config?.folder_path || folderPath || '',
      pipeline_name: masterConfig?.source_object || selectedPipeline?.name || 'New_Fabric_Pipeline',
      dataset_id: masterConfig?.dataset_id || '',
      bronze_target: masterConfig?.target_layer_bronze || '',
      silver_target: masterConfig?.target_layer_silver || '',
      client_name: masterConfig?.client_name || selectedClient || localStorage.getItem('client_name') || 'fabric_client',
      file_format: masterConfig?.file_format || '',
      load_type: masterConfig?.load_type || 'Full',
      platform: selectedPlatform,
      discovery_mode: intelligenceData?.discovery_mode || (isFabricDeploy ? 'FABRIC_RUNTIME' : null),
      deployment_strategy: deploymentStrategy,
      workspace_id: selectedWorkspace?.id,
      pipeline_id: selectedPipeline?.id,
      staging_table: masterConfig?.staging_table || intelligenceData?.staging_table || '',
      package_name: deploymentPackage?.name,
    },
    intelligence: intelligenceData
      ? {
          framework: intelligenceData.framework,
          target: intelligenceData.ingestion_details?.target,
          auth_mode: intelligenceData.auth_mode,
          scan_status: intelligenceData.scan_status,
          is_fallback: intelligenceData.is_fallback,
          source_path: intelligenceData.ingestion_details?.source_path || masterConfig?.source_folder,
        }
      : null,
  };

  // DEBUG LOGS
  useEffect(() => {
    console.log("Master config:", masterConfig);
    console.log("Review payload:", executionPayload);
    
    const requiredFields = ['client_name', 'dataset_id', 'source_path', 'source_type', 'pipeline_name'];
    const missingFields = requiredFields.filter(f => !executionPayload.query[f]);
    if (missingFields.length > 0) {
      console.warn("Missing required fields for execution:", missingFields);
    }
  }, [masterConfig, intelligenceData, folderPath, sourceType]);

  const realScanReady = !!intelligenceData
    && (isFabricDeploy || (!intelligenceData.is_fallback && intelligenceData.auth_mode !== 'none' && intelligenceData.pipeline_capabilities?.scan_mode !== 'mock'))
    && intelligenceData.scan_status !== 'failed'
    && !!configPersisted;

  const isReady = requiresRealScan ? realScanReady : true;
  
  // VALIDATION LOGIC
  const hasRequiredFields = 
    executionPayload.query.client_name && 
    executionPayload.query.dataset_id && 
    executionPayload.query.source_path && 
    executionPayload.query.source_type && 
    executionPayload.query.pipeline_name;

  const deploymentOk = !isFabricDeploy || pipelineDeployed;
  const canExecute = (isReady || isFabricReady) && hasRequiredFields && deploymentOk;

  return (
    <motion.div
      key="step5"
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
      exit={{ opacity: 0, x: -20 }}
      className="orch-step-panel"
    >
      <div className="step-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 30 }}>
        <div>
          <h2 className="step-title" style={{ margin: 0, fontSize: 24, fontWeight: 900 }}>Review & Confirm</h2>
          <p className="step-sub" style={{ margin: '4px 0 0', opacity: 0.8 }}>Validate discovery output and push the run to DEA Agent.</p>
        </div>
        <img src={logo} alt="Agilisium" style={{ height: 32, objectFit: 'contain' }} />
      </div>

      <div className="step-body" style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
        {/* Summary Chips */}
        <div className="dq-config-summary" style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
          {platform && <SummaryChip label="Platform" value={platform.label} color={platform.color} />}
          <SummaryChip label="Client" value={selectedClient} />
          
          {selectedPlatform === 'FABRIC' && (
            <>
              <SummaryChip label="Workspace" value={selectedWorkspace?.name || selectedWorkspace?.displayName} color="#6366f1" />
              <SummaryChip label="Target Pipeline" value={selectedPipeline?.name || selectedPipeline?.displayName} color="#6366f1" />
              <SummaryChip label="Strategy" value={deploymentStrategy?.replace(/_/g, ' ')} color="#6366f1" />
              {deploymentPackage && <SummaryChip label="Package" value={deploymentPackage.name} color="#6366f1" />}
            </>
          )}

          <SummaryChip label="Framework" value={intelligenceData?.framework} />
          <SummaryChip label="Auth" value={intelligenceData?.auth_mode} />
          <SummaryChip label="Scan" value={intelligenceData?.scan_status} />
          <SummaryChip label="Fallback" value={intelligenceData?.is_fallback ? 'Yes' : 'No'} />
          <SummaryChip label="Config Saved" value={configPersisted ? 'Yes' : 'No'} />
          <SummaryChip label="Source" value={sourceType} />
          <SummaryChip label="Endpoint" value={intelligenceData?.staging_table || folderPath} />
        </div>

        {/* Structured Intelligence Cards */}
        {intelligenceData && (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 14 }}>
            <IntelCard icon={<FiDatabase size={15} />} title="Ingestion Support">
              <div className="pi-tag-list" style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                <Tag active={support.file_based}>File-based</Tag>
                <Tag active={support.api}>API</Tag>
                <Tag active={support.database}>Database</Tag>
                <Tag active={support.streaming}>Streaming</Tag>
                <Tag active={support.batch}>Batch</Tag>
              </div>
            </IntelCard>

            <IntelCard icon={<FiFile size={15} />} title="File Types">
              <div className="pi-tag-list" style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {(intelligenceData.file_types || []).map((ft) => <Tag key={ft}>{ft}</Tag>)}
                {(!intelligenceData.file_types || intelligenceData.file_types.length === 0) && <Tag active={false}>None Detected</Tag>}
              </div>
            </IntelCard>

            <IntelCard icon={<FiSettings size={15} />} title="Delimiters">
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6, fontSize: 12 }}>
                <div><strong>Delim:</strong> <code>{intelligenceData.delimiter_config?.column_delimiter || ','}</code></div>
                <div><strong>Quote:</strong> <code>{intelligenceData.delimiter_config?.quote_char || '"'}</code></div>
                <div><strong>Escape:</strong> <code>{intelligenceData.delimiter_config?.escape_char || '\\\\'}</code></div>
                <div><strong>Header:</strong> <code>{intelligenceData.delimiter_config?.header ? 'true' : 'false'}</code></div>
              </div>
            </IntelCard>

            <IntelCard icon={<FiActivity size={15} />} title="DQ Rules">
              <div className="pi-tag-list" style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {Object.entries(intelligenceData.dq_rules || {}).map(([key, value]) => (
                  <Tag key={key} active={!!value}>{key.replace(/_/g, ' ')}</Tag>
                ))}
              </div>
            </IntelCard>

            <IntelCard icon={<FiZap size={15} />} title="Pipeline Capabilities">
              <div className="pi-tag-list" style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {Object.entries(capabilities).filter(([k]) => !['cloud_llm_requested', 'llm_provider', 'scan_mode'].includes(k)).map(([key, value]) => (
                  <Tag key={key} active={!!value}>{key.replace(/_/g, ' ')}</Tag>
                ))}
              </div>
            </IntelCard>

            {(intelligenceData.source_systems || []).length > 0 && (
              <IntelCard icon={<FiCpu size={15} />} title="Source Systems">
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {intelligenceData.source_systems.map((s, i) => (
                    <div key={i} style={{ fontSize: 12, padding: '6px 10px', background: 'var(--surface2)', borderRadius: 8 }}>
                      <strong>{s.name || s.type}</strong>
                      {s.type && <span style={{ marginLeft: 8, opacity: 0.6 }}>{s.type}</span>}
                    </div>
                  ))}
                </div>
              </IntelCard>
            )}
            {selectedPlatform === 'FABRIC' && (
              <IntelCard icon={<FiDatabase size={15} />} title="Staging Layer (NeonDB)">
                <div style={{ fontSize: 12, display: 'flex', flexDirection: 'column', gap: 6 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ opacity: 0.7 }}>Provider:</span>
                    <span style={{ fontWeight: 600 }}>Neon Postgres</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ opacity: 0.7 }}>Status:</span>
                    <span style={{ color: '#10b981', fontWeight: 600 }}>Ready (Preview Staged)</span>
                  </div>
                  <div style={{ fontSize: 10, marginTop: 4, padding: '6px 10px', background: 'rgba(0,0,0,0.03)', borderRadius: 6, fontFamily: 'monospace', wordBreak: 'break-all' }}>
                    ep-dark-morning-aqz49q4z-pooler.c-8.us-east-1.aws.neon.tech
                  </div>
                </div>
              </IntelCard>
            )}
          </div>
        )}

        {/* Interactive Flow */}
        {flow.length > 0 && (
          <IntelCard icon={<FiArrowRight size={15} />} title="Pipeline Flow">
            <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8 }}>
              {flow.map((step, idx) => (
                <span key={`${step}-${idx}`} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ fontSize: 12, fontWeight: 700, padding: '6px 14px', borderRadius: 20, background: 'linear-gradient(135deg, rgba(99,102,241,0.08), rgba(139,92,246,0.08))', color: '#6366f1', border: '1px solid rgba(99,102,241,0.15)' }}>
                    {step}
                  </span>
                  {idx < flow.length - 1 && <FiArrowRight size={12} style={{ color: 'var(--text3)' }} />}
                </span>
              ))}
            </div>
          </IntelCard>
        )}

        {/* GPT Summary */}
        {intelligenceData?.llm_summary && (
          <IntelCard icon={<FiCpu size={15} />} title="GPT Summary">
            <div style={{ fontSize: 13, color: 'var(--text2)', lineHeight: 1.6 }}>{intelligenceData.llm_summary}</div>
          </IntelCard>
        )}

        {/* Execution Payload Preview */}
        <IntelCard icon={<FiSettings size={15} />} title="Execution Payload Preview">
          <pre className="orch-pre" style={{ maxHeight: 180, overflow: 'auto', whiteSpace: 'pre-wrap', fontSize: 11 }}>
            {JSON.stringify(executionPayload, null, 2)}
          </pre>
        </IntelCard>
      </div>

      <div className="step-footer" style={{ justifyContent: 'space-between' }}>
        <button className="orch-btn ghost" onClick={onBack}>Back</button>
        <button
          className="orch-btn primary step-next-btn"
          onClick={() => onConfirm(executionPayload)}
          disabled={isOrchestrating || !canExecute}
          style={{ minWidth: 240, fontWeight: 800 }}
        >
          <FiCheck style={{ marginRight: 8 }} />
          {isOrchestrating ? 'Pushing...' : 'Confirm & Push to DEA Agent'}
        </button>
      </div>
      {(!canExecute && !isFabricDeploy) && (
        <div className="panel-error-alert" style={{ marginTop: 12 }}>
          Please perform a real scan using credentials and save configuration before execution.
        </div>
      )}
    </motion.div>
  );
}
