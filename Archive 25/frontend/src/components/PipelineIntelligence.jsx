import React, { useEffect, useMemo, useRef, useState } from 'react';
import { 
  FiActivity, FiCheck, FiCloud, FiCpu, FiDatabase, 
  FiFile, FiFolder, FiLink, FiSearch, FiSettings, FiZap, 
  FiRefreshCw, FiCopy, FiEdit2, FiPlus, FiAlertCircle, FiUploadCloud,
  FiChevronDown, FiChevronRight, FiGitBranch, FiClock, FiLayers, FiEye, FiSave
} from 'react-icons/fi';
import CloudPortalScanModal from './orchestration/CloudPortalScanModal';
import PipelineFlowCanvas from './orchestration/PipelineFlowCanvas';
import { apiUrl } from '../hooks/useApi';
import 'reactflow/dist/style.css';
import './PipelineIntelligence.css';

const STRATEGIES = [
  { id: 'REUSE', label: 'Reuse Existing', icon: <FiZap />, desc: 'Use orchestration as-is, updating only metadata and parameters.' },
  { id: 'CLONE', label: 'Clone Pipeline', icon: <FiCopy />, desc: 'Duplicate the pipeline within the workspace for this execution.' },
  { id: 'MODIFY', label: 'Modify Template', icon: <FiEdit2 />, desc: 'Patch the pipeline definition with custom activities or flow changes.' },
  { id: 'CREATE_NEW', label: 'Create New', icon: <FiPlus />, desc: 'Deploy a completely new pipeline item from an external package.' },
];

const TARGETS = [
  { id: 'aws', sourceType: 'AWS', label: 'AWS Platform', icon: <FiCloud />, scan: true },
  { id: 'azure', sourceType: 'AZURE', label: 'Azure Platform', icon: <FiCloud />, scan: true },
  { id: 'fabric', sourceType: 'FABRIC', label: 'Microsoft Fabric', icon: <FiZap />, scan: true },
  { id: 's3', sourceType: 'S3', label: 'Amazon S3', icon: <FiDatabase />, scan: true },
  { id: 'adls', sourceType: 'ADLS', label: 'Azure Data Lake', icon: <FiDatabase />, scan: true },
  { id: 'api', sourceType: 'REST_API', label: 'REST API', icon: <FiLink />, scan: false },
  { id: 'local', sourceType: 'LOCAL', label: 'Local Files', icon: <FiFolder />, scan: false },
];

function JsonBlock({ value }) {
  return (
    <pre className="pi-json">
      {JSON.stringify(value || {}, null, 2)}
    </pre>
  );
}

function highlightJsonLine(line, searchTerm) {
  const escaped = line
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  let html = escaped
    .replace(/(".*?")(\s*:)/g, '<span class="pi-json-key">$1</span>$2')
    .replace(/:\s(".*?")/g, ': <span class="pi-json-string">$1</span>')
    .replace(/\b(true|false)\b/g, '<span class="pi-json-boolean">$1</span>')
    .replace(/\b(null)\b/g, '<span class="pi-json-null">$1</span>')
    .replace(/:\s(-?\d+(?:\.\d+)?)/g, ': <span class="pi-json-number">$1</span>');

  if (searchTerm) {
    const safeSearch = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    html = html.replace(new RegExp(safeSearch, 'gi'), '<mark>$&</mark>');
  }
  return html;
}

function SearchableJsonPanel({ title, value, defaultOpen = false }) {
  const [open, setOpen] = useState(defaultOpen);
  const [search, setSearch] = useState('');
  const serialized = useMemo(() => JSON.stringify(value || {}, null, 2), [value]);
  const lines = useMemo(() => serialized.split('\n'), [serialized]);

  return (
    <div className="pi-json-panel">
      <button className="pi-json-panel-header" onClick={() => setOpen((state) => !state)}>
        <span>{open ? <FiChevronDown /> : <FiChevronRight />}</span>
        <span>{title}</span>
      </button>
      {open && (
        <div className="pi-json-panel-body">
          <input
            className="pi-json-search"
            placeholder="Search JSON..."
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
          <pre className="pi-json pi-json-highlight">
            {lines.map((line, index) => (
              <div key={`${title}-${index}`} dangerouslySetInnerHTML={{ __html: highlightJsonLine(line, search) }} />
            ))}
          </pre>
        </div>
      )}
    </div>
  );
}

function getEvidenceValue(value) {
  if (value == null) return null;
  if (Array.isArray(value)) return value.map(getEvidenceValue).filter((item) => item != null);
  if (typeof value === 'object') {
    if (Object.prototype.hasOwnProperty.call(value, 'value')) return value.value;
    return null;
  }
  return value;
}

function renderValue(value, fallback = 'Not present') {
  const resolved = getEvidenceValue(value);
  if (resolved == null || resolved === '') return fallback;
  if (Array.isArray(resolved)) {
    const printable = resolved.filter((item) => item != null && item !== '').map((item) => (
      typeof item === 'object' ? JSON.stringify(item) : String(item)
    ));
    return printable.length ? printable.join(', ') : fallback;
  }
  if (typeof resolved === 'object') return JSON.stringify(resolved);
  return String(resolved);
}

function renderEvidenceMeta(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || !Object.prototype.hasOwnProperty.call(value, 'status')) {
    return null;
  }
  return (
    <div className="pi-evidence-meta">
      <span>{value.status}</span>
      {typeof value.confidence === 'number' && <span>{Math.round(value.confidence * 100)}%</span>}
      {value.evidence && <span>{value.evidence}</span>}
    </div>
  );
}

function renderListItems(items, formatter) {
  if (!Array.isArray(items) || !items.length) return null;
  return items.map((item, index) => <div key={index}>{formatter(item, index)}</div>);
}

function prettyLabel(value) {
  return String(value || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function compactValue(value) {
  if (value == null || value === '') return 'Not captured';
  if (typeof value === 'boolean') return value ? 'Yes' : 'No';
  if (Array.isArray(value)) return value.length ? value.join(', ') : 'Not captured';
  return String(value);
}

function Tag({ active, children }) {
  return <span className={`pi-tag ${active === false ? 'inactive' : 'active'}`}>{children}</span>;
}

function hasApiScanDetails(apiSources = []) {
  return (apiSources || []).some((source) => {
    const endpoints = Array.isArray(source.endpoints)
      ? source.endpoints
      : String(source.endpoints || '').split(',').map((item) => item.trim()).filter(Boolean);
    return !!source.base_url && endpoints.length > 0;
  });
}

export default function PipelineIntelligence({
  clientName,
  initialData,
  clientSourceTypes = [],
  currentSourceType = '',
  apiSources = [],
  fabricDiscoveryData = null,
  fabricMode = 'DISCOVERY',
  selectedPlatform = '',
  selectedWorkspace = null,
  setSelectedWorkspace = () => {},
  selectedPipeline = null,
  setSelectedPipeline = () => {},
  onScanComplete,
  onConfirm
}) {
  const [data, setData] = useState(initialData || null);
  const [loading, setLoading] = useState(false);
  const [scanInProgress, setScanInProgress] = useState(false);
  const [analyzing, setAnalyzing] = useState(false);
  const [error, setError] = useState(null);
  const [target, setTarget] = useState(initialData?.ingestion_details?.target || 'aws');
  const [useCloudLlm, setUseCloudLlm] = useState(true);
  const [showCloudScanModal, setShowCloudScanModal] = useState(false);
  const [scanResults, setScanResults] = useState(null);
  const [deploymentStrategy, setDeploymentStrategy] = useState(null);
  const [bundleAnalysis, setBundleAnalysis] = useState(null);
  const [runtimeAnalysis, setRuntimeAnalysis] = useState(null);
  const [runtimeLoading, setRuntimeLoading] = useState(false);
  const [runtimeError, setRuntimeError] = useState(null);
  const [runtimePermissionDetail, setRuntimePermissionDetail] = useState(null);
  const [bundleUploadStatus, setBundleUploadStatus] = useState('idle');
  const [bundleUploadProgress, setBundleUploadProgress] = useState(0);
  const [bundleUploadError, setBundleUploadError] = useState(null);
  const [isBundleDragging, setIsBundleDragging] = useState(false);
  const [fabricAccessToken, setFabricAccessToken] = useState(initialData?.__fabric_access_token || null);
  const [fabricTokenValidation, setFabricTokenValidation] = useState(initialData?.__fabric_token_validation || null);
  const [runtimePreview, setRuntimePreview] = useState(null);
  const [runtimePreviewLoading, setRuntimePreviewLoading] = useState(false);
  const [runtimePreviewError, setRuntimePreviewError] = useState(null);
  const [runtimeActionMessage, setRuntimeActionMessage] = useState(null);
  const [runtimeSaveLoading, setRuntimeSaveLoading] = useState(false);
  const selectedWorkspaceRef = useRef(selectedWorkspace);
  const selectedPipelineRef = useRef(selectedPipeline);
  const runtimeCaptureRequestRef = useRef(0);
  const runtimePreviewRequestRef = useRef(0);

  const configuredSourceTypes = useMemo(() => {
    const values = (clientSourceTypes || []).map((item) => String(item || '').toUpperCase()).filter(Boolean);
    const current = String(currentSourceType || '').toUpperCase();
    const mapped = current === 'API' ? 'REST_API' : current;
    if (mapped) values.push(mapped);
    if (selectedPlatform === 'FABRIC' && !values.includes('FABRIC')) values.push('FABRIC');
    return [...new Set(values)];
  }, [clientSourceTypes, currentSourceType, selectedPlatform]);

  const allowedTargets = useMemo(() => TARGETS.filter((item) => configuredSourceTypes.includes(item.sourceType)), [configuredSourceTypes]);
  const selectedTarget = allowedTargets.find((item) => item.id === target);
  const apiDetailsAvailable = hasApiScanDetails(apiSources);
  const selectedRequiresScan = selectedTarget?.sourceType ? (['AWS', 'AZURE', 'FABRIC', 'S3', 'ADLS'].includes(selectedTarget.sourceType) || (selectedTarget.sourceType === 'REST_API' && apiDetailsAvailable)) : false;

  useEffect(() => {
    if (initialData) {
      setData(initialData);
      if (initialData.__fabric_access_token) setFabricAccessToken(initialData.__fabric_access_token);
      if (initialData.__fabric_token_validation) setFabricTokenValidation(initialData.__fabric_token_validation);
    } else {
      // Try restoring from sessionStorage if no initialData
      const storedToken = sessionStorage.getItem('fabric_access_token');
      if (storedToken && !fabricAccessToken) {
        setFabricAccessToken(storedToken);
      }
    }
  }, [initialData]);

  // Sync local state to prop changes (e.g. from Stepper restoration)
  useEffect(() => {
    if (fabricAccessToken && !sessionStorage.getItem('fabric_access_token')) {
      sessionStorage.setItem('fabric_access_token', fabricAccessToken);
    }
  }, [fabricAccessToken]);

  useEffect(() => {
    selectedWorkspaceRef.current = selectedWorkspace;
    console.log('Current selected workspace:', selectedWorkspace?.id || null);
  }, [selectedWorkspace]);

  useEffect(() => {
    selectedPipelineRef.current = selectedPipeline;
  }, [selectedPipeline]);

  useEffect(() => {
    if (allowedTargets.length > 0 && !allowedTargets.some(t => t.id === target)) {
      setTarget(allowedTargets[0].id);
    }
  }, [allowedTargets, target]);

  useEffect(() => {
    runtimeCaptureRequestRef.current += 1;
    runtimePreviewRequestRef.current += 1;
    setRuntimeAnalysis(null);
    setRuntimePreview(null);
    setRuntimeError(null);
    setRuntimePreviewError(null);
    setRuntimePermissionDetail(null);
    setRuntimeActionMessage(null);
    setRuntimeLoading(false);
    setRuntimePreviewLoading(false);
    setRuntimeSaveLoading(false);
  }, [selectedWorkspace?.id]);

  useEffect(() => {
    runtimeCaptureRequestRef.current += 1;
    runtimePreviewRequestRef.current += 1;
    setRuntimeAnalysis(null);
    setRuntimePreview(null);
    setRuntimeError(null);
    setRuntimePreviewError(null);
    setRuntimePermissionDetail(null);
    setRuntimeActionMessage(null);
    setRuntimeLoading(false);
    setRuntimePreviewLoading(false);
    setRuntimeSaveLoading(false);
  }, [selectedPipeline?.id]);

  const resolveConnectionWorkspaceId = (connection = {}) => (
    connection.workspaceId
    || connection.workspace_id
    || connection.workspace?.id
    || connection.artifact?.workspaceId
    || connection.artifact?.workspace_id
    || null
  );

  const validateRuntimeWorkspaceSelection = (connection = {}) => {
    const activeWorkspaceId = selectedWorkspaceRef.current?.id || null;
    if (!activeWorkspaceId) {
      throw new Error('Select a Fabric workspace before using the runtime source.');
    }

    const artifactWorkspaceId = resolveConnectionWorkspaceId(connection);

    if (!connection.artifact_id) {
      console.warn('Runtime source is missing an artifact ID. Some actions may be limited.');
      // We no longer throw here to prevent UI "crashes", but we will disable relevant buttons in the UI.
    }

    return {
      activeWorkspaceId,
      artifactWorkspaceId,
    };
  };

  const buildRuntimePreviewPayload = (connection = {}, schemaDiscovery = {}) => {
    const { activeWorkspaceId, artifactWorkspaceId } = validateRuntimeWorkspaceSelection(connection);
    const payload = {
      source_connection: {
        ...connection,
        workspace_id: artifactWorkspaceId || activeWorkspaceId,
      },
      schema_discovery: schemaDiscovery,
      workspaceId: artifactWorkspaceId || activeWorkspaceId,
      artifactId: connection.artifact_id,
      rootFolder: connection.root_folder,
      folderPath: connection.folder_path,
      fileName: connection.file_name,
      format: connection.format,
      header: connection.header_enabled,
      delimiter: connection.delimiter,
    };

    console.log('Runtime preview validation:', {
      activeWorkspaceId,
      artifactWorkspaceId,
      artifactId: connection.artifact_id,
      selectedPipelineId: selectedPipelineRef.current?.id || null,
    });
    console.log('Payload workspace:', payload.workspaceId);

    return payload;
  };

  const handleAnalyzePipeline = async (workspace, pipeline) => {
    if (scanInProgress || analyzing) return;
    setAnalyzing(true);
    setError(null);
    setSelectedWorkspace(workspace);
    setSelectedPipeline(pipeline);

    try {
      const headers = { 'Content-Type': 'application/json' };
      if (fabricAccessToken) headers.Authorization = `Bearer ${fabricAccessToken}`;
      const response = await fetch(apiUrl('/discovery/analyze'), {
        method: 'POST',
        headers,
        body: JSON.stringify({ 
          client_name: clientName,
          platform: 'FABRIC',
          source_type: 'FABRIC',
          payload: { workspace_id: workspace.id, pipeline_id: pipeline.id },
          use_cloud_llm: useCloudLlm
        }),
      });
      if (!response.ok) throw new Error('Analysis failed');
      const result = await response.json();
      const finalResult = { ...result, scan_status: 'success', scan_completed: true };
      setData(finalResult);
      onScanComplete?.(finalResult);
    } catch (e) {
      setError("Pipeline analysis failed. Could not extract intelligence metadata.");
    } finally {
      setAnalyzing(false);
    }
  };

  const handleManualApiScan = async () => {
    if (scanInProgress) return;
    setScanInProgress(true);
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(apiUrl('/discovery/api-scan'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ client_name: clientName }),
      });
      if (!response.ok) throw new Error('API scan failed');
      const result = await response.json();
      setData(result);
      onScanComplete?.(result);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
      setScanInProgress(false);
    }
  };

  const flow = data?.interactive_flow || data?.loading_flow || [];
  const support = data?.ingestion_support || {};
  const delimiter = data?.delimiter_config || {};
  const capabilities = data?.pipeline_capabilities || {};
  const graph = bundleAnalysis?.activity_dependency_graph || { nodes: [], edges: [] };
  const runtimeGraph = runtimeAnalysis?.execution_graph || { nodes: [], edges: [] };
  const graphNodes = useMemo(() => (
    (graph.nodes || []).map((node, index) => ({
      id: node.id,
      data: { label: `${node.label} (${node.type || 'Activity'})` },
      position: { x: 80 + (index * 220), y: index % 2 === 0 ? 90 : 210 },
      style: {
        borderRadius: 12,
        padding: 10,
        border: '1px solid #cbd5e1',
        background: '#fff',
        fontSize: 12,
        fontWeight: 700,
        width: 180
      }
    }))
  ), [graph.nodes]);
  const graphEdges = useMemo(() => (
    (graph.edges || []).map((edge) => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      animated: false,
      label: (edge.condition || []).join(', ') || undefined,
      style: { stroke: '#64748b' },
      labelStyle: { fontSize: 10, fill: '#64748b' }
    }))
  ), [graph.edges]);
  const runtimeGraphNodes = useMemo(() => (
    (runtimeGraph.nodes || []).map((node, index) => ({
      id: node.id,
      data: { label: `${node.label} [${node.status || 'Pending'}${node.duration_ms ? ` • ${node.duration_ms} ms` : ''}]` },
      position: { x: 80 + (index * 220), y: index % 2 === 0 ? 90 : 210 },
      style: {
        borderRadius: 12,
        padding: 10,
        border: '1px solid #cbd5e1',
        background: '#fff',
        fontSize: 12,
        fontWeight: 700,
        width: 220
      }
    }))
  ), [runtimeGraph.nodes]);
  const runtimeGraphEdges = useMemo(() => (
    (runtimeGraph.edges || []).map((edge) => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      animated: false,
      label: (edge.condition || []).join(', ') || undefined,
      style: { stroke: '#64748b' },
      labelStyle: { fontSize: 10, fill: '#64748b' }
    }))
  ), [runtimeGraph.edges]);
  const runtimeDiscovery = runtimeAnalysis?.runtime_source_discovery || {};
  const runtimeSourceConnection = runtimeDiscovery.source_connection || {};
  const runtimeTargetConnection = runtimeDiscovery.target_connection || {};
  const runtimeSchemaDiscovery = runtimeDiscovery.schema_discovery || {};
  const runtimeStatistics = runtimeDiscovery.runtime_statistics || {};

  const handleBundleFile = async (file) => {
    if (!file) return;
    if (!file.name.toLowerCase().endsWith('.zip')) {
      setBundleUploadStatus('error');
      setBundleUploadError('Only Microsoft Fabric exported ZIP bundles are supported.');
      return;
    }

    setBundleUploadStatus('uploading');
    setBundleUploadProgress(0);
    setBundleUploadError(null);
    setBundleAnalysis(null);

    const form = new FormData();
    form.append('client_name', clientName);
    if (selectedWorkspace?.id) form.append('workspace_id', selectedWorkspace.id);
    if (selectedPipeline?.id) form.append('pipeline_id', selectedPipeline.id);
    form.append('use_cloud_llm', String(useCloudLlm));
    form.append('existing_analysis_json', JSON.stringify(data || {}));
    form.append('file', file, file.name);

    await new Promise((resolve) => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', apiUrl('/discovery/fabric-bundle-analysis'));
      if (fabricAccessToken) {
        xhr.setRequestHeader('Authorization', `Bearer ${fabricAccessToken}`);
      }
      xhr.upload.onprogress = (event) => {
        if (event.lengthComputable) {
          setBundleUploadProgress(Math.round((event.loaded / event.total) * 100));
        }
      };
      xhr.onload = () => {
        try {
          const payload = JSON.parse(xhr.responseText || '{}');
          if (xhr.status >= 200 && xhr.status < 300) {
            setBundleAnalysis(payload);
            setBundleUploadStatus('success');
            setBundleUploadProgress(100);
          } else {
            setBundleUploadStatus('error');
            setBundleUploadError(payload.detail || 'Bundle analysis failed.');
          }
        } catch {
          setBundleUploadStatus('error');
          setBundleUploadError('Bundle analysis returned an invalid response.');
        }
        resolve();
      };
      xhr.onerror = () => {
        setBundleUploadStatus('error');
        setBundleUploadError('Bundle upload failed.');
        resolve();
      };
      xhr.send(form);
    });
  };

  const handleRunRuntimeIntelligence = async () => {
    const activeWorkspaceId = selectedWorkspaceRef.current?.id;
    const activePipelineId = selectedPipelineRef.current?.id;
    if (!activeWorkspaceId || !activePipelineId) {
      setRuntimeError('Select and analyze a Fabric pipeline before running runtime capture.');
      return;
    }
    if (!fabricAccessToken) {
      setRuntimeError('Fabric runtime capture requires an active Microsoft Fabric SSO token from the current session.');
      return;
    }

    const requestId = ++runtimeCaptureRequestRef.current;
    setRuntimeLoading(true);
    setRuntimeError(null);
    setRuntimePermissionDetail(null);
    setRuntimeAnalysis(null);
    setRuntimePreview(null);
    setRuntimePreviewError(null);
    setRuntimeActionMessage(null);
    try {
      const response = await fetch(apiUrl('/discovery/fabric-runtime-intelligence'), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${fabricAccessToken}`,
        },
        body: JSON.stringify({
          client_name: clientName,
          workspace_id: activeWorkspaceId,
          pipeline_id: activePipelineId,
          existing_analysis: data || {},
        }),
      });
      const payload = await response.json();
      if (requestId !== runtimeCaptureRequestRef.current) return;
      if (!response.ok) {
        if (payload?.detail && typeof payload.detail === 'object') {
          setRuntimePermissionDetail(payload.detail);
          throw new Error(payload.detail.message || 'Runtime intelligence capture failed.');
        }
        throw new Error(payload.detail || payload.message || 'Runtime intelligence capture failed.');
      }
      if (selectedWorkspaceRef.current?.id !== activeWorkspaceId || selectedPipelineRef.current?.id !== activePipelineId) {
        console.log('Ignoring stale runtime capture response', {
          requestWorkspaceId: activeWorkspaceId,
          currentWorkspaceId: selectedWorkspaceRef.current?.id || null,
          requestPipelineId: activePipelineId,
          currentPipelineId: selectedPipelineRef.current?.id || null,
        });
        return;
      }
      setRuntimeAnalysis(payload);
      // Merge runtime-generated config into the main analysis data
      if (payload.reformatted_config) {
        setData((prev) => ({
          ...prev,
          ...payload, // Include all runtime details
          reformatted_config: {
            ...(prev?.reformatted_config || {}),
            ...payload.reformatted_config
          }
        }));
      }
    } catch (runtimeCaptureError) {
      if (requestId !== runtimeCaptureRequestRef.current) return;
      setRuntimeError(runtimeCaptureError?.message || 'Runtime intelligence capture failed.');
    } finally {
      if (requestId === runtimeCaptureRequestRef.current) {
        setRuntimeLoading(false);
      }
    }
  };

  const handlePreviewRuntimeSource = async () => {
    if (!runtimeSourceConnection || Object.keys(runtimeSourceConnection).length === 0) return;
    const requestId = ++runtimePreviewRequestRef.current;
    setRuntimePreviewLoading(true);
    setRuntimePreviewError(null);
    try {
      const payloadBody = buildRuntimePreviewPayload(runtimeSourceConnection, runtimeSchemaDiscovery);
      const response = await fetch(apiUrl('/discovery/fabric-runtime-source-preview'), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(fabricAccessToken ? { Authorization: `Bearer ${fabricAccessToken}` } : {}),
        },
        body: JSON.stringify(payloadBody),
      });
      const payload = await response.json();
      if (requestId !== runtimePreviewRequestRef.current) return;
      if (!response.ok) {
        const detail = payload.detail;
        throw new Error(typeof detail === 'object' ? (detail.message || JSON.stringify(detail)) : (detail || 'Failed to preview runtime source.'));
      }
      if (selectedWorkspaceRef.current?.id !== payloadBody.workspaceId) {
        console.log('Ignoring stale runtime preview response', {
          requestWorkspaceId: payloadBody.workspaceId,
          currentWorkspaceId: selectedWorkspaceRef.current?.id || null,
        });
        return;
      }
      setRuntimePreview(payload);
    } catch (previewError) {
      if (requestId !== runtimePreviewRequestRef.current) return;
      setRuntimePreview(null);
      setRuntimePreviewError(previewError?.message || 'Failed to preview runtime source.');
    } finally {
      if (requestId === runtimePreviewRequestRef.current) {
        setRuntimePreviewLoading(false);
      }
    }
  };

  const handleUseRuntimeSource = () => {
    if (!runtimeDiscovery || !Object.keys(runtimeDiscovery).length) return;
    const { activeWorkspaceId, artifactWorkspaceId } = validateRuntimeWorkspaceSelection(runtimeSourceConnection);
    const sourcePath = runtimeSourceConnection.full_path || runtimeSourceConnection.folder_path || runtimeSourceConnection.file_name || '';
    const sourceFormat = runtimeSourceConnection.format ? [runtimeSourceConnection.format] : (data?.file_types || []);
    const merged = {
      ...(data || {}),
      runtime_source_discovery: {
        ...runtimeDiscovery,
        source_connection: {
          ...runtimeSourceConnection,
          workspace_id: artifactWorkspaceId || activeWorkspaceId,
        },
      },
      ingestion_details: {
        ...(data?.ingestion_details || {}),
        source_type: 'FABRIC',
        target: 'fabric',
      },
      reformatted_config: {
        ...(data?.reformatted_config || {}),
        source_type: 'FABRIC',
        source_path: sourcePath,
        pipeline_name: selectedPipeline?.name || data?.reformatted_config?.pipeline_name,
        runtime_ingestion_config: runtimeDiscovery.ingestion_config,
      },
      file_types: sourceFormat,
    };
    setData(merged);
    onScanComplete?.(merged);
    setRuntimeActionMessage('Runtime source was applied as the reusable source configuration for the next orchestration steps.');
  };

  const handleSaveRuntimeSource = async () => {
    if (!runtimeDiscovery || !Object.keys(runtimeDiscovery).length) return;
    setRuntimeSaveLoading(true);
    setRuntimeActionMessage(null);
    try {
      const { activeWorkspaceId, artifactWorkspaceId } = validateRuntimeWorkspaceSelection(runtimeSourceConnection);
      const response = await fetch(apiUrl('/discovery/fabric-runtime-source-save'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          client_name: clientName,
          workspace_id: artifactWorkspaceId || activeWorkspaceId,
          pipeline_id: selectedPipelineRef.current?.id || null,
          runtime_source_discovery: {
            ...runtimeDiscovery,
            source_connection: {
              ...runtimeSourceConnection,
              workspace_id: artifactWorkspaceId || activeWorkspaceId,
            },
          },
        }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || 'Failed to save runtime source.');
      setRuntimeActionMessage(`Saved reusable source ${payload.source_object || payload.dataset_id} to the registry.`);
    } catch (saveError) {
      setRuntimeActionMessage(saveError?.message || 'Failed to save runtime source.');
    } finally {
      setRuntimeSaveLoading(false);
    }
  };

  const handleGenerateIngestionConfig = () => {
    if (!runtimeDiscovery?.ingestion_config) return;
    setRuntimeActionMessage('Reusable ingestion config was generated from runtime-discovered metadata and is available below.');
  };

  const handleBuildPipelineFromSource = () => {
    handleUseRuntimeSource();
    setRuntimeActionMessage('Runtime source was promoted into the orchestration flow. Continue to the next step to build the pipeline from this source.');
  };

  return (
    <div className="pipeline-intelligence-container">
      <div className="pi-header">
        <h2>Pipeline Intelligence</h2>
        <p className="step-sub">Discover pipeline architecture, ingestion support, configuration, and DQ signals.</p>
      </div>

      <div className="pi-target-grid">
        {allowedTargets.map((item) => (
          <button
            key={item.id}
            className={`pi-target-card ${target === item.id ? 'selected' : ''}`}
            onClick={() => { setTarget(item.id); setData(null); setScanResults(null); }}
            disabled={loading || scanInProgress || analyzing}
          >
            <span className="pi-target-icon">{item.icon}</span>
            <span>{item.label}</span>
          </button>
        ))}
      </div>

      <div className="pi-scan-trigger">
        {selectedTarget?.sourceType !== 'LOCAL' && (
          <label className="pi-checkbox-row" style={{ marginBottom: 20 }}>
            <input type="checkbox" checked={useCloudLlm} onChange={(e) => setUseCloudLlm(e.target.checked)} disabled={loading || scanInProgress || analyzing} />
            <span>Use GPT API to extract ingestion, source, and DQ rules</span>
          </label>
        )}

        {selectedTarget?.sourceType !== 'REST_API' && (
          <button
            className="pi-btn-confirm"
            onClick={() => setShowCloudScanModal(true)}
            disabled={loading || scanInProgress || analyzing || !selectedRequiresScan}
          >
            <FiSearch /> Scan Framework
          </button>
        )}
        
        {selectedTarget?.sourceType === 'REST_API' && apiDetailsAvailable && (
          <button className="pi-btn-confirm" onClick={handleManualApiScan} disabled={loading || scanInProgress || analyzing}>
            <FiSearch /> Scan REST API
          </button>
        )}
      </div>

      {/* FABRIC ASSET EXPLORER */}
      {selectedPlatform === 'FABRIC' && scanResults && !data && !loading && !analyzing && (
        <div className="fabric-explorer-section" style={{ marginTop: 30, padding: 20, background: '#f8fafc', borderRadius: 16, border: '1px solid #e2e8f0' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
            <h3 style={{ fontSize: 18, fontWeight: 900, display: 'flex', alignItems: 'center', gap: 10, margin: 0 }}>
              <FiFolder color="#2563eb" /> Discovered Fabric Workspaces
            </h3>
            <span style={{ fontSize: 12, color: '#64748b', fontWeight: 600 }}>{scanResults.length} Workspaces Found</span>
          </div>

          {scanResults.length > 0 ? (
            <div className="workspace-list" style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
              {scanResults.map(ws => (
                <div key={ws.id} className="workspace-card pi-card pi-wide" style={{ background: '#fff', border: '1px solid #e2e8f0', padding: 0, overflow: 'hidden', borderRadius: 12, boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.05)' }}>
                  <div style={{ background: '#fff', padding: '16px 20px', borderBottom: '1px solid #f1f5f9', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                      <div style={{ padding: 8, background: '#eff6ff', borderRadius: 8 }}><FiDatabase color="#2563eb" size={18} /></div>
                      <div>
                        <div style={{ fontWeight: 800, fontSize: 15, color: '#1e293b' }}>{ws.name || ws.displayName}</div>
                        <div style={{ fontSize: 10, color: '#94a3b8', fontFamily: 'monospace' }}>ID: {ws.id}</div>
                      </div>
                    </div>
                    <div className="pipeline-count-tag" style={{ padding: '4px 10px', background: '#f1f5f9', borderRadius: 20, fontSize: 11, fontWeight: 700, color: '#64748b' }}>
                      {(ws.pipelines || ws.data_pipelines || []).length} Pipelines
                    </div>
                  </div>
                  <div className="pipeline-list" style={{ padding: 15, display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: 10, background: '#fafafa' }}>
                    {(ws.pipelines || ws.data_pipelines || []).map(pl => (
                      <div key={pl.id} style={{ padding: '12px 16px', background: '#fff', border: '1px solid #e2e8f0', borderRadius: 10, display: 'flex', justifyContent: 'space-between', alignItems: 'center', transition: 'all 0.2s ease' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                          <FiActivity color="#6366f1" size={16} />
                          <div style={{ fontSize: 13, fontWeight: 700, color: '#334155' }}>{pl.name || pl.displayName}</div>
                        </div>
                        <button 
                          className="orch-btn primary tiny" 
                          onClick={() => handleAnalyzePipeline(ws, pl)}
                          style={{ fontSize: 11, padding: '6px 14px', borderRadius: 8, background: '#2563eb', color: '#fff', border: 'none', cursor: 'pointer', fontWeight: 600 }}
                        >
                          Analyze Pipeline
                        </button>
                      </div>
                    ))}
                    {!(ws.pipelines || ws.data_pipelines || []).length && (
                      <div style={{ gridColumn: '1/-1', textAlign: 'center', padding: '20px', color: '#94a3b8', fontSize: 13, fontStyle: 'italic' }}>
                        No pipelines discovered in this workspace.
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div style={{ textAlign: 'center', padding: '40px 20px', background: '#fff', borderRadius: 12, border: '1px dashed #cbd5e1' }}>
              <FiSearch size={40} color="#94a3b8" style={{ marginBottom: 16 }} />
              <div style={{ fontWeight: 700, fontSize: 16, color: '#475569' }}>No Workspaces Discovered</div>
              <p style={{ color: '#94a3b8', fontSize: 14, margin: '8px 0 0' }}>The scan completed but no accessible Fabric workspaces were found.</p>
            </div>
          )}
        </div>
      )}

      {analyzing && (
        <div className="pi-loading" style={{ marginTop: 30 }}>
          <div className="pi-spinner" />
          <p>Extracting deep intelligence from <strong>{selectedPipeline?.name}</strong>...</p>
        </div>
      )}

      {loading && (
        <div className="pi-loading">
          <div className="pi-spinner" />
          <p>Scanning live environment...</p>
        </div>
      )}

      {error && <div className="pi-error"><FiAlertCircle /> {error}</div>}

      {/* INTELLIGENCE & STRATEGY */}
      {data && !analyzing && !loading && (
        <>
          {selectedPlatform === 'FABRIC' && (
            <div className="pi-card pi-wide" style={{ border: '1px solid #3b82f6', background: 'rgba(59, 130, 246, 0.05)', marginTop: 24 }}>
              <div className="pi-card-title" style={{ color: '#2563eb' }}><FiSettings /> PIPELINE REUSE STRATEGY</div>
              <div className="pi-strategy-grid">
                {STRATEGIES.map((s) => (
                  <button key={s.id} className={`pi-strategy-card ${deploymentStrategy === s.id ? 'selected' : ''}`} onClick={() => setDeploymentStrategy(s.id)}>
                    <div className="pi-strategy-icon">{s.icon}</div>
                    <div className="pi-strategy-info">
                      <div className="pi-strategy-label">{s.label}</div>
                      <div className="pi-strategy-desc">{s.desc}</div>
                    </div>
                  </button>
                ))}
              </div>
            </div>
          )}

          <div className="pi-grid">
            <div className="pi-card"><div className="pi-card-title"><FiCpu /> Detected Framework</div><div className="pi-card-content pi-framework">{data.framework || 'Unknown'}</div></div>
            <div className="pi-card"><div className="pi-card-title"><FiDatabase /> Ingestion Support</div><div className="pi-tag-list"><Tag active={support.file_based}>File-based</Tag><Tag active={support.api}>API</Tag><Tag active={support.database}>Database</Tag></div></div>
            <div className="pi-card"><div className="pi-card-title"><FiSettings /> Delimiters</div><div className="pi-card-content">{delimiter.column_delimiter || ','} | {delimiter.quote_char || '"'}</div></div>
            <div className="pi-card"><div className="pi-card-title"><FiZap /> Capabilities</div><div className="pi-tag-list">{Object.entries(capabilities).map(([k, v]) => <Tag key={k} active={!!v}>{k}</Tag>)}</div></div>
            <div className="pi-card pi-wide"><div className="pi-card-title"><FiActivity /> Interactive Flow</div><div className="pi-flow-viz">{flow.map((n, i) => <span key={i}>{n.label} {i < flow.length - 1 && '→'} </span>)}</div></div>
            <div className="pi-card"><div className="pi-card-title">Cloud Scan</div><JsonBlock value={data.raw_cloud_scan || {}} /></div>
            <div className="pi-card"><div className="pi-card-title">Reformatted Config</div><JsonBlock value={data.reformatted_config || {}} /></div>
          </div>

          {selectedPlatform === 'FABRIC' && (
            <div className="pi-card pi-wide" style={{ marginTop: 16 }}>
              <div className="pi-card-title"><FiActivity /> Runtime Execution + Live Capture</div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 16, flexWrap: 'wrap' }}>
                <div className="pi-card-content">
                  Execute the selected Fabric pipeline, poll live activity runs, capture actual runtime values, and append runtime intelligence below.
                </div>
                <button className="pi-btn-confirm" onClick={handleRunRuntimeIntelligence} disabled={runtimeLoading || !selectedWorkspace?.id || !selectedPipeline?.id}>
                  <FiActivity /> {runtimeLoading ? 'Running & Capturing...' : 'Run & Capture Runtime Intelligence'}
                </button>
              </div>
              {fabricTokenValidation && (
                <div className="pi-alert warning" style={{ marginTop: 12 }}>
                  <div><strong>Current scopes:</strong> {(fabricTokenValidation.scp || []).join(', ') || 'None'}</div>
                  <div><strong>Required scopes:</strong> {(fabricTokenValidation.required_scopes || []).join(', ') || 'None'}</div>
                  <div><strong>Audience:</strong> {fabricTokenValidation.aud || 'Unavailable'}</div>
                  {!!(fabricTokenValidation.missing_scopes || []).length && (
                    <div><strong>Missing scopes:</strong> {(fabricTokenValidation.missing_scopes || []).join(', ')}</div>
                  )}
                </div>
              )}
              {runtimeError && <div className="pi-error" style={{ marginTop: 12 }}><FiAlertCircle /> {runtimeError}</div>}
              {runtimePermissionDetail && (
                <div className="pi-alert error" style={{ marginTop: 12 }}>
                  <div><strong>{runtimePermissionDetail.message}</strong></div>
                  <div><strong>Current scopes:</strong> {(runtimePermissionDetail.current_scopes || []).join(', ') || 'None'}</div>
                  <div><strong>Required scopes:</strong> {(runtimePermissionDetail.required_scopes || []).join(', ') || 'None'}</div>
                  <div><strong>Missing scopes:</strong> {(runtimePermissionDetail.missing_scopes || []).join(', ') || 'None'}</div>
                  <div><strong>Audience:</strong> {runtimePermissionDetail.token_audience || 'Unavailable'}</div>
                  <div><strong>Tenant:</strong> {runtimePermissionDetail.tenant || 'Unavailable'}</div>
                  {runtimePermissionDetail.admin_instructions && (
                    <div style={{ marginTop: 8, whiteSpace: 'pre-wrap' }}>{runtimePermissionDetail.admin_instructions}</div>
                  )}
                </div>
              )}
            </div>
          )}

          {selectedPlatform === 'FABRIC' && runtimeAnalysis && (
            <div className="pi-card pi-wide" style={{ marginTop: 16 }}>
              <div className="pi-card-title"><FiLayers /> Runtime Intelligence Capture</div>
              <div className="pi-grid">
                <div className="pi-card">
                  <div className="pi-card-title">Runtime Execution Summary</div>
                  <div className="pi-kv-grid">
                    <div><strong>Run ID:</strong> {runtimeAnalysis.pipeline_run_id}</div>
                    <div><strong>Status:</strong> {runtimeAnalysis.execution_status}</div>
                    <div><strong>Activities:</strong> {runtimeAnalysis.runtime_metrics?.total_activities ?? 0}</div>
                    <div><strong>Retries:</strong> {runtimeAnalysis.runtime_metrics?.retry_count ?? 0}</div>
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Runtime Statistics</div>
                  <div className="pi-kv-grid">
                    {Object.entries(runtimeStatistics).map(([key, value]) => (
                      <div key={key}><strong>{prettyLabel(key)}:</strong> {compactValue(value)}</div>
                    ))}
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Source Connection Card</div>
                  <div className="pi-kv-grid">
                    {Object.entries(runtimeSourceConnection).map(([key, value]) => (
                      <div key={key}><strong>{prettyLabel(key)}:</strong> {compactValue(value)}</div>
                    ))}
                  </div>
                  <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginTop: 14 }}>
                    <button className="pi-btn-confirm" onClick={handlePreviewRuntimeSource} disabled={runtimePreviewLoading || !runtimeSourceConnection.artifact_id}>
                      <FiEye /> {runtimePreviewLoading ? 'Loading Preview...' : 'Preview Data'}
                    </button>
                    <button className="pi-btn-confirm" onClick={handleUseRuntimeSource} disabled={!runtimeSourceConnection.artifact_id}>
                      <FiCheck /> Use Source
                    </button>
                    <button className="pi-btn-confirm" onClick={handleSaveRuntimeSource} disabled={runtimeSaveLoading || !runtimeSourceConnection.artifact_id}>
                      <FiSave /> {runtimeSaveLoading ? 'Saving...' : 'Save Source'}
                    </button>
                    <button className="pi-btn-confirm" onClick={handleGenerateIngestionConfig}>
                      <FiCopy /> Generate Ingestion Config
                    </button>
                    <button className="pi-btn-confirm" onClick={handleBuildPipelineFromSource} disabled={!runtimeSourceConnection.artifact_id}>
                      <FiGitBranch /> Build Pipeline From Source
                    </button>
                  </div>
                  {!runtimeSourceConnection.artifact_id && (
                    <div className="pi-alert warning" style={{ marginTop: 12, fontSize: 12 }}>
                      <FiAlertCircle /> <strong>Artifact resolution failed:</strong> The runtime source is missing a Fabric artifact ID. 
                      Try identifying the Lakehouse name in the pipeline configuration or ensure the source is accessible.
                    </div>
                  )}
                </div>
                {runtimeDiscovery.resolution_diagnostics && runtimeDiscovery.resolution_diagnostics.length > 0 && (
                  <div className="pi-card">
                    <div className="pi-card-title">Artifact Resolution Diagnostics</div>
                    <div className="pi-list">
                      {renderListItems(runtimeDiscovery.resolution_diagnostics, (item) => (
                        <div style={{ fontSize: 11, marginBottom: 4 }}>
                          <span style={{ fontWeight: 700, color: item.status === 'success' ? '#10b981' : '#f59e0b' }}>
                            [{item.strategy.replace(/_/g, ' ').toUpperCase()}]
                          </span> {item.status.toUpperCase()}
                          {item.artifact_id && <span> • ID: <code style={{ background: '#f1f5f9', padding: '2px 4px', borderRadius: 4 }}>{item.artifact_id}</code></span>}
                          {item.item_name && <span> • Match: <strong>{item.item_name}</strong></span>}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
                <div className="pi-card">
                  <div className="pi-card-title">Target Connection Card</div>
                  <div className="pi-kv-grid">
                    {Object.entries(runtimeTargetConnection).map(([key, value]) => (
                      <div key={key}><strong>{prettyLabel(key)}:</strong> {compactValue(value)}</div>
                    ))}
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Schema Discovery</div>
                  <div className="pi-kv-grid">
                    <div><strong>Columns:</strong> {(runtimeSchemaDiscovery.columns || []).length}</div>
                    <div><strong>Timestamp Columns:</strong> {compactValue(runtimeSchemaDiscovery.timestamp_columns)}</div>
                    <div><strong>Nullable Columns:</strong> {compactValue(runtimeSchemaDiscovery.nullable_columns)}</div>
                    <div><strong>Primary Key Candidates:</strong> {compactValue(runtimeSchemaDiscovery.primary_key_candidates)}</div>
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Activity Intelligence</div>
                  <div className="pi-list">
                    {renderListItems(runtimeDiscovery.activity_intelligence || [], (item) => (
                      item
                    ))}
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">DQ Recommendation Engine</div>
                  <div className="pi-list">
                    {renderListItems(runtimeDiscovery.dq_recommendations || [], (item) => (
                      `${item.rule}: ${item.reason}${item.column ? ` [${item.column}]` : ''}`
                    ))}
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Lineage Visualization</div>
                  <div className="pi-list">
                    {renderListItems(runtimeDiscovery.lineage_summary || [], (item) => (
                      `${item.source_label} → ${item.activity_label} → ${item.target_label}`
                    ))}
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Runtime Activity Tracker</div>
                  <div className="pi-list">
                    {renderListItems(runtimeAnalysis.runtime_activity_tracker || [], (item) => (
                      `${item.activity_name} • ${item.activity_type} • ${item.status}${item.duration_ms ? ` • ${item.duration_ms} ms` : ''}`
                    ))}
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Ingestion Config Generator</div>
                  <JsonBlock value={runtimeDiscovery.ingestion_config || {}} />
                </div>
              </div>

              {(runtimeActionMessage || runtimePreviewError) && (
                <div className={`pi-alert ${runtimePreviewError ? 'error' : 'warning'}`} style={{ marginTop: 16 }}>
                  <div>{runtimePreviewError || runtimeActionMessage}</div>
                </div>
              )}

              <div className="pi-card pi-wide" style={{ marginTop: 16 }}>
                <div className="pi-card-title"><FiFile /> Previewable Dataset</div>
                {runtimePreviewLoading ? (
                  <div className="pi-card-content">Loading source preview from runtime metadata...</div>
                ) : runtimePreview?.rows?.length ? (
                  <div style={{ overflowX: 'auto' }}>
                    <div className="pi-card-content" style={{ marginBottom: 10 }}>
                      <strong>{runtimePreview.source_name || runtimePreview.path}</strong> {runtimePreview.total_rows_approx ? `• ${runtimePreview.total_rows_approx}` : ''}
                    </div>
                    <table className="preview-table" style={{ width: '100%', borderCollapse: 'collapse' }}>
                      <thead>
                        <tr>{(runtimePreview.columns || []).map((column) => <th key={column}>{column}</th>)}</tr>
                      </thead>
                      <tbody>
                        {(runtimePreview.rows || []).slice(0, 15).map((row, index) => (
                          <tr key={`preview-${index}`}>
                            {(runtimePreview.columns || []).map((column, valueIndex) => (
                              <td key={`${column}-${valueIndex}`}>{Array.isArray(row) ? compactValue(row[valueIndex]) : compactValue(row?.[column])}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="pi-card-content">Use <strong>Preview Data</strong> to load sampled rows and inferred schema from the captured runtime source.</div>
                )}
              </div>

              <div className="pi-card pi-wide" style={{ marginTop: 16 }}>
                <div className="pi-card-title"><FiDatabase /> Schema Discovery Table</div>
                {(runtimeSchemaDiscovery.columns || []).length ? (
                  <div style={{ overflowX: 'auto' }}>
                    <table className="preview-table" style={{ width: '100%', borderCollapse: 'collapse' }}>
                      <thead>
                        <tr>
                          <th>Column</th>
                          <th>Type</th>
                          <th>Nullable</th>
                          <th>Order</th>
                          <th>Sample</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(runtimeSchemaDiscovery.columns || []).map((column) => (
                          <tr key={column.column_name}>
                            <td>{column.column_name}</td>
                            <td>{column.data_type}</td>
                            <td>{column.nullable ? 'Yes' : 'No'}</td>
                            <td>{column.ordinal_position}</td>
                            <td>{compactValue(column.sample_value)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="pi-card-content">No runtime sample rows were captured for schema inference.</div>
                )}
              </div>

              <div className="pi-card pi-wide" style={{ marginTop: 16 }}>
                <div className="pi-card-title"><FiGitBranch /> Runtime Execution Graph</div>
                {runtimeGraphNodes.length ? (
                  <PipelineFlowCanvas nodes={runtimeGraphNodes} edges={runtimeGraphEdges} />
                ) : (
                  <div className="pi-card-content">No runtime execution graph available.</div>
                )}
              </div>

              <div className="pi-grid" style={{ marginTop: 16 }}>
                <div className="pi-card">
                  <div className="pi-card-title">Resolved Dynamic Expressions</div>
                  <div className="pi-list">
                    {renderListItems(runtimeAnalysis.resolved_expressions || [], (item) => (
                      `${item.expression} => ${typeof item.resolved_value === 'object' ? JSON.stringify(item.resolved_value) : String(item.resolved_value)}`
                    ))}
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Actual API Endpoints</div>
                  <div className="pi-list">
                    {renderListItems(runtimeAnalysis.actual_api_endpoints || [], (item) => (
                      `${item.activity}: ${item.method || 'GET'} ${item.url || 'No URL captured'}${item.status_code ? ` • ${item.status_code}` : ''}`
                    ))}
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Actual Metadata Rows</div>
                  <div className="pi-list">
                    {renderListItems(runtimeAnalysis.actual_metadata_rows || [], (item) => (
                      `${item.activity}: ${item.executed_sql || 'No SQL captured'}`
                    ))}
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Runtime DQ Observations</div>
                  <div className="pi-list">
                    {renderListItems(runtimeAnalysis.runtime_dq_observations || [], (item) => (
                      `${item.activity}: ${item.observation}${item.evidence ? ` (${item.evidence})` : ''}`
                    ))}
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Runtime Notebook Parameters</div>
                  <div className="pi-list">
                    {renderListItems(runtimeAnalysis.runtime_notebook_parameters || [], (item) => (
                      `${item.activity}: ${JSON.stringify(item.parameters || {})}`
                    ))}
                  </div>
                </div>
                <div className="pi-card">
                  <div className="pi-card-title">Runtime SQL Queries</div>
                  <div className="pi-list">
                    {renderListItems(runtimeAnalysis.runtime_sql_queries || [], (item) => (
                      `${item.activity}: ${item.sql}`
                    ))}
                  </div>
                </div>
              </div>

              <div className="pi-card pi-wide" style={{ marginTop: 16 }}>
                <div className="pi-card-title"><FiRefreshCw /> Activity Output Explorer</div>
                <div className="pi-json-grid">
                  {(runtimeAnalysis.activity_outputs ? Object.entries(runtimeAnalysis.activity_outputs) : []).map(([activityName, payload]) => (
                    <SearchableJsonPanel key={activityName} title={activityName} value={payload} />
                  ))}
                </div>
              </div>

              <div className="pi-card pi-wide" style={{ marginTop: 16 }}>
                <div className="pi-card-title"><FiFile /> Runtime Payload Viewer</div>
                <div className="pi-json-grid">
                  <SearchableJsonPanel title="Runtime Payload" value={runtimeAnalysis.runtime_payload_viewer} />
                  <SearchableJsonPanel title="Resolved Expressions" value={runtimeAnalysis.resolved_expressions} />
                </div>
              </div>
            </div>
          )}

          {selectedPlatform === 'FABRIC' && (
            <div className="pi-bundle-section pi-card pi-wide">
              <div className="pi-bundle-header">
                <div>
                  <div className="pi-card-title" style={{ marginBottom: 6 }}><FiLayers /> Fabric Export Bundle Analysis</div>
                  <div className="pi-bundle-subtitle">Append uploaded Fabric export metadata to the existing live discovery result and reverse-engineer deeper ingestion logic.</div>
                </div>
              </div>

              <div className="pi-upload-status-grid">
                <div className="pi-card">
                  <div className="pi-card-title"><FiUploadCloud /> Upload Bundle Status</div>
                  <div className="pi-upload-zone-wrapper">
                    <div
                      className={`upload-zone ${isBundleDragging ? 'dragging' : ''} ${bundleUploadStatus === 'uploading' ? 'loading' : ''}`}
                      onDragOver={(event) => { event.preventDefault(); setIsBundleDragging(true); }}
                      onDragLeave={() => setIsBundleDragging(false)}
                      onDrop={(event) => {
                        event.preventDefault();
                        setIsBundleDragging(false);
                        handleBundleFile(event.dataTransfer.files?.[0]);
                      }}
                    >
                      <input
                        type="file"
                        id="fabric-bundle-upload"
                        hidden
                        accept=".zip"
                        onChange={(event) => handleBundleFile(event.target.files?.[0])}
                      />
                      <label htmlFor="fabric-bundle-upload" className="pi-upload-label">
                        {bundleUploadStatus === 'uploading' ? <div className="pi-spinner" /> : <FiUploadCloud size={42} color="#2563eb" />}
                        <div className="pi-upload-title">{bundleUploadStatus === 'uploading' ? 'Uploading and analyzing bundle...' : 'Click or Drag ZIP to Upload'}</div>
                        <div className="pi-upload-help">Supports Microsoft Fabric exported pipeline bundles (.zip)</div>
                      </label>
                    </div>
                    <div className="pi-upload-meta">
                      <div>Status: <strong>{bundleUploadStatus.toUpperCase()}</strong></div>
                      <div>Progress: <strong>{bundleUploadProgress}%</strong></div>
                    </div>
                    {bundleUploadStatus === 'uploading' && (
                      <div className="pi-progress-bar">
                        <div className="pi-progress-bar-fill" style={{ width: `${bundleUploadProgress}%` }} />
                      </div>
                    )}
                    {bundleUploadError && <div className="pi-error" style={{ marginTop: 12 }}><FiAlertCircle /> {bundleUploadError}</div>}
                  </div>
                </div>

                <div className="pi-card">
                  <div className="pi-card-title"><FiFile /> Manifest Analysis</div>
                  {bundleAnalysis ? (
                    <div className="pi-kv-grid">
                      <div><strong>Name:</strong> {renderValue(bundleAnalysis.manifest_analysis?.name, 'Unknown')}</div>
                      <div><strong>Type:</strong> {renderValue(bundleAnalysis.manifest_analysis?.type, 'Unknown')}</div>
                      <div><strong>Files:</strong> {bundleAnalysis.manifest_analysis?.file_count || 0}</div>
                      <div><strong>Pipeline JSON:</strong> {bundleAnalysis.manifest_analysis?.selected_pipeline_path || 'Unknown'}</div>
                    </div>
                  ) : (
                    <div className="pi-card-content">Upload a Fabric export bundle to inspect `manifest.json` and bundle metadata.</div>
                  )}
                </div>
              </div>

              {bundleAnalysis && (
                <>
                  <div className="pi-grid" style={{ marginTop: 16 }}>
                    <div className="pi-card">
                      <div className="pi-card-title"><FiSearch /> Source Discovery</div>
                      <div className="pi-tag-list">
                        {(bundleAnalysis.source_discovery?.linked_services || []).map((item, index) => <Tag key={`${renderValue(item, 'item')}-${index}`}>{renderValue(item, 'Unknown')}</Tag>)}
                        {!bundleAnalysis.source_discovery?.linked_services?.length && <span className="pi-card-content">No linked services discovered.</span>}
                      </div>
                    </div>
                    <div className="pi-card">
                      <div className="pi-card-title"><FiDatabase /> Ingestion Intelligence</div>
                      <div className="pi-kv-grid">
                        <div><strong>Source:</strong> {renderValue(bundleAnalysis.ingestion_intelligence?.source_system, 'Not present')}{renderEvidenceMeta(bundleAnalysis.ingestion_intelligence?.source_system)}</div>
                        <div><strong>Endpoint:</strong> {renderValue(bundleAnalysis.ingestion_intelligence?.endpoint, 'Not present')}{renderEvidenceMeta(bundleAnalysis.ingestion_intelligence?.endpoint)}</div>
                        <div><strong>Frequency:</strong> {renderValue(bundleAnalysis.ingestion_intelligence?.frequency, 'Not present')}{renderEvidenceMeta(bundleAnalysis.ingestion_intelligence?.frequency)}</div>
                        <div><strong>Trigger:</strong> {renderValue(bundleAnalysis.ingestion_intelligence?.trigger_type, 'Not present')}{renderEvidenceMeta(bundleAnalysis.ingestion_intelligence?.trigger_type)}</div>
                      </div>
                    </div>
                    <div className="pi-card">
                      <div className="pi-card-title"><FiFile /> File Structure Intelligence</div>
                      <div className="pi-kv-grid">
                        <div><strong>Delimiters:</strong> {renderValue(bundleAnalysis.file_structure_intelligence?.delimiters, 'Not present')}</div>
                        <div><strong>Nested:</strong> {renderValue(bundleAnalysis.file_structure_intelligence?.nested_structures, 'None')}</div>
                        <div><strong>Mandatory Fields:</strong> {(bundleAnalysis.file_structure_intelligence?.mandatory_fields || []).length}</div>
                        <div><strong>Date Formats:</strong> {renderValue(bundleAnalysis.file_structure_intelligence?.date_formats, 'Not present')}</div>
                      </div>
                    </div>
                    <div className="pi-card">
                      <div className="pi-card-title"><FiCheck /> DQ Recommendations</div>
                      <div className="pi-list">
                        {renderListItems(bundleAnalysis.dq_recommendations || [], (item) => (
                          typeof item === 'object'
                            ? `${item.rule || 'Rule'}: ${item.reason || 'No reason'}${item.evidence ? ` (${item.evidence})` : ''}`
                            : String(item)
                        ))}
                      </div>
                    </div>
                    <div className="pi-card">
                      <div className="pi-card-title"><FiClock /> Trigger & Scheduling Analysis</div>
                      <div className="pi-kv-grid">
                        <div><strong>Trigger Type:</strong> {renderValue(bundleAnalysis.trigger_scheduling_analysis?.scheduling?.trigger_type, 'Not present')}{renderEvidenceMeta(bundleAnalysis.trigger_scheduling_analysis?.scheduling?.trigger_type)}</div>
                        <div><strong>Frequency:</strong> {renderValue(bundleAnalysis.trigger_scheduling_analysis?.scheduling?.frequency, 'Not present')}{renderEvidenceMeta(bundleAnalysis.trigger_scheduling_analysis?.scheduling?.frequency)}</div>
                        <div><strong>Retry Policies:</strong> {(bundleAnalysis.trigger_scheduling_analysis?.retry_policies || []).length}</div>
                        <div><strong>Triggers:</strong> {(bundleAnalysis.trigger_scheduling_analysis?.triggers || []).length}</div>
                      </div>
                    </div>
                    <div className="pi-card">
                      <div className="pi-card-title"><FiZap /> AI Insights Panel</div>
                      <div className="pi-list">
                        {renderListItems(bundleAnalysis.ai_structured_output?.ai_insights || [], (item) => (
                          typeof item === 'object' ? JSON.stringify(item) : String(item)
                        ))}
                      </div>
                    </div>
                  </div>

                  <div className="pi-card pi-wide" style={{ marginTop: 16 }}>
                    <div className="pi-card-title"><FiGitBranch /> Activity Dependency Graph</div>
                    {graphNodes.length ? (
                      <PipelineFlowCanvas nodes={graphNodes} edges={graphEdges} />
                    ) : (
                      <div className="pi-card-content">No activity graph could be derived from the uploaded/exported configuration.</div>
                    )}
                  </div>

                  <div className="pi-card pi-wide" style={{ marginTop: 16 }}>
                    <div className="pi-card-title"><FiRefreshCw /> Extracted Config Viewer</div>
                    <div className="pi-json-grid">
                      <SearchableJsonPanel title="Raw Uploaded JSON" value={bundleAnalysis.uploaded_pipeline_config?.raw_uploaded_json} defaultOpen />
                      <SearchableJsonPanel title="Raw manifest.json" value={bundleAnalysis.uploaded_pipeline_config?.raw_manifest_json} />
                      <SearchableJsonPanel title="Final Merged Config" value={bundleAnalysis.final_pipeline_config} />
                      <SearchableJsonPanel title="AI-Generated Structured Output" value={bundleAnalysis.ai_structured_output} />
                    </div>
                  </div>
                </>
              )}
            </div>
          )}

          <div className="pi-actions">
            <button className="pi-btn-confirm" onClick={() => onConfirm({ ...data, deploymentStrategy, selectedWorkspace, selectedPipeline })} disabled={selectedPlatform === 'FABRIC' && !deploymentStrategy}>
              <FiCheck /> {selectedPlatform === 'FABRIC' ? 'Confirm Strategy & Configure' : 'Configure Data Sources'}
            </button>
          </div>
        </>
      )}

      {showCloudScanModal && (
        <CloudPortalScanModal
          selectedClient={clientName}
          initialTarget={target}
          allowedTargets={allowedTargets.filter((item) => item.scan).map((item) => item.id)}
          sourceType={selectedTarget?.sourceType}
          useCloudLlm={useCloudLlm}
          onTargetChange={setTarget}
          onClose={() => setShowCloudScanModal(false)}
          onScanComplete={(result) => {
            setShowCloudScanModal(false);
            if (result.__fabric_access_token) {
              setFabricAccessToken(result.__fabric_access_token);
            }
            if (result.__fabric_token_validation) {
              setFabricTokenValidation(result.__fabric_token_validation);
            }
            if (selectedPlatform === 'FABRIC') {
               const discovered = result.fabric_workspaces || 
                                  result.workspaces || 
                                  result.raw_cloud_scan?.fabric_workspaces || 
                                  result.raw_cloud_scan?.workspaces || 
                                  result.payload?.fabric_workspaces || [];
               setScanResults(discovered);
               setData(null);
               setSelectedPipeline(null);
            } else {
               setData(result);
               onScanComplete?.(result);
            }
          }}
        />
      )}
    </div>
  );
}
