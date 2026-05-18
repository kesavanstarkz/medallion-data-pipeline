import { FiSearch, FiFolder, FiFileText, FiLink, FiCloud, FiBox, FiUploadCloud, FiCheck, FiSettings, FiFile, FiChevronRight, FiZap, FiPlus, FiTrash2, FiRefreshCw } from 'react-icons/fi';
import { useState, useEffect, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";
import FluentSelect from "../FluentSelect";
import logo from "../../assets/images/image.png";
import "../PipelineIntelligence.css";

export default function StepSources({
  selectedClient,
  apiSources,
  s3Sources = [],
  adlsSources = [],
  apiSourcesLoading,
  selectedApiSource,
  setSelectedApiSource,
  selectedEndpoint,
  setSelectedEndpoint,
  sourceType,
  setSourceType,
  setFolderPath,
  setShowUploadModal,
  openExplorer,
  onNext,
  call,
  refreshTrigger,
  intelligenceData,
  configPersisted,
  setConfigPersisted,
  setSelectedSources,
  toast,
  onManualSourceSelected,
  extractedFabricData,
  setExtractedFabricData,
  fabricMode = 'DISCOVERY',
  setPipelineDeployed,
  selectedPlatform = '',
  folderPath = '',
}) {
  const [localFiles, setLocalFiles] = useState([]);
  const [localFilesLoading, setLocalFilesLoading] = useState(false);
  const [selectedLocalFiles, setSelectedLocalFiles] = useState([]);
  const [selectedS3Path, setSelectedS3Path] = useState("");
  const [selectedADLSPath, setSelectedADLSPath] = useState("");
  const [apiEndpoints, setApiEndpoints] = useState([]);
  const [apiEndpointDraft, setApiEndpointDraft] = useState("");
  const [savingGeneratedConfig, setSavingGeneratedConfig] = useState(false);
  const [saveGeneratedError, setSaveGeneratedError] = useState("");
  const [activeTab, setActiveTab] = useState("LOCAL");

  useEffect(() => {
    if (selectedClient) {
      setSelectedLocalFiles([]);
      setSelectedS3Path("");
      setSelectedADLSPath("");
      setApiEndpoints([]);
      setApiEndpointDraft("");
      setLocalFiles([]);
      fetchLocalFiles(selectedClient);
    }
  }, [selectedClient, refreshTrigger]);

  useEffect(() => {
    if (sourceType === 'FABRIC') {
      setActiveTab('FABRIC');
    } else if (sourceType === 'ADLS' && folderPath) {
      setActiveTab('ADLS');
      setSelectedADLSPath(folderPath);
    }
  }, [sourceType, folderPath]);

  const selectedSources = useMemo(() => [
    ...selectedLocalFiles.map((datasetId) => {
      const file = localFiles.find((item) => item.dataset_id === datasetId) || {};
      return {
        type: "LOCAL",
        file: file.display_name || file.source_object || file.file_name || datasetId,
        dataset_id: datasetId,
      };
    }),
    ...apiEndpoints.map((endpoint) => ({ type: "API", endpoint })),
    ...(selectedS3Path ? [{ type: "S3", path: selectedS3Path }] : []),
    ...(selectedADLSPath ? [{ type: "ADLS", path: selectedADLSPath }] : []),
    ...(extractedFabricData ? [{ type: "FABRIC", data: extractedFabricData }] : []),
    ...(sourceType === 'FABRIC' && intelligenceData?.staging_table && !extractedFabricData ? [{ 
      type: "FABRIC", 
      data: { ...intelligenceData, staging_table: intelligenceData.staging_table } 
    }] : []),
  ], [selectedLocalFiles, localFiles, apiEndpoints, selectedS3Path, selectedADLSPath, extractedFabricData, sourceType, intelligenceData]);
  
  const isFabricDeploy = fabricMode === 'DEPLOY';
  const canContinue = selectedSources.length > 0 || isFabricDeploy;

  useEffect(() => {
    setSelectedSources?.(selectedSources);
    const primary = selectedSources[0];
    if (!primary) {
      setSourceType("");
      setFolderPath("");
      setSelectedEndpoint("");
      setSelectedApiSource(null);
      return;
    }
    if (primary.type === "LOCAL") {
      setSourceType("LOCAL");
      const ids = selectedSources.filter((item) => item.type === "LOCAL").map((item) => item.dataset_id).join(",");
      setFolderPath(ids);
      setSelectedEndpoint(ids);
      setSelectedApiSource("local-multi");
    } else if (primary.type === "API") {
      setSourceType("API");
      const endpoints = selectedSources.filter((item) => item.type === "API").map((item) => item.endpoint).join(",");
      setFolderPath(endpoints);
      setSelectedEndpoint(endpoints);
      setSelectedApiSource("api-multi");
    } else if (primary.type === "S3") {
      setSourceType("S3");
      setFolderPath(primary.path);
      setSelectedEndpoint(primary.path);
      setSelectedApiSource("s3-path");
    } else if (primary.type === "ADLS") {
      setSourceType("ADLS");
      setFolderPath(primary.path);
      setSelectedEndpoint(primary.path);
      setSelectedApiSource("adls-path");
    } else if (primary.type === "FABRIC") {
      setSourceType("FABRIC");
      const name = primary.data?.manifest_json?.name || "Fabric Pipeline";
      setFolderPath(name);
      setSelectedEndpoint(name);
      setSelectedApiSource("fabric-extracted");
    }
  }, [selectedSources, setSelectedSources, setSourceType, setFolderPath, setSelectedEndpoint, setSelectedApiSource]);

  async function fetchLocalFiles(client) {
    setLocalFilesLoading(true);
    try {
      const data = await call(`/upload/list?client_name=${client}`);
      setLocalFiles(data.files || []);
    } catch (e) {
      setLocalFiles([]);
    } finally {
      setLocalFilesLoading(false);
    }
  }

  const toggleLocalFile = (file) => {
    onManualSourceSelected?.();
    let newSelected;
    if (selectedLocalFiles.includes(file.dataset_id)) {
      newSelected = selectedLocalFiles.filter((id) => id !== file.dataset_id);
    } else {
      newSelected = [...selectedLocalFiles, file.dataset_id];
    }
    setSelectedLocalFiles(newSelected);
  };

  const sourceTabs = useMemo(() => [
    { id: "LOCAL", label: "Local Files", icon: <FiFolder />, color: "#10b981" },
    { id: "API", label: "REST API", icon: <FiLink />, color: "#3b82f6" },
    ...(s3Sources.length > 0 ? [{ id: "S3", label: "AWS S3", icon: <FiBox />, color: "#f59e0b" }] : []),
    ...(adlsSources.length > 0 ? [{ id: "ADLS", label: "Azure ADLS", icon: <FiCloud />, color: "#0078d4" }] : []),
    ...(intelligenceData?.framework === "Microsoft Fabric" ? [{ id: "FABRIC", label: "Fabric Intelligence", icon: <FiZap />, color: "#6366f1" }] : []),
  ], [s3Sources.length, adlsSources.length, intelligenceData]);

  const intelligenceSuggestion = useMemo(() => {
    if (!intelligenceData || intelligenceData.is_fallback) return null;
    return {
      title: intelligenceData.framework === "Microsoft Fabric" ? "Fabric Pipeline Detected" : "Source Suggestion",
      path: intelligenceData.reformatted_config?.source_path || "",
      type: intelligenceData.reformatted_config?.source_type || "API"
    };
  }, [intelligenceData]);

  return (
    <motion.div
      key="step2"
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
      exit={{ opacity: 0, x: -20 }}
      className="orch-step-panel"
    >
      <div className="step-header-responsive">
        <div className="step-header-text">
          <h2 className="step-title">Data Sources — {selectedClient}</h2>
          <p className="step-sub">Select files or endpoints to extract data from.</p>
        </div>

        <div className="step-header-actions">
          <div className="step-tabs">
            {sourceTabs.map((t) => (
              <button
                key={t.id}
                className={`step-tab ${activeTab === t.id ? "active" : ""}`}
                onClick={() => setActiveTab(t.id)}
              >
                <span style={{ color: activeTab === t.id ? t.color : 'inherit' }}>{t.icon}</span>
                {t.label}
              </button>
            ))}
          </div>
          <div className="header-logo-divider" />
          <img src={logo} alt="Agilisium" className="step-header-logo" />
        </div>
      </div>

      <div className="step-body">
        {intelligenceSuggestion && (
          <div className="intelligence-suggestion-box">
            <div className="suggestion-header">
              <FiZap />
              <span>Intelligence Detection</span>
            </div>
            <div className="suggestion-body">
              <div className="suggestion-info">
                <div className="suggestion-title">{intelligenceSuggestion.title}</div>
                <div className="suggestion-path">{intelligenceSuggestion.path}</div>
              </div>
              <button 
                className="orch-btn ghost tiny"
                onClick={() => {
                  const isFabric = intelligenceSuggestion.type === "FABRIC" || intelligenceSuggestion.type === "NEON_STAGED_SOURCE";
                  setActiveTab(isFabric ? "FABRIC" : intelligenceSuggestion.type);
                  if (isFabric) {
                    setSourceType(intelligenceSuggestion.type);
                    setFolderPath(intelligenceSuggestion.path);
                    setSelectedEndpoint(intelligenceSuggestion.path);
                    setSelectedApiSource("fabric-extracted");
                  }
                }}
              >
                Use Intelligence Source
              </button>
            </div>
          </div>
        )}

        <div className="source-content-area">
          {activeTab === "LOCAL" && (
            <div className="local-source-view">
              <div className="source-controls">
                <div className="search-input-wrapper">
                  <FiSearch className="search-icon" />
                  <input type="text" placeholder="Search uploaded files..." className="search-input" />
                </div>
                <button className="orch-btn ghost" onClick={() => setShowUploadModal(true)}>
                  <FiUploadCloud /> Upload New
                </button>
              </div>

              <div className="dataset-grid">
                {localFilesLoading ? (
                  <div className="loading-state">Loading files...</div>
                ) : localFiles.length === 0 ? (
                  <div className="empty-state">No files uploaded yet.</div>
                ) : (
                  localFiles.map((file) => (
                    <div
                      key={file.dataset_id}
                      className={`dataset-card ${selectedLocalFiles.includes(file.dataset_id) ? "selected" : ""}`}
                      onClick={() => toggleLocalFile(file)}
                    >
                      <div className="dataset-icon">
                        <FiFileText />
                      </div>
                      <div className="dataset-info">
                        <div className="dataset-name">{file.display_name || file.file_name}</div>
                        <div className="dataset-meta">{file.file_size || 'Unknown size'} • {file.file_type || 'RAW'}</div>
                      </div>
                      {selectedLocalFiles.includes(file.dataset_id) && (
                        <div className="selection-badge">
                          <FiCheck size={12} />
                        </div>
                      )}
                    </div>
                  ))
                )}
              </div>
            </div>
          )}

          {activeTab === "FABRIC" && (
            <div className="fabric-source-view">
              <div className="dataset-grid">
                <div 
                  className={`dataset-card selected`}
                  style={{ borderColor: '#6366f1' }}
                >
                  <div className="dataset-icon" style={{ background: 'rgba(99, 102, 241, 0.1)', color: '#6366f1' }}>
                    <FiZap />
                  </div>
                  <div className="dataset-info">
                    <div className="dataset-name">{intelligenceData?.reformatted_config?.pipeline_name || intelligenceData?.staging_table || "Extracted Pipeline"}</div>
                    <div className="dataset-meta">
                      {intelligenceData?.reformatted_config?.source_type || 'FABRIC'} • {intelligenceData?.staging_table || intelligenceData?.reformatted_config?.source_path}
                    </div>
                  </div>
                  <div className="selection-badge" style={{ background: '#6366f1' }}>
                    <FiCheck size={12} />
                  </div>
                </div>
              </div>
              <div className="pi-alert info" style={{ marginTop: 20 }}>
                <FiZap /> <strong>NeonDB Staging Active:</strong> Data from the {intelligenceData?.framework || 'Fabric'} environment has been staged in NeonDB. 
                Orchestration will use the <code>{intelligenceData?.staging_table}</code> table as the authoritative source.
              </div>
            </div>
          )}

          {activeTab === "API" && (
            <div className="api-source-view">
              <div className="api-input-row">
                <input
                  value={apiEndpointDraft}
                  onChange={(e) => setApiEndpointDraft(e.target.value)}
                  placeholder="Enter endpoint relative path (e.g. /users)"
                  className="orch-input"
                />
                <button
                  className="orch-btn primary"
                  onClick={() => {
                    if (apiEndpointDraft) {
                      setApiEndpoints([...new Set([...apiEndpoints, apiEndpointDraft])]);
                      setApiEndpointDraft("");
                    }
                  }}
                >
                  <FiPlus /> Add
                </button>
              </div>

              <div className="endpoint-list">
                {apiEndpoints.map((ep) => (
                  <div key={ep} className="endpoint-item">
                    <FiLink />
                    <span className="endpoint-path">{ep}</span>
                    <button className="delete-btn" onClick={() => setApiEndpoints(apiEndpoints.filter(e => e !== ep))}>
                      <FiTrash2 size={14} />
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}
          
          {(activeTab === "S3" || activeTab === "ADLS") && (
            <div className="cloud-source-view">
              <div className="cloud-path-input">
                <div className="path-icon">{activeTab === "S3" ? <FiBox /> : <FiCloud />}</div>
                <input 
                  value={activeTab === "S3" ? selectedS3Path : selectedADLSPath}
                  onChange={(e) => activeTab === "S3" ? setSelectedS3Path(e.target.value) : setSelectedADLSPath(e.target.value)}
                  placeholder={activeTab === "S3" ? "s3://bucket/path/to/data" : "https://account.dfs.core.windows.net/container/path"}
                  className="orch-input"
                />
              </div>
              <p className="helper-text">Enter the full path to your dataset. Ensure the client has access.</p>
            </div>
          )}
        </div>
      </div>

      <AnimatePresence>
        {canContinue && (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="step-footer-actions-container"
          >
            <div className="selection-summary">
              Selected: <strong>{selectedSources.length} Source(s)</strong>
            </div>
            <button className="orch-btn primary premium-btn" onClick={onNext}>
              Continue <FiChevronRight />
            </button>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
