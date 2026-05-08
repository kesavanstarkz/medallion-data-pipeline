import { FiSearch, FiFolder, FiFileText, FiLink, FiCloud, FiBox, FiUploadCloud, FiCheck, FiSettings, FiFile, FiChevronRight, FiZap } from 'react-icons/fi';
import { useState, useEffect, useMemo } from "react";
import { motion } from "framer-motion";
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
  const [fabricToken, setFabricToken] = useState(null);
  const [selectedFabricWorkspace, setSelectedFabricWorkspace] = useState(null);
  const [fabricTab, setFabricTab] = useState('discovery'); // 'discovery' | 'deploy'

  useEffect(() => {
    if (selectedClient) {
      setSelectedLocalFiles([]); // Clear selections when switching clients
      setSelectedS3Path("");
      setSelectedADLSPath("");
      setApiEndpoints([]);
      setApiEndpointDraft("");
      setLocalFiles([]); // Reset list to show loading state if needed
      fetchLocalFiles(selectedClient);
    }
  }, [selectedClient, refreshTrigger]);

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
  ], [selectedLocalFiles, localFiles, apiEndpoints, selectedS3Path, selectedADLSPath, extractedFabricData]);
  
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

  useEffect(() => {
    if (String(sourceType || "").toUpperCase() === "LOCAL" && selectedEndpoint) {
      const ids = String(selectedEndpoint).split(",").map((item) => item.trim()).filter(Boolean);
      if (ids.length > 0) {
        setSelectedLocalFiles(ids);
        setActiveTab("LOCAL");
      }
    }
  }, [sourceType, selectedEndpoint, refreshTrigger]);

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

  const [activeTab, setActiveTab] = useState("LOCAL");

  const isTabSupported = () => {
    // Pipeline Intelligence is advisory only. Keep every published/manual tab available.
    return true;
  };

  const sourceTabs = useMemo(() => [
    { id: "LOCAL", label: "Local Files", icon: <FiFolder />, color: "#10b981" },
    { id: "API", label: "REST API", icon: <FiLink />, color: "#3b82f6" },
    ...(s3Sources.length > 0 ? [{ id: "S3", label: "AWS S3", icon: <FiBox />, color: "#f59e0b" }] : []),
    ...(adlsSources.length > 0 ? [{ id: "ADLS", label: "Azure ADLS", icon: <FiCloud />, color: "#0078d4" }] : []),
  ], [s3Sources.length, adlsSources.length]);

  useEffect(() => {
    if (sourceTabs.length > 0 && !sourceTabs.some((tab) => tab.id === activeTab)) {
      setActiveTab(sourceTabs[0].id);
    }
  }, [sourceTabs, activeTab]);

  const scanDetails =
    intelligenceData?.ingestion_details ||
    intelligenceData?.reformatted_config ||
    {};
  const detectedSourceType =
    sourceType ||
    scanDetails.source_type ||
    intelligenceData?.reformatted_config?.source_type ||
    sourceTabs.find((t) => t.id === activeTab)?.id;
  const detectedSourcePath =
    selectedEndpoint ||
    scanDetails.source_path ||
    intelligenceData?.reformatted_config?.source_path ||
    "";
  const support = intelligenceData?.ingestion_support || {};
  const ingestionRows = [
    { key: "file_based", label: "File-based ingestion" },
    { key: "api", label: "API ingestion" },
    { key: "database", label: "Database ingestion" },
    { key: "streaming", label: "Streaming" },
    { key: "batch", label: "Batch" },
  ];
  const isRealIntelligenceScan =
    !!intelligenceData &&
    !intelligenceData.is_fallback &&
    intelligenceData.scan_status !== "failed" &&
    (intelligenceData.framework === "REST API" || intelligenceData.auth_mode !== "none") &&
    intelligenceData.pipeline_capabilities?.scan_mode !== "mock";

  const getFabricSuggestion = () => {
    if (
      detectedSourceType !== "FABRIC" &&
      intelligenceData?.framework !== "Microsoft Fabric"
    )
      return null;

    const runtimeConfig = intelligenceData?.reformatted_config;
    const isRuntime = runtimeConfig?.discovery_mode === "FABRIC_RUNTIME";

    if (isRuntime) {
      return {
        tab: null,
        type: "FABRIC",
        title: runtimeConfig.pipeline_name,
        path: runtimeConfig.source_path,
        meta: [
          `Workspace: ${runtimeConfig.source?.workspace_id || runtimeConfig.targets?.workspace_id || "Fabric Workspace"}`,
          `Pipeline: ${runtimeConfig.pipeline_name}`,
          ...(runtimeConfig.linked_services || []).map((ls) => `Linked Service: ${ls}`),
          ...(runtimeConfig.file_types || []).map((ft) => `Format: ${ft}`),
          runtimeConfig.targets?.lakehouse ? "Target: Lakehouse" : null,
          runtimeConfig.targets?.warehouse ? "Target: Warehouse" : null,
          `Connector: ${runtimeConfig.source_type || "DelimitedTextSource"}`
        ].filter(Boolean),
      };
    }

    const rawScan = intelligenceData?.raw_cloud_scan || {};
    const workspace = rawScan.fabric_workspaces?.[0];
    const pipeline =
      intelligenceData?.data_pipelines?.[0] ||
      rawScan.pipeline_definitions?.[0] ||
      {};
    const lakehouse = rawScan.fabric_items?.find(
      (item) => item?.configuration?.Type === "Lakehouse",
    );
    const warehouse = rawScan.fabric_items?.find(
      (item) => item?.configuration?.Type === "Warehouse",
    );
    const workspaceName =
      workspace?.name ||
      workspace?.displayName ||
      workspace?.id?.replace(/^fabric \|\| /, "") ||
      "Fabric workspace";
    const pipelineName =
      pipeline?.name ||
      pipeline?.pipeline_name ||
      pipeline?.displayName ||
      intelligenceData?.reformatted_config?.pipeline_name ||
      "Fabric pipeline";

    return {
      tab: null,
      type: "FABRIC",
      title: "Microsoft Fabric Pipeline",
      path: detectedSourcePath || `fabric://${workspaceName}/${pipelineName}`,
      meta: [
        `Workspace: ${workspaceName}`,
        `Pipeline: ${pipelineName}`,
        lakehouse
          ? `Lakehouse: ${lakehouse.id?.replace(/^fabric \|\| /, "") || lakehouse.displayName}`
          : null,
        warehouse
          ? `Warehouse: ${warehouse.id?.replace(/^fabric \|\| /, "") || warehouse.displayName}`
          : null,
      ].filter(Boolean),
    };
  };

  const getIntelligenceSuggestion = () => {
    if (!intelligenceData) return null;
    const normalizedType = (detectedSourceType || "").toUpperCase();
    if (
      normalizedType === "FABRIC" ||
      intelligenceData.framework === "Microsoft Fabric"
    )
      return getFabricSuggestion();
    if (normalizedType === "S3")
      return {
        tab: "S3",
        type: "S3",
        title: "AWS S3 Source",
        path: detectedSourcePath,
        meta: ["Prefilled under AWS S3"],
      };
    if (normalizedType === "ADLS")
      return {
        tab: "ADLS",
        type: "ADLS",
        title: "Azure ADLS Source",
        path: detectedSourcePath,
        meta: ["Prefilled under Azure ADLS"],
      };
    if (normalizedType === "API")
      return {
        tab: "API",
        type: "API",
        title: "REST API Source",
        path: detectedSourcePath,
        meta: ["Prefilled under REST API"],
      };
    if (normalizedType === "LOCAL")
      return {
        tab: "LOCAL",
        type: "LOCAL",
        title: "Local/File Source",
        path: detectedSourcePath,
        meta: ["Prefilled under Local Files"],
      };
    return {
      tab: null,
      type: normalizedType || "DETECTED",
      title: "Detected Source",
      path: detectedSourcePath,
      meta: ["Review and add manually if needed"],
    };
  };

  const findRegisteredSuggestionSource = (suggestion) => {
    if (!suggestion) return null;
    const path = String(suggestion.path || "");
    if (suggestion.type === "S3") {
      const bucket = path.startsWith("s3://") ? path.slice(5).split("/")[0] : "";
      return s3Sources.find((s) => s.bucket_name === bucket || s.aws_bucket === bucket) || null;
    }
    if (suggestion.type === "ADLS") {
      return adlsSources.find((s) => path.includes(`${s.azure_account}/${s.azure_container}`)) || null;
    }
    if (suggestion.type === "API") {
      return apiSources.find((s) => path.startsWith(s.base_url || "") || (s.endpoints || []).includes(path)) || null;
    }
    return null;
  };

  const applyIntelligenceSuggestion = () => {
    const suggestion = getIntelligenceSuggestion();
    if (!suggestion) return;
    const registered = findRegisteredSuggestionSource(suggestion);
    if (["S3", "ADLS", "API"].includes(suggestion.type) && !registered) {
      toast?.("Suggestion is not mapped to a registered source yet. Register the source first, or add it manually.", "warning");
      if (suggestion.tab) setActiveTab(suggestion.tab);
      return;
    }
    onManualSourceSelected?.();
    if (suggestion.type === "S3") {
      setSelectedS3Path(suggestion.path || "");
    } else if (suggestion.type === "ADLS") {
      setSelectedADLSPath(suggestion.path || "");
    } else if (suggestion.type === "API" && suggestion.path) {
      setApiEndpoints((prev) => [...new Set([...prev, suggestion.path])]);
    } else if (suggestion.type === "LOCAL" && suggestion.path) {
      setSelectedLocalFiles((prev) => [...new Set([...prev, suggestion.path])]);
    }
    setSelectedApiSource(registered?.id || `${suggestion.type.toLowerCase()}-suggestion`);
    if (suggestion.tab) setActiveTab(suggestion.tab);
  };

  async function saveIntelligenceConfigAndContinue() {
    if (!intelligenceData) {
      onNext();
      return;
    }
    if (!isRealIntelligenceScan) {
      console.debug(
        "Skipping intelligence config save before Step 4 because scan is not execution-ready",
        {
          framework: intelligenceData.framework,
          scan_status: intelligenceData.scan_status,
          auth_mode: intelligenceData.auth_mode,
          is_fallback: intelligenceData.is_fallback,
        },
      );
      onNext();
      return;
    }

    setSavingGeneratedConfig(true);
    setSaveGeneratedError("");
    try {
      console.debug("Saving intelligenceData before Step 4", {
        framework: intelligenceData.framework,
        scan_status: intelligenceData.scan_status,
        auth_mode: intelligenceData.auth_mode,
        is_fallback: intelligenceData.is_fallback,
        source_path: detectedSourcePath,
      });
      const response = await call("/config/save", "POST", {
        client_name: selectedClient,
        intelligence_data: intelligenceData,
        source_type: detectedSourceType,
        source_path: detectedSourcePath,
      });
      console.debug("Config save API response", response);
      setConfigPersisted?.(true);
      toast?.(
        `Saved ${response.rows_inserted || 0} configuration row(s)`,
        "success",
      );
      onNext();
    } catch (e) {
      const msg = e?.message || "Failed to save generated configuration";
      setSaveGeneratedError(msg);
      toast?.(msg, "error");
    } finally {
      setSavingGeneratedConfig(false);
    }
  }

  // Deprecated replacement view kept out of the render path; Data Sources must stay on the published tabbed UI.
  // eslint-disable-next-line no-unused-vars
  const renderScanDrivenSources = () => (
    <div className="step-body">
      <div
        style={{
          marginBottom: 20,
          padding: 12,
          background: "rgba(16, 185, 129, 0.1)",
          border: "1px solid rgba(16, 185, 129, 0.2)",
          borderRadius: 8,
          display: "flex",
          alignItems: "center",
          gap: 12,
          color: "#047857",
        }}
      >
        <FiZap size={20} />
        <div style={{ fontSize: 13, fontWeight: 600 }}>
          <strong>Auto-detected from Pipeline Intelligence.</strong> Confirm the
          detected source and ingestion modes before configuration.
        </div>
      </div>

      <div className="pi-grid">
        {ingestionRows.map((row) => {
          const supported = !!support[row.key];
          return (
            <div
              key={row.key}
              className="pi-card"
              style={{ opacity: supported ? 1 : 0.55 }}
            >
              <div className="pi-card-title">{row.label}</div>
              <div className="pi-card-content">
                <span className={`pi-tag ${supported ? "active" : "inactive"}`}>
                  {supported ? "Supported" : "Not Detected"}
                </span>
              </div>
            </div>
          );
        })}
      </div>

      <div className="pi-card" style={{ marginTop: 16 }}>
        <div className="pi-card-title">Detected Source</div>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "180px 1fr",
            gap: 12,
            alignItems: "center",
          }}
        >
          <label className="cloud-scan-field">
            <span>Source Type</span>
            <select
              className="orch-input"
              value={detectedSourceType || ""}
              onChange={(e) => setSourceType(e.target.value)}
            >
              <option value="S3">S3</option>
              <option value="ADLS">ADLS</option>
              <option value="API">API</option>
              <option value="LOCAL">LOCAL</option>
            </select>
          </label>
          <label className="cloud-scan-field">
            <span>Suggested source path / bucket / API / table</span>
            <input
              className="orch-input"
              value={detectedSourcePath}
              onChange={(e) => {
                setFolderPath(e.target.value);
                setSelectedEndpoint(e.target.value);
                setSelectedApiSource("intelligence-scan");
              }}
            />
          </label>
        </div>
      </div>

      <div className="pi-card" style={{ marginTop: 16 }}>
        <div className="pi-card-title">Formats</div>
        <div className="pi-tag-list">
          {(intelligenceData?.file_types || []).map((ft) => (
            <span key={ft} className="pi-tag active">
              {ft}
            </span>
          ))}
          {(!intelligenceData?.file_types ||
            intelligenceData.file_types.length === 0) && (
            <span className="pi-tag inactive">None Detected</span>
          )}
        </div>
      </div>

      <div className="pi-card" style={{ marginTop: 16 }}>
        <div className="pi-card-title">Detected Ingestion Types</div>
        <div className="pi-tag-list">
          {(intelligenceData?.ingestion_types || []).map((mode) => (
            <span key={mode} className="pi-tag active">
              {mode.replace(/_/g, " ")}
            </span>
          ))}
          {(!intelligenceData?.ingestion_types ||
            intelligenceData.ingestion_types.length === 0) && (
            <span className="pi-tag inactive">None Detected</span>
          )}
        </div>
      </div>

      <div className="step-footer">
        {saveGeneratedError && (
          <div className="panel-error-alert" style={{ marginRight: "auto" }}>
            {saveGeneratedError}
          </div>
        )}
        {configPersisted && (
          <div className="config-chip" style={{ marginRight: "auto" }}>
            <strong>Config:</strong> Saved
          </div>
        )}
        <button
          className="orch-btn primary step-next-btn"
          disabled={
            !detectedSourcePath ||
            savingGeneratedConfig ||
            intelligenceData?.is_fallback ||
            intelligenceData?.scan_status === "failed" ||
            intelligenceData?.auth_mode === "none" ||
            intelligenceData?.pipeline_capabilities?.scan_mode === "mock"
          }
          onClick={saveIntelligenceConfigAndContinue}
        >
          {savingGeneratedConfig
            ? "Saving Config..."
            : "Continue to Configuration →"}
        </button>
      </div>
    </div>
  );

  const renderIntelligenceSuggestions = () => {
    const suggestion = getIntelligenceSuggestion();
    if (!suggestion) return null;
    const registeredSuggestionSource = findRegisteredSuggestionSource(suggestion);
    const isApplied =
      !!registeredSuggestionSource &&
      selectedApiSource === registeredSuggestionSource.id &&
      selectedEndpoint === suggestion.path;
    return (
      <div
        style={{
          marginBottom: 20,
          padding: 14,
          background: "rgba(16, 185, 129, 0.08)",
          border: "1px solid rgba(16, 185, 129, 0.2)",
          borderRadius: 10,
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            color: "#047857",
            fontSize: 13,
            fontWeight: 800,
            marginBottom: 10,
          }}
        >
          <FiZap size={18} />
          <span>Intelligence Suggestions</span>
          <span
            className={`pi-tag ${isRealIntelligenceScan ? "active" : "inactive"}`}
            style={{ marginLeft: "auto" }}
          >
            {isRealIntelligenceScan ? "Real Scan" : "Preview Only"}
          </span>
        </div>
        <div
          className="source-card selected"
          style={{ cursor: "default", borderStyle: "solid", marginBottom: 0 }}
        >
          <div className="source-info" style={{ flex: 1 }}>
            <div className="source-name">{suggestion.title}</div>
            <div className="source-url">
              {suggestion.path || "No source path detected yet"}
            </div>
            <div
              style={{
                display: "flex",
                gap: 8,
                flexWrap: "wrap",
                marginTop: 8,
              }}
            >
              {(intelligenceData?.file_types || []).map((ft) => (
                <span key={ft} className="pi-tag active">
                  {ft}
                </span>
              ))}
              {(intelligenceData?.ingestion_types || [])
                .slice(0, 4)
                .map((mode) => (
                  <span key={mode} className="pi-tag active">
                    {String(mode).replace(/_/g, " ")}
                  </span>
                ))}
              {suggestion.meta.map((item) => (
                <span key={item} className="pi-tag inactive">
                  {item}
                </span>
              ))}
            </div>
          </div>
          <div className="source-actions">
            <button
              className="orch-btn tiny"
              onClick={applyIntelligenceSuggestion}
            >
              {isApplied ? "Applied" : (registeredSuggestionSource ? "Add to Sources" : "Register Source First")}
            </button>
          </div>
        </div>
      </div>
    );
  };

  return (
    <motion.div
      key="step2"
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
      exit={{ opacity: 0, x: -20 }}
      className="orch-step-panel"
    >
      <div
        className="step-header"
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 24,
          paddingBottom: 24,
          borderBottom: "1px solid rgba(0,0,0,0.05)",
        }}
      >
        <div style={{ flex: 1 }}>
          <h2
            className="step-title"
            style={{
              margin: 0,
              fontSize: 24,
              fontWeight: 900,
              background: "linear-gradient(90deg, var(--text1), var(--text2))",
              WebkitBackgroundClip: "text",
              WebkitTextFillColor: "transparent",
            }}
          >
            Data Sources — {selectedClient}
          </h2>
          <p
            className="step-sub"
            style={{
              margin: "4px 0 0",
              opacity: 0.8,
              fontSize: 13,
              fontWeight: 500,
            }}
          >
            Choose an existing source or endpoint to begin.
          </p>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div
            className="source-tabs"
            style={{
              display: "flex",
              gap: 4,
              background: "var(--surface2)",
              padding: 4,
              borderRadius: 14,
            }}
          >
            {sourceTabs.map((t) => (
              <button
                key={t.id}
                className={`source-tab-btn ${activeTab === t.id ? "active" : ""}`}
                disabled={!isTabSupported(t.id)}
                onClick={() => isTabSupported(t.id) && setActiveTab(t.id)}
                style={{
                  padding: "8px 16px",
                  borderRadius: 11,
                  border: "none",
                  background: "transparent",
                  color: activeTab === t.id ? t.color : "var(--text3)",
                  fontWeight: 700,
                  cursor: isTabSupported(t.id) ? "pointer" : "not-allowed",
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  fontSize: 12,
                  transition: "all 0.3s",
                  position: "relative",
                  opacity: isTabSupported(t.id) ? 1 : 0.45,
                }}
              >
                {activeTab === t.id && (
                  <motion.div
                    layoutId="active-source-pill"
                    className="active-tab-indicator"
                    style={{
                      position: "absolute",
                      inset: 0,
                      background: "#fff",
                      borderRadius: 11,
                      boxShadow: "0 4px 12px rgba(0,0,0,0.08)",
                      zIndex: 2,
                    }}
                    transition={{ type: "spring", bounce: 0.2, duration: 0.4 }}
                  />
                )}
                <div
                  style={{
                    position: "relative",
                    zIndex: 3,
                    display: "flex",
                    alignItems: "center",
                    gap: 8,
                  }}
                >
                  <span style={{ fontSize: 16 }}>{t.icon}</span>
                  {t.label}
                  {t.id === "LOCAL" && localFiles.length > 0 && (
                    <span className="tab-badge" style={{ background: t.color }}>
                      {localFiles.length}
                    </span>
                  )}
                  {t.id === "API" && apiSources.length > 0 && (
                    <span className="tab-badge" style={{ background: t.color }}>
                      {apiSources.length}
                    </span>
                  )}
                  {t.id === "S3" && s3Sources.length > 0 && (
                    <span className="tab-badge" style={{ background: t.color }}>
                      {s3Sources.length}
                    </span>
                  )}
                  {t.id === "ADLS" && adlsSources.length > 0 && (
                    <span className="tab-badge" style={{ background: t.color }}>
                      {adlsSources.length}
                    </span>
                  )}
                </div>
              </button>
            ))}
          </div>

          <div
            className="header-logo-divider"
            style={{
              width: 1,
              height: 24,
              background: "rgba(0,0,0,0.1)",
              marginLeft: 8,
            }}
          />
          <img
            src={logo}
            alt="Agilisium"
            style={{ height: 28, objectFit: "contain" }}
          />
        </div>
      </div>

      <div className="step-body">
        {renderIntelligenceSuggestions()}
        <div className="tab-content" style={{ minHeight: 300 }}>
          {/* API Sources Tab */}
          {activeTab === "API" && (
            <div className="source-section animate-in">
              <div className="source-card" style={{ marginBottom: 12 }}>
                <div className="source-info" style={{ flex: 1 }}>
                  <div className="source-name">Add REST API Endpoint</div>
                  <input
                    className="orch-input"
                    value={apiEndpointDraft}
                    onChange={(e) => setApiEndpointDraft(e.target.value)}
                    placeholder="/users or users"
                    style={{ marginTop: 8 }}
                  />
                </div>
                <div className="source-actions">
                  <button
                    className="orch-btn tiny"
                    disabled={!apiEndpointDraft.trim()}
                    onClick={() => {
                      const endpoint = apiEndpointDraft.trim();
                      if (!endpoint) return;
                      onManualSourceSelected?.();
                      setApiEndpoints((prev) => [...new Set([...prev, endpoint])]);
                      setApiEndpointDraft("");
                    }}
                  >
                    Add Endpoint
                  </button>
                </div>
              </div>
              {apiEndpoints.length > 0 && (
                <div className="pi-tag-list" style={{ marginBottom: 12 }}>
                  {apiEndpoints.map((endpoint) => (
                    <span key={endpoint} className="pi-tag active">
                      {endpoint}
                      <button
                        type="button"
                        className="orch-btn ghost tiny"
                        style={{ marginLeft: 8, padding: "0 6px" }}
                        onClick={() => setApiEndpoints((prev) => prev.filter((item) => item !== endpoint))}
                      >
                        Remove
                      </button>
                    </span>
                  ))}
                </div>
              )}
              <div className="source-list">
                {apiSourcesLoading ? (
                  [1, 2].map((i) => (
                    <div
                      key={i}
                      className="skeleton"
                      style={{ height: 72, borderRadius: 14, marginBottom: 12 }}
                    />
                  ))
                ) : apiSources.length > 0 ? (
                  apiSources.map((s) => (
                    <div
                      key={s.id}
                      className={`source-card ${(s.endpoints || []).some((ep) => apiEndpoints.includes(ep)) ? "selected" : ""}`}
                      onClick={() => {
                        setSelectedApiSource(s.id);
                      }}
                    >
                      <div className="source-info" style={{ flex: 1 }}>
                        <div className="source-name">{s.source_name}</div>
                        <div className="source-url">{s.base_url}</div>
                      </div>
                      <div
                        className="source-actions"
                        onClick={(e) => e.stopPropagation()}
                        style={{ minWidth: 220 }}
                      >
                        <FluentSelect
                          multi
                          style={{ minWidth: 220 }}
                          value={(s.endpoints || []).filter((ep) => apiEndpoints.includes(ep))}
                          placeholder="Select endpoints..."
                          onChange={(e) => {
                            const vals = e.target.value;
                            onManualSourceSelected?.();
                            const otherEndpoints = apiEndpoints.filter((ep) => !(s.endpoints || []).includes(ep));
                            setApiEndpoints([...new Set([...otherEndpoints, ...vals])]);
                            setSelectedApiSource(s.id);
                          }}
                          options={(s.endpoints || []).map((ep) => ({
                            value: ep,
                            label: ep,
                          }))}
                        />
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="empty-source">
                    No API sources registered for {selectedClient}.
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Local Tab */}
          {activeTab === "LOCAL" && (
            <div className="source-section animate-in">
              <div className="local-layout">
                <div
                  className="source-card upload-trigger"
                  onClick={() => setShowUploadModal(true)}
                >
                  <div className="source-info">
                    <div className="source-name">Upload New File</div>
                    <div className="source-url">
                      Target: Raw/{selectedClient}/...
                    </div>
                  </div>
                  <button className="orch-btn primary tiny">Upload Now</button>
                </div>

                <div className="local-list" style={{ marginTop: 20 }}>
                  <div
                    className="sub-title"
                    style={{
                      fontSize: 13,
                      color: "var(--text2)",
                      marginBottom: 12,
                      fontWeight: 700,
                    }}
                  >
                    Previously Uploaded Datasets
                  </div>
                  {localFilesLoading ? (
                    [1, 2, 3].map((i) => (
                      <div
                        key={i}
                        className="skeleton"
                        style={{
                          height: 50,
                          borderRadius: 10,
                          marginBottom: 8,
                        }}
                      />
                    ))
                  ) : localFiles.length === 0 ? (
                    <div className="empty-local">
                      No previous uploads found.
                    </div>
                  ) : (
                    localFiles.map((file) => (
                        <div
                        key={file.dataset_id}
                        className={`local-file-card ${selectedLocalFiles.includes(file.dataset_id) ? "selected" : ""}`}
                        onClick={() => toggleLocalFile(file)}
                      >
                        <div className="file-icon">
                          <FiFile size={16} />
                        </div>
                        <div className="file-info">
                          <div className="file-name">{file.display_name || file.source_object}</div>
                          <div className="file-meta">
                            <span>{file.file_format}</span> •{" "}
                            <span>
                              {new Date(file.created_at).toLocaleDateString()}
                            </span>
                          </div>
                        </div>
                        {selectedLocalFiles.includes(file.dataset_id) && (
                          <div className="file-check">
                            <FiCheck size={14} />
                          </div>
                        )}
                      </div>
                    ))
                  )}
                </div>
              </div>
            </div>
          )}

          {/* AWS S3 Tab */}
          {activeTab === "S3" && (
            <div className="source-section animate-in">
              <div className="source-list">
                {s3Sources.length > 0 ? (
                  s3Sources.map((s) => (
                    <div
                      key={s.id}
                      className={`source-card ${selectedS3Path.startsWith(`s3://${s.bucket_name}`) ? "selected" : ""}`}
                      onClick={() => {
                        setSelectedApiSource(s.id);
                      }}
                    >
                      <div className="source-info" style={{ flex: 1 }}>
                        <div className="source-name">
                          {s.source_name} (AWS S3)
                        </div>
                        <div className="source-url">{s.bucket_name}</div>
                        {selectedS3Path.startsWith(`s3://${s.bucket_name}`) && (
                          <div
                            className="selected-path-msg"
                            style={{
                              fontSize: 11,
                              color: "var(--blue)",
                              marginTop: 4,
                              fontWeight: 700,
                            }}
                          >
                            📁 Selected: {selectedEndpoint}
                          </div>
                        )}
                      </div>
                      <div className="source-actions">
                        <button
                          className="orch-btn tiny"
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedApiSource(s.id);
                            const basePath = `s3://${s.bucket_name}`;
                            openExplorer(basePath, "pick", (path) => {
                              onManualSourceSelected?.();
                              setSelectedS3Path(path);
                            });
                          }}
                        >
                          {selectedS3Path.startsWith(`s3://${s.bucket_name}`)
                            ? "Change Folder"
                            : "Browse Storage"}
                        </button>
                      </div>
                    </div>
                  ))
                ) : (
                  <div
                    className="empty-state"
                    style={{ textAlign: "center", padding: "40px 0" }}
                  >
                    <FiFolder
                      size={48}
                      style={{ opacity: 0.2, marginBottom: 16 }}
                    />
                    <div style={{ fontWeight: 600, color: "var(--text2)" }}>
                      No S3 Buckets Connected
                    </div>
                    <div
                      style={{
                        fontSize: 12,
                        color: "var(--text3)",
                        marginTop: 4,
                      }}
                    >
                      Register a new S3 source in Step 1 to see it here.
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Azure ADLS Tab */}
          {activeTab === "ADLS" && (
            <div className="source-section animate-in">
              <div className="source-list">
                {adlsSources.length > 0 ? (
                  adlsSources.map((s) => (
                    <div
                      key={s.id}
                      className={`source-card ${selectedADLSPath.startsWith(`az://${s.azure_account}/${s.azure_container}`) ? "selected" : ""}`}
                      onClick={() => {
                        setSelectedApiSource(s.id);
                      }}
                    >
                      <div className="source-info" style={{ flex: 1 }}>
                        <div className="source-name">
                          {s.source_name} (Azure ADLS)
                        </div>
                        <div className="source-url">
                          {s.azure_account}/{s.azure_container}
                        </div>
                        {selectedADLSPath.startsWith(`az://${s.azure_account}/${s.azure_container}`) && (
                          <div
                            className="selected-path-msg"
                            style={{
                              fontSize: 11,
                              color: "var(--blue)",
                              marginTop: 4,
                              fontWeight: 700,
                            }}
                          >
                            📁 Selected: {selectedEndpoint}
                          </div>
                        )}
                      </div>
                      <div className="source-actions">
                        <button
                          className="orch-btn tiny"
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedApiSource(s.id);
                            const basePath = `az://${s.azure_account}/${s.azure_container}`;
                            openExplorer(basePath, "pick", (path) => {
                              onManualSourceSelected?.();
                              setSelectedADLSPath(path);
                            });
                          }}
                        >
                          {selectedADLSPath.startsWith(`az://${s.azure_account}/${s.azure_container}`)
                            ? "Change Folder"
                            : "Browse Storage"}
                        </button>
                      </div>
                    </div>
                  ))
                ) : (
                  <div
                    className="empty-state"
                    style={{ textAlign: "center", padding: "40px 0" }}
                  >
                    <FiChevronRight
                      size={48}
                      style={{ opacity: 0.2, marginBottom: 16 }}
                    />
                    <div style={{ fontWeight: 600, color: "var(--text2)" }}>
                      No ADLS Containers Connected
                    </div>
                    <div
                      style={{
                        fontSize: 12,
                        color: "var(--text3)",
                        marginTop: 4,
                      }}
                    >
                      Register a new ADLS source in Step 1 to see it here.
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}



        </div>

        {/* Continue */}
        <div className="step-footer">
          {saveGeneratedError && (
            <div className="panel-error-alert" style={{ marginRight: "auto" }}>
              {saveGeneratedError}
            </div>
          )}
          {configPersisted && (
            <div className="config-chip" style={{ marginRight: "auto" }}>
              <strong>Config:</strong> Saved
            </div>
          )}
          <button
            className="orch-btn primary step-next-btn"
            disabled={!canContinue || savingGeneratedConfig}
            onClick={
              intelligenceData ? saveIntelligenceConfigAndContinue : onNext
            }
          >
            Continue to Configuration →
          </button>
        </div>
      </div>
    </motion.div>
  );
}
