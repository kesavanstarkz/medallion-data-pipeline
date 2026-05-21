import { useState } from "react";
import {
    FiChevronDown,
    FiChevronRight,
    FiCheck,
    FiAlertCircle,
} from "react-icons/fi";
import { motion, AnimatePresence } from "framer-motion";
import StepConfig from "./StepConfig";
import StepDQ from "./StepDQ";
import "../../pages/orchestration.css";

export default function StepSourceConfig({
    sourceType,
    folderPath,
    selectedClient,
    selectedApiSource,
    setSelectedApiSource,
    selectedEndpoint,
    setSelectedEndpoint,
    setFolderPath,
    datasets,
    fetchDatasets,
    call,
    toast,
    intelligenceData,
    configPersisted,
    setConfigPersisted,
    syncMasterConfig,
    clientSourceTypes,
    editingConfigDataset,
    setEditingConfigDataset,
    editingConfigColumns,
    editingConfigLoading,
    selectedDqDataset,
    setSelectedDqDataset,
    showDQPanel,
    setShowDQPanel,
    loadDqConfig,
    setPendingDqDataset,
    setShowModeModal,
    dqError,
    setDqError,
    dqLoading,
    isSuggesting,
    editingRuleDrafts,
    setEditingRuleDrafts,
    toggleColumnActive,
    changeColumnSeverity,
    saveColumnRule,
    saveDqConfig,
    editingConfigSaving,
    formatDatasetLabel,
    onNext,
    onBack,
    runOrchestration,
    isOrchestrating,
    datasetsLoading,
    selectedPlatform,
    setMasterConfig,
    fabricMode,
    apiSources,
    s3Sources,
    adlsSources,
    apiSourcesLoading,
    openExplorer,
    extractedFabricData,
    setExtractedFabricData,
    clientSourceTypes: clientSourceTypes2,
    onManualSourceSelected,
    refreshTrigger,
    showUploadModal,
    setShowUploadModal,
    extractedFabricData: fabricData,
    setPipelineDeployed,
    pipelineDeployed,
}) {
    const [expandedSections, setExpandedSections] = useState({
        config: true,
        dq: true,
    });

    const toggleSection = (section) => {
        setExpandedSections((prev) => ({
            ...prev,
            [section]: !prev[section],
        }));
    };

    return (
        <motion.div
            key="step-source-config"
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
            exit={{ opacity: 0, x: -20 }}
            className="orch-step-panel"
        >
            <div className="step-header-responsive">
                <div className="step-header-text">
                    <h2 className="step-title">
                        Configure Source & Data Quality
                    </h2>
                    <p className="step-sub">
                        Set up your source connection, configure schema mapping,
                        and define data quality rules.
                    </p>
                </div>
            </div>

            <div
                className="step-body"
                style={{ overflowY: "auto", maxHeight: "calc(100vh - 300px)" }}
            >
                {/* SOURCE CONFIGURATION SECTION */}
                <div className="collapsible-section">
                    <button
                        className="collapsible-header"
                        onClick={() => toggleSection("config")}
                        style={{
                            display: "flex",
                            alignItems: "center",
                            gap: "12px",
                            width: "100%",
                            padding: "12px 16px",
                            border: "1px solid var(--border)",
                            borderRadius: "8px",
                            background: "var(--surface)",
                            cursor: "pointer",
                            fontSize: "16px",
                            fontWeight: "600",
                            color: "var(--text)",
                            transition: "all 0.2s",
                        }}
                    >
                        {expandedSections.config ? (
                            <FiChevronDown size={20} />
                        ) : (
                            <FiChevronRight size={20} />
                        )}
                        <span>Source Configuration</span>
                        {configPersisted && (
                            <FiCheck
                                size={16}
                                color="var(--green)"
                                style={{ marginLeft: "auto" }}
                            />
                        )}
                    </button>

                    <AnimatePresence>
                        {expandedSections.config && (
                            <motion.div
                                initial={{ opacity: 0, height: 0 }}
                                animate={{ opacity: 1, height: "auto" }}
                                exit={{ opacity: 0, height: 0 }}
                                transition={{ duration: 0.2 }}
                                style={{ overflow: "hidden" }}
                            >
                                <div
                                    style={{
                                        padding: "16px",
                                        borderTop: "1px solid var(--border)",
                                        background: "var(--bg)",
                                    }}
                                >
                                    <StepConfig
                                        selectedClient={selectedClient}
                                        folderPath={folderPath}
                                        sourceType={sourceType}
                                        call={call}
                                        toast={toast}
                                        onNext={() => {}} // Don't navigate, just save
                                        syncMasterConfig={syncMasterConfig}
                                        intelligenceData={intelligenceData}
                                        fabricMode={fabricMode}
                                        setConfigPersisted={setConfigPersisted}
                                        selectedPlatform={selectedPlatform}
                                        setMasterConfig={setMasterConfig}
                                    />
                                </div>
                            </motion.div>
                        )}
                    </AnimatePresence>
                </div>

                <div style={{ height: "16px" }} />

                {/* DATA QUALITY RULES SECTION */}
                <div className="collapsible-section">
                    <button
                        className="collapsible-header"
                        onClick={() => toggleSection("dq")}
                        style={{
                            display: "flex",
                            alignItems: "center",
                            gap: "12px",
                            width: "100%",
                            padding: "12px 16px",
                            border: "1px solid var(--border)",
                            borderRadius: "8px",
                            background: "var(--surface)",
                            cursor: "pointer",
                            fontSize: "16px",
                            fontWeight: "600",
                            color: "var(--text)",
                            transition: "all 0.2s",
                        }}
                    >
                        {expandedSections.dq ? (
                            <FiChevronDown size={20} />
                        ) : (
                            <FiChevronRight size={20} />
                        )}
                        <span>Data Quality Rules</span>
                        {datasets?.length > 0 && (
                            <span
                                style={{
                                    marginLeft: "auto",
                                    fontSize: "13px",
                                    color: "var(--text-secondary)",
                                }}
                            >
                                {datasets.length} datasets
                            </span>
                        )}
                    </button>

                    <AnimatePresence>
                        {expandedSections.dq && (
                            <motion.div
                                initial={{ opacity: 0, height: 0 }}
                                animate={{ opacity: 1, height: "auto" }}
                                exit={{ opacity: 0, height: 0 }}
                                transition={{ duration: 0.2 }}
                                style={{ overflow: "hidden" }}
                            >
                                <div
                                    style={{
                                        padding: "16px",
                                        borderTop: "1px solid var(--border)",
                                        background: "var(--bg)",
                                    }}
                                >
                                    <StepDQ
                                        selectedClient={selectedClient}
                                        sourceType={sourceType}
                                        folderPath={folderPath}
                                        datasets={datasets}
                                        fetchDatasets={fetchDatasets}
                                        editingConfigDataset={
                                            editingConfigDataset
                                        }
                                        setEditingConfigDataset={
                                            setEditingConfigDataset
                                        }
                                        editingConfigColumns={
                                            editingConfigColumns
                                        }
                                        editingConfigLoading={
                                            editingConfigLoading
                                        }
                                        selectedDqDataset={selectedDqDataset}
                                        setSelectedDqDataset={
                                            setSelectedDqDataset
                                        }
                                        showDQPanel={showDQPanel}
                                        setShowDQPanel={setShowDQPanel}
                                        loadDqConfig={loadDqConfig}
                                        setPendingDqDataset={
                                            setPendingDqDataset
                                        }
                                        setShowModeModal={setShowModeModal}
                                        dqError={dqError}
                                        setDqError={setDqError}
                                        dqLoading={dqLoading}
                                        isSuggesting={isSuggesting}
                                        editingRuleDrafts={editingRuleDrafts}
                                        setEditingRuleDrafts={
                                            setEditingRuleDrafts
                                        }
                                        toggleColumnActive={toggleColumnActive}
                                        changeColumnSeverity={
                                            changeColumnSeverity
                                        }
                                        saveColumnRule={saveColumnRule}
                                        saveDqConfig={saveDqConfig}
                                        editingConfigSaving={
                                            editingConfigSaving
                                        }
                                        formatDatasetLabel={formatDatasetLabel}
                                        onNext={() => {}} // Don't navigate
                                        onBack={() => {}} // Don't navigate
                                        onRunOrchestration={() => {}} // Don't run here
                                        isOrchestrating={isOrchestrating}
                                        datasetsLoading={datasetsLoading}
                                    />
                                </div>
                            </motion.div>
                        )}
                    </AnimatePresence>
                </div>
            </div>

            <div className="step-footer">
                <button className="btn btn-outline" onClick={onBack}>
                    Back
                </button>
                <button
                    className="btn btn-primary"
                    onClick={onNext}
                    disabled={!configPersisted}
                >
                    Continue to Deployment
                    <FiChevronRight size={18} />
                </button>
            </div>
        </motion.div>
    );
}
