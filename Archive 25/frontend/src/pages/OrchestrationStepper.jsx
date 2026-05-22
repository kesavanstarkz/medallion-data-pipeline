import { useEffect, useState, useRef, useCallback } from "react";
import { createPortal } from "react-dom";
import { useApi, apiUrl } from "../hooks/useApi";
import { useToast } from "../hooks/useToast";
import {
    FiX,
    FiCheck,
    FiZap,
    FiEdit2,
    FiBarChart2,
    FiClipboard,
    FiSearch,
    FiSettings,
    FiActivity,
    FiFolder,
    FiFileText,
    FiDownload,
    FiCornerUpLeft,
    FiClock,
} from "react-icons/fi";
import { motion, AnimatePresence } from "framer-motion";
import { useLocation, useNavigate } from "react-router-dom";
import FluentSelect from "../components/FluentSelect";
import StepPlatform from "../components/orchestration/StepPlatform";
import StepClient from "../components/orchestration/StepClient";
import StepSourceConfig from "../components/orchestration/StepSourceConfig";
import StepProgress from "../components/orchestration/StepProgress";
import StepReviewConfirm from "../components/orchestration/StepReviewConfirm";
import StepDeployment from "../components/orchestration/StepDeployment";
import PipelineIntelligence from "../components/PipelineIntelligence";
import HistoryView from "./HistoryView";
import {
    normalizeFabricWorkspace,
    normalizeFabricPipeline,
    isFabricGuid,
} from "../utils/fabricContext";
import "./orchestration.css";

const STEPS_CONFIG = [
    { id: "platform", num: 0, label: "Platform", path: "/platform" },
    { id: "client", num: 1, label: "Client", path: "/client" },
    {
        id: "intelligence",
        num: 2,
        label: "Intelligence",
        path: "/intelligence",
    },
    {
        id: "source-config",
        num: 3,
        label: "Source Config",
        path: "/source-config",
    },
    { id: "deployment", num: 4, label: "Deployment", path: "/deployment" },
    { id: "review", num: 5, label: "Review", path: "/review" },
    { id: "execution", num: 6, label: "Execution", path: "/execution" },
    { id: "history", num: 7, label: "History / Monitoring", path: "/history" },
];

function parseStrictJson(text) {
    const safeText = String(text || "")
        .replace(/\bNaN\b/g, "null")
        .replace(/\b-?Infinity\b/g, "null")
        .replace(/\bundefined\b/g, "null");
    return JSON.parse(safeText);
}

export default function OrchestrationStepper({ hideHeader = false }) {
    const toast = useToast();
    const nav = useNavigate();
    const location = useLocation();
    const { call } = useApi();

    const [step, setStep] = useState(() => {
        const path = location.pathname.split("/").pop();
        const found = STEPS_CONFIG.find((s) => s.id === path);
        return found ? found.num : 0;
    });

    const [selectedPlatform, setSelectedPlatform] = useState(
        localStorage.getItem("selected_platform") || "",
    );
    const [clients, setClients] = useState([]);
    const [selectedClient, setSelectedClient] = useState(
        localStorage.getItem("client_name") || "",
    );

    // Navigation Sync
    useEffect(() => {
        const path = location.pathname.split("/").pop();
        const found = STEPS_CONFIG.find((s) => s.id === path);
        if (found && found.num !== step) {
            setStep(found.num);
        }
    }, [location.pathname, step]);

    const goToStep = (s) => {
        const config = STEPS_CONFIG.find((c) => c.num === s);
        if (config) {
            setStep(s);
            nav(`/orchestration-beta/${config.id}`);
        }
    };

    const [sourceType, setSourceType] = useState(
        sessionStorage.getItem("wizard_source_type") || "",
    );
    const [folderPath, setFolderPath] = useState("");
    const [apiSources, setApiSources] = useState([]);
    const [s3Sources, setS3Sources] = useState([]);
    const [adlsSources, setAdlsSources] = useState([]);
    const [selectedApiSource, setSelectedApiSource] = useState(null);

    const [selectedEndpoint, setSelectedEndpoint] = useState("");
    const [clientLoading, setClientLoading] = useState(false);
    const [apiSourcesLoading, setApiSourcesLoading] = useState(false);
    const [orchestrateResp, setOrchestrateResp] = useState(null);
    const [datasets, setDatasets] = useState([]);
    const [showDQPanel, setShowDQPanel] = useState(false);
    const [editingConfigDataset, setEditingConfigDataset] = useState(null);
    const [editingConfigColumns, setEditingConfigColumns] = useState([]);
    const [editingConfigLoading, setEditingConfigLoading] = useState(false);
    const [editingConfigSaving, setEditingConfigSaving] = useState(false);
    const [editingRuleDrafts, setEditingRuleDrafts] = useState({});
    const [showModeModal, setShowModeModal] = useState(false);
    const [pendingDqDataset, setPendingDqDataset] = useState(null);
    const [dqError, setDqError] = useState(null);
    const [dqLoading, setDqLoading] = useState(false);
    const [isOrchestrating, setIsOrchestrating] = useState(false);
    const [customDqPrompt, setCustomDqPrompt] = useState("");
    const [showCustomPrompt, setShowCustomPrompt] = useState(false);
    const [selectedDqDataset, setSelectedDqDataset] = useState(null);
    const [isSuggesting, setIsSuggesting] = useState(false);
    const [localRefreshTrigger, setLocalRefreshTrigger] = useState(0);
    const [datasetsLoading, setDatasetsLoading] = useState(false);
    const [intelligenceData, setIntelligenceData] = useState(null);
    const [configPersisted, setConfigPersisted] = useState(false);
    const [clientSourceTypes, setClientSourceTypes] = useState([]);
    const [extractedFabricData, setExtractedFabricData] = useState(null);
    const [pipelineDeployed, setPipelineDeployed] = useState(false);

    // Fabric/Orchestration State
    const [deploymentStrategy, setDeploymentStrategy] = useState(
        sessionStorage.getItem("wizard_deployment_strategy") || null,
    );
    const [deploymentPackage, setDeploymentPackage] = useState(null);
    const [selectedWorkspace, setSelectedWorkspace] = useState(() => {
        try {
            const raw = JSON.parse(
                sessionStorage.getItem("wizard_fabric_workspace") || "null",
            );
            return normalizeFabricWorkspace(raw);
        } catch {
            return null;
        }
    });
    const [selectedPipeline, setSelectedPipeline] = useState(() => {
        try {
            const raw = JSON.parse(
                sessionStorage.getItem("wizard_fabric_pipeline") || "null",
            );
            const ws = normalizeFabricWorkspace(
                JSON.parse(
                    sessionStorage.getItem("wizard_fabric_workspace") || "null",
                ),
            );
            return normalizeFabricPipeline(
                raw,
                ws?.workspace_id || ws?.id,
            );
        } catch {
            return null;
        }
    });
    const [fabricAccessToken, setFabricAccessToken] = useState(null);
    const [masterConfig, setMasterConfig] = useState(null);
    const [targets, setTargets] = useState([]);
    const [selectedTarget, setSelectedTarget] = useState(() => {
        try {
            return JSON.parse(sessionStorage.getItem("wizard_selected_target") || "null");
        } catch {
            return null;
        }
    });
    const selectedWorkspaceRef = useRef(null);

    // Source form state
    const [sourceForm, setSourceForm] = useState({
        client_name: "",
        source_name: "",
        source_type: "API",
        base_url: "",
        auth_type: "none",
        auth_token: "",
        api_key_header: "X-Api-Key",
        endpoints: "",
        aws_access_key_id: "",
        aws_secret_access_key: "",
        region: "",
        bucket_name: "",
        azure_account_name: "",
        azure_account_key: "",
        azure_container_name: "",
        fabricMode: "discover",
    });
    const [savingSource, setSavingSource] = useState(false);
    const [testingConnection, setTestingConnection] = useState(false);
    const [connectionVerified, setConnectionVerified] = useState(false);
    const [testResult, setTestResult] = useState(null);

    // File Explorer State
    const [showExplorerModal, setShowExplorerModal] = useState(false);
    const [explorerCurrentPath, setExplorerCurrentPath] = useState("");
    const [explorerItems, setExplorerItems] = useState({
        folders: [],
        files: [],
    });
    const [explorerLoading, setExplorerLoading] = useState(false);
    const [previewData, setPreviewData] = useState(null);
    const [previewLoading, setPreviewLoading] = useState(false);
    const [explorerPurpose, setExplorerPurpose] = useState("preview"); // 'preview' | 'pick'
    const [explorerOnSelect, setExplorerOnSelect] = useState(null);

    // Upload UI state
    const [uploadFiles, setUploadFiles] = useState(null);
    const [uploadLoading, setUploadLoading] = useState(false);
    const [showUploadModal, setShowUploadModal] = useState(false);
    const [isDragOver, setIsDragOver] = useState(false);

    // Animation variants
    const modalVariants = {
        initial: { opacity: 0, scale: 0.95 },
        animate: {
            opacity: 1,
            scale: 1,
            transition: { type: "spring", damping: 25, stiffness: 300 },
        },
        exit: { opacity: 0, scale: 0.95, transition: { duration: 0.2 } },
    };

    const fetchTargets = useCallback(
        async (client) => {
            if (!client) {
                setTargets([]);
                return;
            }
            try {
                const res = await call(
                    `/config/targets/${encodeURIComponent(client)}`,
                );
                setTargets(res || []);
            } catch (e) {
                console.warn("Failed to fetch targets:", e);
            }
        },
        [call],
    );

    const resetSessionState = useCallback(
        ({ clearSourceSelection = false } = {}) => {
            console.log("STEPPER: Resetting session state", {
                clearSourceSelection,
            });
            setIntelligenceData(null);
            setConfigPersisted(false);
            setDatasets([]);
            setOrchestrateResp(null);
            setEditingConfigDataset(null);
            setEditingConfigColumns([]);
            setSelectedDqDataset(null);
            setDqError(null);
            setPipelineDeployed(false);
            setDeploymentStrategy(null);
            setDeploymentPackage(null);
            setSelectedWorkspace(null);
            setSelectedPipeline(null);
            sessionStorage.removeItem("wizard_deployment_strategy");
            sessionStorage.removeItem("wizard_fabric_workspace");
            sessionStorage.removeItem("wizard_fabric_pipeline");

            if (clearSourceSelection) {
                setSelectedApiSource(null);
                setSelectedEndpoint("");
                setFolderPath("");
                setSourceType("");
                sessionStorage.removeItem("wizard_source_type");
                setApiSources([]);
                setS3Sources([]);
                setAdlsSources([]);
            }
            fetchTargets(selectedClient);
        },
        [selectedClient, fetchTargets],
    );

    const resetWorkspaceScopedState = useCallback(() => {
        setIntelligenceData(null);
        setConfigPersisted(false);
        setDatasets([]);
        setOrchestrateResp(null);
        setEditingConfigDataset(null);
        setEditingConfigColumns([]);
        setSelectedDqDataset(null);
        setDqError(null);
        setPipelineDeployed(false);
        setDeploymentStrategy(null);
        setDeploymentPackage(null);
        setExtractedFabricData(null);
        sessionStorage.removeItem("wizard_deployment_strategy");
    }, []);

    const handleWorkspaceSelection = (workspace) => {
        const normalized = normalizeFabricWorkspace(workspace);
        const previousWorkspaceId =
            selectedWorkspaceRef.current?.workspace_id ||
            selectedWorkspaceRef.current?.id ||
            null;
        const nextWorkspaceId =
            normalized?.workspace_id || normalized?.id || null;
        console.log("STEPPER: Workspace selection change", {
            previousWorkspaceId,
            nextWorkspaceId,
            workspace_name: normalized?.workspace_name,
        });

        selectedWorkspaceRef.current = normalized || null;
        setSelectedWorkspace(normalized || null);

        if (previousWorkspaceId !== nextWorkspaceId) {
            setSelectedPipeline(null);
            resetWorkspaceScopedState();
        }
    };

    function handlePipelineSelection(pipeline) {
        const normalized = normalizeFabricPipeline(
            pipeline,
            selectedWorkspaceRef.current?.workspace_id ||
                selectedWorkspaceRef.current?.id,
        );
        const previousPipelineId =
            selectedPipeline?.pipeline_item_id || selectedPipeline?.id || null;
        const nextPipelineId =
            normalized?.pipeline_item_id || normalized?.id || null;
        console.log("STEPPER: Pipeline selection change", {
            workspaceId:
                selectedWorkspaceRef.current?.workspace_id ||
                selectedWorkspaceRef.current?.id ||
                null,
            previousPipelineId,
            nextPipelineId,
            pipeline_name: normalized?.pipeline_name,
            id_is_guid: isFabricGuid(nextPipelineId),
        });
        setSelectedPipeline(normalized || null);
    }

    const fetchClientSourceTypes = useCallback(
        async (client) => {
            try {
                const res = await call(
                    `/api-source/client-source-types?client_name=${encodeURIComponent(client)}`,
                );
                const sourceTypes = res.source_types || [];
                setClientSourceTypes(sourceTypes);
                const preferred =
                    sourceTypes.find((t) => t === "LOCAL") ||
                    sourceTypes.find((t) => t === "REST_API") ||
                    sourceTypes[0];
                if (preferred) {
                    setSourceType(
                        preferred === "AWS"
                            ? "S3"
                            : preferred === "AZURE"
                              ? "ADLS"
                              : preferred === "REST_API"
                                ? "API"
                                : preferred,
                    );
                }
                console.debug("Client source type mapping", {
                    client_name: client,
                    source_types: sourceTypes,
                });
            } catch (e) {
                setClientSourceTypes([]);
                console.warn(
                    "Client source type mapping unavailable:",
                    e?.message || e,
                );
            }
        },
        [call],
    );

    const fetchApiSourcesForClient = useCallback(
        async (client) => {
            setApiSourcesLoading(true);
            try {
                const res = await call(
                    `/api-source/list?client_name=${encodeURIComponent(client)}`,
                );
                const list = (res.configs || []).filter(
                    (s) =>
                        String(s.client_name).toLowerCase() ===
                        String(client).toLowerCase(),
                );
                console.debug("Fetched registered sources", {
                    client_name: client,
                    total: list.length,
                    source_types: list.map((s) => s.source_type),
                });

                const sourceKind = (s) => {
                    const value = String(s.source_type || "API").toUpperCase();
                    if (value === "REST_API") return "API";
                    if (value === "AWS") return "S3";
                    if (value === "AZURE") return "ADLS";
                    return value;
                };
                const apis = list.filter((s) => sourceKind(s) === "API");
                const s3s = list.filter((s) => sourceKind(s) === "S3");
                const adl = list.filter((s) => sourceKind(s) === "ADLS");

                setApiSources(apis);
                setS3Sources(s3s);
                setAdlsSources(adl);

                setSelectedApiSource(null);
                setSelectedEndpoint("");
            } catch (e) {
                setApiSources([]);
                setS3Sources([]);
                setAdlsSources([]);
            } finally {
                setApiSourcesLoading(false);
            }
        },
        [call],
    );

    const fetchClients = useCallback(async () => {
        setClientLoading(true);
        try {
            const res = await call("/config/clients");
            setClients(res.clients || []);
        } catch (e) {
            setClients([]);
            toast("Failed to load clients: " + (e?.message || e), "error");
        } finally {
            setClientLoading(false);
        }
    }, [call, toast]);

    // Upload handler
    async function handleUpload() {
        if (!selectedClient) return toast("Select a client first", "error");
        if (!uploadFiles || uploadFiles.length === 0)
            return toast("Select files to upload", "error");
        setUploadLoading(true);
        try {
            const form = new FormData();
            form.append("client_name", selectedClient);
            for (let i = 0; i < uploadFiles.length; i++) {
                form.append("files", uploadFiles[i], uploadFiles[i].name);
            }
            const resp = await fetch(apiUrl("/upload/ingest"), {
                method: "POST",
                body: form,
            });
            if (!resp.ok) throw new Error(`Upload failed: ${resp.status}`);
            const data = await resp.json();
            if (data.status && data.status.toUpperCase() === "SUCCESS") {
                toast(`Uploaded ${data.uploaded || 0} files`, "success");
                resetSessionState({ clearSourceSelection: true });
                setSourceType("LOCAL");
                const uploadedIds = (data.results || [])
                    .map((r) => r.dataset_id)
                    .filter(Boolean)
                    .join(",");
                const localSelection =
                    uploadedIds || `upload/${selectedClient}`;
                setFolderPath(localSelection);
                setSelectedEndpoint(localSelection);
                setSelectedApiSource("local-multi");
                setShowUploadModal(false);
                setUploadFiles(null);
                // Refresh master list and sources list to show the new landing zone
                fetchClients();
                fetchApiSourcesForClient(selectedClient);
                fetchClientSourceTypes(selectedClient);
                setLocalRefreshTrigger((prev) => prev + 1);
                goToStep(2);
            } else {
                toast("Upload completed with warnings", "warning");
            }
        } catch (e) {
            toast("Upload failed: " + (e?.message || e), "error");
        } finally {
            setUploadLoading(false);
        }
    }

    function handleFileInputChange(e) {
        setUploadFiles(e?.target?.files ? Array.from(e.target.files) : []);
    }
    function handleDrop(e) {
        e.preventDefault();
        const files = e?.dataTransfer?.files
            ? Array.from(e.dataTransfer.files)
            : [];
        if (files.length === 0) return;
        setUploadFiles((prev) => [
            ...(Array.isArray(prev) ? prev : []),
            ...files,
        ]);
        setIsDragOver(false);
    }
    function handleDragOver(e) {
        e.preventDefault();
    }
    function handleDragEnter(e) {
        e.preventDefault();
        setIsDragOver(true);
    }
    function handleDragLeave(e) {
        e.preventDefault();
        setIsDragOver(false);
    }
    const removeFile = (idx) => {
        setUploadFiles((prev) =>
            Array.isArray(prev) ? prev.filter((_, i) => i !== idx) : prev,
        );
    };

    const EXECUTION_NODE_PRIORITY = {
        discover: 0,
        land: 0,
        report_ingestion: 0,
        configure: 1,
        prepare_dq: 1,
        report: 2,
        transform: 3,
    };

    function datasetDisplayName(row = {}, pipelineResults = []) {
        const matched = pipelineResults.find(
            (item) => item.dataset_id && item.dataset_id === row.dataset_id,
        );
        return (
            row.dataset_name ||
            matched?.dataset_name ||
            row.source_object ||
            row.file_name ||
            matched?.source_object ||
            matched?.file_name ||
            "Dataset"
        );
    }

    function mergeProgressByDataset(
        current = [],
        incoming = [],
        pipelineResults = [],
    ) {
        const merged = new Map();
        current.forEach((row) => {
            const displayName = datasetDisplayName(row, pipelineResults);
            merged.set(row.dataset_id, { ...row, dataset_name: displayName });
        });
        incoming.forEach((row) => {
            const existing = merged.get(row.dataset_id) || {};
            const displayName = datasetDisplayName(
                { ...existing, ...row },
                pipelineResults,
            );
            merged.set(row.dataset_id, {
                ...existing,
                ...row,
                dataset_name: displayName,
                steps: {
                    ...(existing.steps || {}),
                    ...(row.steps || {}),
                },
            });
        });
        return Array.from(merged.values());
    }

    function preferExecutionPacket(current, incoming) {
        if (!current) return incoming;
        if (incoming?.completed) return incoming;
        const currentPriority = EXECUTION_NODE_PRIORITY[current.node] ?? -1;
        const incomingPriority = EXECUTION_NODE_PRIORITY[incoming?.node] ?? -1;
        return incomingPriority >= currentPriority ? incoming : current;
    }

    // Orchestration
    async function runOrchestration(payloadFromReview) {
        const query = payloadFromReview?.query || {};

        // 1. Normalize payload before validation
        const normalizedPayload = {
            source_path: query.source_path,
            source_type: query.source_type,
            folder_path: query.folder_path || query.source_path,
            staging_table: query.staging_table || "",
            pipeline_name: query.pipeline_name,
            dataset_id: query.dataset_id,
            bronze_target: query.bronze_target,
            silver_target: query.silver_target,
            client_name: query.client_name,
            file_format: query.file_format,
            load_type: query.load_type,
            platform: query.platform,
            discovery_mode: query.discovery_mode,
            deployment_strategy: query.deployment_strategy,
            workspace_id: query.workspace_id,
            pipeline_id: query.pipeline_id,
            package_name: query.package_name,
        };

        console.log("DEA normalized payload:", normalizedPayload);

        if (!selectedClient && !normalizedPayload.client_name)
            return toast("Select a client first", "error");

        const runningIntelligenceSuggestion =
            selectedApiSource === "intelligence-scan";
        const publicRestApiScan = intelligenceData?.framework === "REST API";

        if (
            runningIntelligenceSuggestion &&
            (!intelligenceData ||
                intelligenceData.is_fallback ||
                intelligenceData.scan_status === "failed" ||
                (!publicRestApiScan && intelligenceData.auth_mode === "none") ||
                intelligenceData.pipeline_capabilities?.scan_mode === "mock")
        ) {
            return toast(
                "Please perform a real scan using credentials before execution.",
                "error",
            );
        }

        if (runningIntelligenceSuggestion && !configPersisted) {
            return toast(
                "Save generated configuration before execution.",
                "error",
            );
        }

        // 2. Validate normalizedPayload only
        if (!normalizedPayload.source_type) {
            toast("Specify source_type", "error");
            return;
        }
        if (!normalizedPayload.source_path) {
            toast("Specify source_path", "error");
            return;
        }
        if (normalizedPayload.platform === "FABRIC") {
            if (!normalizedPayload.workspace_id) {
                toast("Specify workspace_id", "error");
                return;
            }
            if (!normalizedPayload.pipeline_id) {
                toast("Specify pipeline_id", "error");
                return;
            }
        }
        if (!normalizedPayload.client_name) {
            toast("Specify client_name", "error");
            return;
        }

        console.debug("Execution trigger validation passed", normalizedPayload);

        goToStep(6);
        setOrchestrateResp({ progress: [] });
        setIsOrchestrating(true);
        try {
            toast("Running orchestration — streaming progress...", "info");

            const params = new URLSearchParams();
            // 3. DEA API request must use normalizedPayload
            Object.entries(normalizedPayload).forEach(([k, v]) => {
                if (v !== null && v !== undefined && v !== "")
                    params.set(k, String(v));
            });

            // Keep flags
            params.set(
                "require_real_scan",
                runningIntelligenceSuggestion ? "true" : "false",
            );

            const qs = `?${params.toString()}`;
            const response = await fetch(apiUrl(`/orchestrate/run${qs}`), {
                method: "POST",
                headers: { Accept: "application/x-ndjson" },
            });
            if (!response.ok)
                throw new Error(
                    `Orchestration failed with status ${response.status}`,
                );

            const reader = response.body.getReader();
            const decoder = new TextDecoder("utf-8");
            let buffer = "";
            let latestResponse = { progress: [], pipeline_results: [] };
            let preferredNodeResponse = null;
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split("\n");
                buffer = lines.pop();
                for (const line of lines) {
                    if (!line.trim()) continue;
                    try {
                        const data = parseStrictJson(line);
                        if (
                            data.progress &&
                            Array.isArray(data.progress) &&
                            data.progress.length > 0
                        ) {
                            const pipelineResults =
                                data.pipeline_results ||
                                latestResponse.pipeline_results ||
                                [];
                            latestResponse = {
                                ...latestResponse,
                                ...data,
                                pipeline_results: pipelineResults,
                                progress: mergeProgressByDataset(
                                    latestResponse.progress || [],
                                    data.progress,
                                    pipelineResults,
                                ),
                            };
                            preferredNodeResponse = preferExecutionPacket(
                                preferredNodeResponse,
                                latestResponse,
                            );
                            setOrchestrateResp(latestResponse);
                        } else if (
                            data.status ||
                            data.completed ||
                            data.pipeline_results
                        ) {
                            latestResponse = {
                                ...latestResponse,
                                ...data,
                                pipeline_results:
                                    data.pipeline_results ||
                                    latestResponse.pipeline_results ||
                                    [],
                                progress: latestResponse.progress || [],
                            };
                            preferredNodeResponse = preferExecutionPacket(
                                preferredNodeResponse,
                                latestResponse,
                            );
                            setOrchestrateResp(latestResponse);
                        }
                        if (data.completed) {
                            const finalResponse = {
                                ...latestResponse,
                                ...data,
                                progress: data.progress?.length
                                    ? mergeProgressByDataset(
                                          preferredNodeResponse?.progress ||
                                              latestResponse.progress ||
                                              [],
                                          data.progress,
                                          data.pipeline_results || [],
                                      )
                                    : preferredNodeResponse?.progress ||
                                      latestResponse.progress ||
                                      [],
                                pipeline_results:
                                    data.pipeline_results ||
                                    latestResponse.pipeline_results ||
                                    [],
                                refreshKey: Date.now(),
                            };
                            console.log("FINAL RESPONSE:", finalResponse);
                            console.log("STEPS:", finalResponse.progress);
                            setOrchestrateResp(finalResponse);
                            if (data.status === "SUCCESS")
                                toast(
                                    "Orchestration finished successfully",
                                    "success",
                                );
                            else
                                toast(
                                    `Orchestration failed: ${data.error}`,
                                    "error",
                                );
                            try {
                                await fetchDatasets();
                            } catch {}
                        }
                    } catch (e) {
                        console.error("Error parsing stream chunk", e);
                    }
                }
            }
        } catch (e) {
            toast("Orchestration failed: " + (e?.message || e), "error");
        } finally {
            setIsOrchestrating(false);
        }
    }

    // DQ functions
    async function loadDqConfig(datasetId) {
        if (!datasetId) return;
        setEditingConfigDataset(datasetId);
        setEditingConfigLoading(true);
        setDqError(null);
        setIsSuggesting(false); // Reset this for standard loads
        try {
            const res = await call(`/dq/config/${datasetId}`);
            setEditingConfigColumns(res.columns || []);
            setShowDQPanel(true);
        } catch (e) {
            const msg = e?.message || "Failed to load configuration";
            setDqError(msg);
            toast(msg, "error");
        } finally {
            setEditingConfigLoading(false);
        }
    }

    async function saveDqConfig() {
        if (!editingConfigDataset) return toast("No dataset selected", "error");
        setEditingConfigSaving(true);
        try {
            const payload = {
                dataset_id: editingConfigDataset,
                columns: editingConfigColumns.map((c) => ({
                    column_name: c.column_name,
                    expected_data_type: String(
                        c.expected_data_type || "STRING",
                    ).toUpperCase(),
                    dq_rules: (c.dq_rules || [])
                        .map((r) => String(r).toUpperCase())
                        .filter((r) => r.length > 0),
                    rule_value: c.rule_value || null,
                    severity: String(c.severity || "ERROR").toUpperCase(),
                    is_active: !!c.is_active,
                })),
            };
            const res = await call("/dq/configure", "POST", payload);

            if (res && res.columns) {
                toast("Configuration saved successfully ✔", "success");
                setEditingConfigDataset(null);
                setEditingConfigColumns([]);
                setShowDQPanel(false);
            } else if (res && res.detail) {
                toast("Save error: " + res.detail, "error");
            }
        } catch (e) {
            toast("Save failed: " + (e?.message || e), "error");
        } finally {
            setEditingConfigSaving(false);
        }
    }

    async function previewDq(datasetId, mode) {
        if (!datasetId || !mode) return;
        setDqError(null);
        setDqLoading(true);
        setSelectedDqDataset(datasetId);
        setEditingConfigDataset(datasetId);
        setShowDQPanel(true);
        setIsSuggesting(true);
        try {
            toast(
                `Generating ${mode.replace("_", " ")} DQ suggestions...`,
                "info",
            );
            const res = await call("/dq/suggest", "POST", {
                dataset_id: datasetId,
                mode,
                prompt: mode === "custom" ? customDqPrompt : null,
            });
            if (mode === "custom") setCustomDqPrompt("");
            if (res.columns) setEditingConfigColumns(res.columns);
            else {
                const configRes = await call(`/dq/config/${datasetId}`);
                setEditingConfigColumns(configRes.columns || []);
            }
            toast("AI suggested rules applied successfully", "success");
        } catch (e) {
            const msg = e?.message || "Failed to generate suggestions";
            setDqError(msg);
            toast(msg, "error");
        } finally {
            setDqLoading(false);
            setIsSuggesting(false);
        }
    }

    function toggleColumnActive(columnName) {
        setEditingConfigColumns((prev) =>
            prev.map((c) =>
                c.column_name === columnName
                    ? { ...c, is_active: !c.is_active }
                    : c,
            ),
        );
    }
    function changeColumnSeverity(columnName, sev) {
        setEditingConfigColumns((prev) =>
            prev.map((c) =>
                c.column_name === columnName ? { ...c, severity: sev } : c,
            ),
        );
    }
    function saveColumnRule(columnName) {
        const draft = (editingRuleDrafts || {})[columnName];
        if (draft === undefined) return;
        setEditingConfigColumns((prev) =>
            prev.map((c) =>
                c.column_name === columnName
                    ? {
                          ...c,
                          dq_rules: draft
                              .split(",")
                              .map((s) => s.trim())
                              .filter(Boolean),
                      }
                    : c,
            ),
        );
        setEditingRuleDrafts((prev) => {
            const copy = { ...prev };
            delete copy[columnName];
            return copy;
        });
    }

    async function fetchDatasets(ids) {
        if (!selectedClient) return;
        const targetIds = ids !== undefined ? ids : folderPath;
        setDatasetsLoading(true);
        try {
            const res = await call(
                `/orchestrate/master-config?client_name=${encodeURIComponent(selectedClient)}&source_type=${encodeURIComponent(sourceType || "")}&dataset_ids=${encodeURIComponent(targetIds || "")}`,
            );
            const scoped = (res.config || []).map((row) => ({
                ...row,
                dataset_name:
                    row.dataset_name ||
                    row.display_name ||
                    row.source_object ||
                    row.file_name ||
                    row.dataset_id,
            }));
            console.debug("Fetched datasets for active source", {
                client_name: selectedClient,
                source_type: sourceType,
                target_ids: targetIds,
                rows: scoped.length,
            });
            setDatasets(scoped);
        } catch (e) {
            toast("Failed to load datasets", "error");
        } finally {
            setDatasetsLoading(false);
        }
    }

    async function syncMasterConfig(client) {
        const c = client || selectedClient;
        if (!c) return;
        try {
            await call("/dq/sync_master_config", "POST", {
                client_name: c,
                source_type: sourceType || "ADLS",
            });
        } catch (e) {
            // Graceful — a missing config is expected for new clients
            console.warn(
                "Sync skipped (new client or no config yet):",
                e?.message || e,
            );
        }
    }

    async function registerSource() {
        const {
            source_type,
            client_name,
            source_name,
            base_url,
            bucket_name,
            azure_account_name,
            azure_container_name,
        } = sourceForm;
        const normalizedSourceType = String(source_type || "API").toUpperCase();

        if (!client_name) return toast("Client Name is required", "error");
        if (!source_name) return toast("Source Name is required", "error");

        if (normalizedSourceType === "S3" && !bucket_name) {
            return toast(
                "Bucket Name is required for S3 registration",
                "error",
            );
        }
        if (
            normalizedSourceType === "ADLS" &&
            (!azure_account_name || !azure_container_name)
        ) {
            return toast(
                "Azure Account Name and Container Name are required",
                "error",
            );
        }

        setSavingSource(true);

        try {
            await call("/api-source/register", "POST", {
                ...sourceForm,
                source_type: normalizedSourceType,
            });
            resetSessionState({ clearSourceSelection: true });
            toast(
                `${normalizedSourceType} source registered successfully`,
                "success",
            );
            setSelectedClient(client_name);
            setSourceForm({
                client_name: "",
                source_name: "",
                source_type: "API",
                base_url: "",
                auth_type: "none",
                auth_token: "",
                api_key_header: "X-Api-Key",
                endpoints: "",
                aws_access_key_id: "",
                aws_secret_access_key: "",
                region: "",
                bucket_name: "",
                azure_account_name: "",
                azure_account_key: "",
                azure_container_name: "",
            });
            await fetchClients();
            await fetchApiSourcesForClient(client_name);
            await fetchClientSourceTypes(client_name);
            if (normalizedSourceType === "S3") {
                setSourceType("S3");
                setFolderPath(`s3://${bucket_name}`);
                setSelectedEndpoint(`s3://${bucket_name}`);
            } else if (normalizedSourceType === "ADLS") {
                setSourceType("ADLS");
                setFolderPath(
                    `az://${azure_account_name}/${azure_container_name}`,
                );
                setSelectedEndpoint(
                    `az://${azure_account_name}/${azure_container_name}`,
                );
            } else if (normalizedSourceType === "API") {
                setSourceType("API");
                const firstEndpoint =
                    String(sourceForm.endpoints || "")
                        .split(",")
                        .map((v) => v.trim())
                        .filter(Boolean)[0] || base_url;
                setFolderPath(firstEndpoint);
                setSelectedEndpoint(firstEndpoint);
            }
        } catch (e) {
            toast("Registration failed: " + (e?.message || e), "error");
        } finally {
            setSavingSource(false);
        }
    }

    async function testConnection() {
        const {
            source_type,
            client_name,
            base_url,
            bucket_name,
            azure_account_name,
            azure_container_name,
        } = sourceForm;

        // Basic validation before testing
        if (!client_name) return toast("Client Name is required", "error");
        if (source_type === "API" && !base_url)
            return toast("Base URL is required", "error");
        if (source_type === "S3" && !bucket_name)
            return toast("Bucket Name is required", "error");
        if (
            source_type === "ADLS" &&
            (!azure_account_name || !azure_container_name)
        )
            return toast("Azure details are required", "error");

        setTestingConnection(true);
        setTestResult(null);
        setConnectionVerified(false);

        try {
            const res = await call(
                "/api-source/test-connection",
                "POST",
                sourceForm,
            );
            if (res.status === "SUCCESS") {
                setConnectionVerified(true);
                setTestResult({ type: "success", message: res.message });
                toast(res.message, "success");
            } else {
                setConnectionVerified(false);
                setTestResult({ type: "error", message: res.message });
                toast("Connection Test Failed: " + res.message, "error");
            }
        } catch (e) {
            setConnectionVerified(false);
            const msg = e?.message || String(e);
            setTestResult({ type: "error", message: msg });
            toast("Connection Test Error: " + msg, "error");
        } finally {
            setTestingConnection(false);
        }
    }

    function formatDatasetLabel(datasetId) {
        const d = (datasets || []).find((dd) => dd.dataset_id === datasetId);
        if (d && d.dataset_name) return d.dataset_name;
        if (datasetId && datasetId.length > 20)
            return `${datasetId.slice(0, 8)}…${datasetId.slice(-6)}`;
        return datasetId || "";
    }

    function statusColor(status) {
        if (!status) return "#6b7280";
        const s = String(status).toLowerCase();
        if (s === "passed" || s === "success") return "#16a34a";
        if (s === "failed" || s === "error") return "#ef4444";
        if (s === "warn" || s === "warning") return "#f59e0b";
        if (s === "skipped") return "#64748b";
        return "#6b7280";
    }

    // File Explorer
    async function openExplorer(
        initialPath,
        purpose = "preview",
        onSelect = null,
    ) {
        setShowExplorerModal(true);
        setPreviewData(null);
        setExplorerPurpose(purpose);
        setExplorerOnSelect(() => onSelect);

        let folderToOpen = initialPath || "";
        const isFile = /\.(csv|parquet|json|xls|xlsx|txt)$/i.test(initialPath);

        if (isFile) {
            // Open parent folder
            const parts = (initialPath || "").split("/");
            parts.pop();
            folderToOpen = parts.join("/");
            // Also trigger preview
            previewFile(initialPath);
        }

        setExplorerCurrentPath(folderToOpen);
        setExplorerLoading(true);
        try {
            const res = await call(
                `/storage/list?path=${encodeURIComponent(folderToOpen)}`,
            );
            setExplorerItems({
                folders: res.folders || [],
                files: res.files || [],
            });
        } catch (e) {
            toast("Failed to load storage", "error");
        } finally {
            setExplorerLoading(false);
        }
    }

    async function navigateToFolder(path) {
        setExplorerCurrentPath(path);
        setExplorerLoading(true);
        try {
            const res = await call(
                `/storage/list?path=${encodeURIComponent(path)}`,
            );
            setExplorerItems({
                folders: res.folders || [],
                files: res.files || [],
            });
        } catch (e) {
            toast("Failed to load folder", "error");
        } finally {
            setExplorerLoading(false);
        }
    }

    function navigateUp() {
        if (!explorerCurrentPath) return;
        const parts = explorerCurrentPath.replace(/\/+$/, "").split("/");
        parts.pop();
        navigateToFolder(parts.join("/"));
    }

    async function previewFile(filePath) {
        setPreviewLoading(true);
        try {
            const res = await call(
                `/storage/preview?path=${encodeURIComponent(filePath)}`,
            );
            setPreviewData({ ...res, path: filePath });
        } catch (e) {
            toast("Failed to preview file", "error");
        } finally {
            setPreviewLoading(false);
        }
    }

    function renderExplorerBreadcrumbs() {
        const parts = (explorerCurrentPath || "").split("/").filter(Boolean);
        if (parts.length === 0)
            return (
                <span
                    className="breadcrumb-item"
                    onClick={() => navigateToFolder("")}
                >
                    Root
                </span>
            );
        let cumulativePath = "";
        return (
            <>
                <span
                    className="breadcrumb-item"
                    onClick={() => navigateToFolder("")}
                >
                    Root
                </span>
                {parts.map((part, idx) => {
                    cumulativePath += idx === 0 ? part : `/${part}`;
                    const currentPath = cumulativePath;
                    return (
                        <span
                            key={idx}
                            style={{ display: "flex", alignItems: "center" }}
                        >
                            <span
                                style={{
                                    color: "var(--text3)",
                                    margin: "0 4px",
                                    fontSize: 10,
                                }}
                            >
                                /
                            </span>
                            <span
                                className="breadcrumb-item"
                                onClick={() => navigateToFolder(currentPath)}
                            >
                                {part}
                            </span>
                        </span>
                    );
                })}
            </>
        );
    }

    function renderPreviewContent() {
        if (previewLoading)
            return (
                <div className="explorer-preview-panel">
                    <div className="loading-overlay" style={{ height: "100%" }}>
                        <div
                            className="skeleton"
                            style={{
                                width: "80%",
                                height: 200,
                                borderRadius: 12,
                            }}
                        ></div>
                    </div>
                </div>
            );
        if (!previewData) return null;
        return (
            <div className="explorer-preview-panel">
                <div
                    style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        marginBottom: 16,
                    }}
                >
                    <div
                        style={{
                            fontWeight: 800,
                            fontSize: 16,
                            display: "flex",
                            alignItems: "center",
                            gap: 8,
                        }}
                    >
                        <FiFileText style={{ color: "var(--accent)" }} />
                        {previewData.path.split("/").pop()}
                    </div>
                    <button
                        className="orch-btn ghost"
                        onClick={() => setPreviewData(null)}
                    >
                        <FiCornerUpLeft style={{ marginRight: 6 }} /> Back
                    </button>
                </div>
                {previewData.type === "csv" && (
                    <div className="preview-table-wrapper">
                        <table className="preview-table">
                            <thead>
                                <tr>
                                    {previewData.columns.map((col, i) => (
                                        <th key={i}>{col}</th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {previewData.rows.map((row, i) => (
                                    <tr key={i}>
                                        {row.map((cell, j) => (
                                            <td key={j}>
                                                {cell !== null
                                                    ? String(cell)
                                                    : ""}
                                            </td>
                                        ))}
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                        <div
                            style={{
                                padding: "8px 12px",
                                fontSize: 11,
                                background: "var(--surface2)",
                                color: "var(--text3)",
                                textAlign: "right",
                            }}
                        >
                            {previewData.total_rows_approx}
                        </div>
                    </div>
                )}
                {previewData.type === "json" && (
                    <pre
                        className="orch-pre"
                        style={{
                            flex: 1,
                            margin: 0,
                            fontSize: 13,
                            overflow: "auto",
                        }}
                    >
                        {JSON.stringify(previewData.data, null, 2)}
                    </pre>
                )}
                {previewData.type === "text" && (
                    <pre
                        className="orch-pre"
                        style={{
                            flex: 1,
                            margin: 0,
                            fontSize: 13,
                            whiteSpace: "pre-wrap",
                            overflow: "auto",
                        }}
                    >
                        {previewData.content}
                    </pre>
                )}
            </div>
        );
    }

    // ----- RENDER -----
    const isHistoryView = location.pathname.endsWith("/history");

    // --- Effects (Moved to bottom) ---

    // Persist Fabric token to sessionStorage whenever it changes
    useEffect(() => {
        if (fabricAccessToken) {
            console.log("STEPPER: Persisting Fabric token to sessionStorage");
            sessionStorage.setItem("fabric_access_token", fabricAccessToken);
        }
    }, [fabricAccessToken]);

    // Restore Fabric token on mount
    useEffect(() => {
        const restoreToken = async () => {
            const storedToken = sessionStorage.getItem("fabric_access_token");
            if (storedToken) {
                console.log(
                    "STEPPER: Restored Fabric token from sessionStorage",
                );
                setFabricAccessToken(storedToken);
            } else {
                // Try fetching from backend session
                try {
                    console.log(
                        "STEPPER: Attempting to fetch Fabric token from backend",
                    );
                    const res = await call("/auth/fabric/token");
                    if (res && res.accessToken) {
                        console.log(
                            "STEPPER: Fetched Fabric token from backend",
                        );
                        setFabricAccessToken(res.accessToken);
                        sessionStorage.setItem(
                            "fabric_access_token",
                            res.accessToken,
                        );
                    }
                } catch (e) {
                    console.warn(
                        "STEPPER: Failed to fetch token from backend",
                        e,
                    );
                }
            }
        };
        restoreToken();
    }, [call]);

    // Fetch initial clients
    useEffect(() => {
        fetchClients();
    }, [fetchClients]);

    useEffect(() => {
        try {
            const manageFlag = location?.state?.manage;
            if (
                manageFlag === true ||
                String(manageFlag) === "1" ||
                String(manageFlag) === "true"
            ) {
                // No more manage modal, just go to step 1
            }
        } catch (e) {}
    }, [location?.state]);

    // Handle client selection changes
    useEffect(() => {
        resetSessionState({ clearSourceSelection: true });
        setClientSourceTypes([]);
        if (sourceForm.source_type !== "LOCAL") {
            setSourceType("");
        }
        if (selectedClient) {
            fetchApiSourcesForClient(selectedClient);
            fetchClientSourceTypes(selectedClient);
        } else {
            setApiSources([]);
            setSelectedApiSource(null);
            setSelectedEndpoint("");
        }
    }, [
        selectedClient,
        fetchApiSourcesForClient,
        fetchClientSourceTypes,
        resetSessionState,
        sourceForm.source_type,
    ]);

    // Sync workspace ref
    useEffect(() => {
        selectedWorkspaceRef.current = selectedWorkspace;
    }, [selectedWorkspace]);

    useEffect(() => {
        if (sourceType) sessionStorage.setItem("wizard_source_type", sourceType);
    }, [sourceType]);

    useEffect(() => {
        if (deploymentStrategy) {
            sessionStorage.setItem("wizard_deployment_strategy", deploymentStrategy);
        }
    }, [deploymentStrategy]);

    useEffect(() => {
        if (selectedTarget) {
            sessionStorage.setItem("wizard_selected_target", JSON.stringify(selectedTarget));
        } else {
            sessionStorage.removeItem("wizard_selected_target");
        }
    }, [selectedTarget]);

    useEffect(() => {
        if (selectedWorkspace) {
            sessionStorage.setItem("wizard_fabric_workspace", JSON.stringify(selectedWorkspace));
        }
    }, [selectedWorkspace]);

    useEffect(() => {
        if (selectedPipeline) {
            sessionStorage.setItem("wizard_fabric_pipeline", JSON.stringify(selectedPipeline));
        }
    }, [selectedPipeline]);

    return (
        <div className="orch-root-fullscreen">
            {isHistoryView ? (
                <div
                    className="stepper-content"
                    style={{ marginTop: 40, padding: "0 40px 100px" }}
                >
                    <div style={{ marginBottom: 20 }}>
                        <button
                            className="orch-btn ghost tiny"
                            onClick={() => nav("/orchestration-beta")}
                            style={{
                                display: "flex",
                                alignItems: "center",
                                gap: 8,
                                fontWeight: 800,
                                color: "var(--blue)",
                            }}
                        >
                            ← Back to Orchestration
                        </button>
                    </div>
                    <HistoryView isEmbedded />
                </div>
            ) : (
                <>
                    {/* Stepper Bar */}
                    <div className="stepper-bar">
                        <div
                            className="stepper-steps-container"
                            style={{
                                display: "flex",
                                alignItems: "center",
                                gap: 0,
                                flex: 1,
                                justifyContent: "center",
                            }}
                        >
                            {STEPS_CONFIG.map((s, idx) => {
                                return (
                                    <div
                                        key={s.num}
                                        className={`stepper-item ${step === s.num ? "active" : ""} ${step > s.num ? "done" : ""}`}
                                    >
                                        <div
                                            className="stepper-num"
                                            onClick={() => {
                                                if (step > s.num)
                                                    goToStep(s.num);
                                            }}
                                        >
                                            {step > s.num ? (
                                                <FiCheck size={18} />
                                            ) : (
                                                s.num + 1
                                            )}
                                        </div>
                                        <span className="stepper-label">
                                            {s.label}
                                        </span>
                                        {idx < STEPS_CONFIG.length - 1 && (
                                            <div className="stepper-connector" />
                                        )}
                                    </div>
                                );
                            })}
                        </div>

                        <div
                            className="stepper-actions"
                            style={{
                                position: "absolute",
                                right: 40,
                                display: "flex",
                                alignItems: "center",
                                gap: 12,
                            }}
                        >
                            <button
                                className="orch-btn ghost premium-history-btn"
                                onClick={() =>
                                    nav("/orchestration-beta/history")
                                }
                                style={{
                                    display: "flex",
                                    alignItems: "center",
                                    gap: 8,
                                    padding: "8px 16px",
                                    borderRadius: 12,
                                    background: "rgba(255,255,255,0.7)",
                                    border: "1px solid rgba(0,0,0,0.08)",
                                    boxShadow: "0 4px 12px rgba(0,0,0,0.03)",
                                    fontWeight: 800,
                                    fontSize: 13,
                                    color: "var(--text1)",
                                    transition:
                                        "all 0.2s cubic-bezier(0.4, 0, 0.2, 1)",
                                }}
                            >
                                <div
                                    style={{
                                        position: "relative",
                                        display: "flex",
                                        alignItems: "center",
                                        justifyContent: "center",
                                    }}
                                >
                                    <FiActivity
                                        size={18}
                                        style={{ color: "var(--blue)" }}
                                    />
                                    <FiClock
                                        size={10}
                                        style={{
                                            position: "absolute",
                                            bottom: -2,
                                            right: -2,
                                            background: "#fff",
                                            borderRadius: "50%",
                                        }}
                                    />
                                </div>
                                <span>Pipeline History</span>
                            </button>
                        </div>
                    </div>

                    <div className="stepper-content">
                        <AnimatePresence mode="wait">
                            {step === 0 && (
                                <StepPlatform
                                    selectedPlatform={selectedPlatform}
                                    setSelectedPlatform={(p) => {
                                        setSelectedPlatform(p);
                                        localStorage.setItem(
                                            "selected_platform",
                                            p,
                                        );
                                    }}
                                    onNext={() => {
                                        if (selectedPlatform !== "FABRIC") {
                                            toast(
                                                "Only Microsoft Fabric is configured. Select Microsoft Fabric to continue.",
                                                "warning",
                                            );
                                            return;
                                        }
                                        goToStep(1);
                                    }}
                                />
                            )}
                            {step === 1 && (
                                <StepClient
                                    clients={clients}
                                    clientLoading={clientLoading}
                                    selectedClient={selectedClient}
                                    setSelectedClient={setSelectedClient}
                                    fetchClients={fetchClients}
                                    onNext={() => {
                                        if (
                                            sourceForm.source_type ===
                                                "LOCAL" &&
                                            sourceForm.client_name
                                        ) {
                                            setSourceType("LOCAL");
                                            setClientSourceTypes(["LOCAL"]);
                                            resetSessionState({
                                                clearSourceSelection: true,
                                            });
                                        }
                                        goToStep(2);
                                    }}
                                    call={call}
                                    toast={toast}
                                    sourceForm={sourceForm}
                                    setSourceForm={setSourceForm}
                                    registerSource={registerSource}
                                    savingSource={savingSource}
                                    testConnection={testConnection}
                                    testingConnection={testingConnection}
                                    connectionVerified={connectionVerified}
                                    setConnectionVerified={
                                        setConnectionVerified
                                    }
                                    testResult={testResult}
                                    setTestResult={setTestResult}
                                    extractedFabricData={extractedFabricData}
                                    setExtractedFabricData={
                                        setExtractedFabricData
                                    }
                                    targets={targets}
                                    setTargets={setTargets}
                                    selectedTarget={selectedTarget}
                                    setSelectedTarget={setSelectedTarget}
                                    fetchTargets={fetchTargets}
                                    setShowUploadModal={setShowUploadModal}
                                />
                            )}
                            {step === 2 && (
                                <PipelineIntelligence
                                    clientName={selectedClient}
                                    initialData={intelligenceData}
                                    clientSourceTypes={clientSourceTypes}
                                    currentSourceType={sourceType}
                                    apiSources={apiSources}
                                    fabricDiscoveryData={
                                        sourceForm.fabricDiscoveryData
                                    }
                                    fabricMode={sourceForm.fabricMode}
                                    selectedPlatform={selectedPlatform}
                                    selectedWorkspace={selectedWorkspace}
                                    setSelectedWorkspace={
                                        handleWorkspaceSelection
                                    }
                                    selectedPipeline={selectedPipeline}
                                    setSelectedPipeline={
                                        handlePipelineSelection
                                    }
                                    selectedDeploymentStrategy={
                                        deploymentStrategy
                                    }
                                    setSelectedDeploymentStrategy={
                                        setDeploymentStrategy
                                    }
                                    onScanComplete={(data) => {
                                        console.log(
                                            "STEPPER: Intelligence scan completed",
                                            data,
                                        );
                                        if (data?.__fabric_access_token) {
                                            setFabricAccessToken(
                                                data.__fabric_access_token,
                                            );
                                        }
                                    }}
                                    onConfirm={(data) => {
                                        if (data) {
                                            setIntelligenceData(data);
                                            if (data.deploymentStrategy)
                                                setDeploymentStrategy(
                                                    data.deploymentStrategy,
                                                );
                                            if (data.__fabric_access_token)
                                                setFabricAccessToken(
                                                    data.__fabric_access_token,
                                                );

                                            if (selectedPlatform === "FABRIC") {
                                                setSourceType("FABRIC");
                                                const fabricSource =
                                                    data.source_path ||
                                                    data.folder_path ||
                                                    selectedPipeline?.id ||
                                                    selectedPipeline?.name ||
                                                    "fabric_pipeline";
                                                setFolderPath(fabricSource);
                                                setSelectedEndpoint(fabricSource);
                                                setSelectedApiSource(
                                                    "fabric-intelligence",
                                                );
                                            }

                                            // Preload source into orchestration if it's a Fabric runtime promotion
                                            if (
                                                data.staging_table ||
                                                data.source_type ===
                                                    "NEON_STAGED_SOURCE"
                                            ) {
                                                setSourceType(
                                                    data.source_type ||
                                                        "NEON_STAGED_SOURCE",
                                                );
                                                setFolderPath(
                                                    data.folder_path ||
                                                        data.source_path ||
                                                        data.staging_table,
                                                );
                                                setSelectedEndpoint(
                                                    data.source_path ||
                                                        data.staging_table,
                                                );
                                                setSelectedApiSource(
                                                    "fabric-runtime",
                                                );
                                                fetchDatasets(
                                                    data.folder_path ||
                                                        data.source_path ||
                                                        data.staging_table,
                                                );
                                            }
                                        }
                                        fetchDatasets();
                                        goToStep(3);
                                    }}
                                />
                            )}
                            {step === 3 && (
                                <StepSourceConfig
                                    sourceType={sourceType}
                                    folderPath={folderPath}
                                    selectedClient={selectedClient}
                                    selectedApiSource={selectedApiSource}
                                    setSelectedApiSource={setSelectedApiSource}
                                    selectedEndpoint={selectedEndpoint}
                                    setSelectedEndpoint={setSelectedEndpoint}
                                    setFolderPath={setFolderPath}
                                    datasets={datasets}
                                    fetchDatasets={fetchDatasets}
                                    call={call}
                                    toast={toast}
                                    intelligenceData={intelligenceData}
                                    configPersisted={configPersisted}
                                    setConfigPersisted={setConfigPersisted}
                                    syncMasterConfig={syncMasterConfig}
                                    clientSourceTypes={clientSourceTypes}
                                    editingConfigDataset={editingConfigDataset}
                                    setEditingConfigDataset={
                                        setEditingConfigDataset
                                    }
                                    editingConfigColumns={editingConfigColumns}
                                    editingConfigLoading={editingConfigLoading}
                                    selectedDqDataset={selectedDqDataset}
                                    setSelectedDqDataset={setSelectedDqDataset}
                                    showDQPanel={showDQPanel}
                                    setShowDQPanel={setShowDQPanel}
                                    loadDqConfig={loadDqConfig}
                                    setPendingDqDataset={setPendingDqDataset}
                                    setShowModeModal={setShowModeModal}
                                    dqError={dqError}
                                    setDqError={setDqError}
                                    dqLoading={dqLoading}
                                    isSuggesting={isSuggesting}
                                    editingRuleDrafts={editingRuleDrafts}
                                    setEditingRuleDrafts={setEditingRuleDrafts}
                                    toggleColumnActive={toggleColumnActive}
                                    changeColumnSeverity={changeColumnSeverity}
                                    saveColumnRule={saveColumnRule}
                                    saveDqConfig={saveDqConfig}
                                    editingConfigSaving={editingConfigSaving}
                                    formatDatasetLabel={formatDatasetLabel}
                                    onNext={() => goToStep(4)}
                                    onBack={() => goToStep(2)}
                                    runOrchestration={runOrchestration}
                                    isOrchestrating={isOrchestrating}
                                    datasetsLoading={datasetsLoading}
                                    selectedPlatform={selectedPlatform}
                                    setMasterConfig={setMasterConfig}
                                    fabricMode={sourceForm.fabricMode}
                                    apiSources={apiSources}
                                    s3Sources={s3Sources}
                                    adlsSources={adlsSources}
                                    apiSourcesLoading={apiSourcesLoading}
                                    openExplorer={openExplorer}
                                    extractedFabricData={extractedFabricData}
                                    setExtractedFabricData={
                                        setExtractedFabricData
                                    }
                                    onManualSourceSelected={() => {}}
                                    refreshTrigger={localRefreshTrigger}
                                    showUploadModal={showUploadModal}
                                    setShowUploadModal={setShowUploadModal}
                                    setPipelineDeployed={setPipelineDeployed}
                                    pipelineDeployed={pipelineDeployed}
                                />
                            )}
                            {step === 4 && (
                                <StepDeployment
                                    selectedWorkspace={selectedWorkspace}
                                    selectedPipeline={selectedPipeline}
                                    deploymentStrategy={deploymentStrategy}
                                    deploymentPackage={deploymentPackage}
                                    setDeploymentPackage={setDeploymentPackage}
                                    fabricAccessToken={fabricAccessToken}
                                    intelligenceData={intelligenceData}
                                    onNext={() => {
                                        setPipelineDeployed(true);
                                        goToStep(5);
                                    }}
                                    onBack={() => goToStep(3)}
                                />
                            )}
                            {step === 5 && (
                                <StepReviewConfirm
                                    selectedClient={selectedClient}
                                    sourceType={sourceType}
                                    folderPath={folderPath}
                                    intelligenceData={intelligenceData}
                                    configPersisted={configPersisted}
                                    masterConfig={masterConfig}
                                    requiresRealScan={
                                        selectedApiSource ===
                                        "intelligence-scan"
                                    }
                                    onBack={() => goToStep(4)}
                                    onConfirm={runOrchestration}
                                    isOrchestrating={isOrchestrating}
                                    fabricMode={sourceForm.fabricMode}
                                    pipelineDeployed={pipelineDeployed}
                                    selectedPlatform={selectedPlatform}
                                    deploymentStrategy={deploymentStrategy}
                                    deploymentPackage={deploymentPackage}
                                    selectedWorkspace={selectedWorkspace}
                                    selectedPipeline={selectedPipeline}
                                    selectedTarget={selectedTarget}
                                />
                            )}
                            {step === 6 && (
                                <StepProgress
                                    orchestrateResp={orchestrateResp}
                                    isOrchestrating={isOrchestrating}
                                    runOrchestration={runOrchestration}
                                    loadDqConfig={loadDqConfig}
                                    setPendingDqDataset={setPendingDqDataset}
                                    setShowModeModal={setShowModeModal}
                                    openExplorer={openExplorer}
                                    statusColor={statusColor}
                                    call={call}
                                    intelligenceData={intelligenceData}
                                />
                            )}
                            {step === 7 && <HistoryView isEmbedded />}
                        </AnimatePresence>
                    </div>
                </>
            )}

            {/* ===== MODALS ===== */}
            {/* ===== MODALS (Portaled to Root) ===== */}
            {/* ===== MODALS (Portaled to Root) ===== */}
            {/* Upload Modal */}
            {showUploadModal &&
                createPortal(
                    <div
                        className="mode-modal-overlay"
                        style={{ zIndex: 1250 }}
                    >
                        <motion.div
                            variants={modalVariants}
                            initial="initial"
                            animate="animate"
                            exit="exit"
                            className="upload-modal-card"
                            key="upload-modal"
                        >
                            <div
                                style={{
                                    display: "flex",
                                    justifyContent: "space-between",
                                    alignItems: "center",
                                    marginBottom: 8,
                                }}
                            >
                                <div
                                    style={{
                                        display: "flex",
                                        gap: 12,
                                        alignItems: "center",
                                    }}
                                >
                                    <div
                                        style={{
                                            width: 44,
                                            height: 44,
                                            borderRadius: 10,
                                            background: "rgba(59,130,246,0.08)",
                                            display: "flex",
                                            alignItems: "center",
                                            justifyContent: "center",
                                            fontSize: 20,
                                        }}
                                    >
                                        📤
                                    </div>
                                    <div>
                                        <h3 style={{ margin: 0 }}>
                                            Upload files
                                        </h3>
                                        <div
                                            className="step-sub"
                                            style={{ marginTop: 4 }}
                                        >
                                            Choose client and upload files.
                                        </div>
                                    </div>
                                </div>
                                <button
                                    className="orch-btn ghost tiny"
                                    onClick={() => {
                                        setShowUploadModal(false);
                                        setUploadFiles(null);
                                    }}
                                    aria-label="Close"
                                >
                                    <FiX />
                                </button>
                            </div>

                            <div
                                style={{
                                    display: "flex",
                                    flexDirection: "column",
                                    gap: 12,
                                }}
                            >
                                <div>
                                    <label
                                        style={{
                                            fontSize: 12,
                                            fontWeight: 700,
                                        }}
                                    >
                                        Client
                                    </label>
                                    <div>
                                        <select
                                            value={selectedClient || ""}
                                            onChange={(e) =>
                                                setSelectedClient(
                                                    e.target.value,
                                                )
                                            }
                                            style={{
                                                width: "100%",
                                                padding: "8px 10px",
                                                borderRadius: 8,
                                                border: "1px solid var(--border)",
                                            }}
                                        >
                                            <option value="">
                                                Choose client
                                            </option>
                                            {selectedClient &&
                                                !clients.includes(
                                                    selectedClient,
                                                ) && (
                                                    <option
                                                        value={selectedClient}
                                                    >
                                                        {selectedClient}{" "}
                                                        (Pending)
                                                    </option>
                                                )}
                                            {clients.map((c) => (
                                                <option key={c} value={c}>
                                                    {c}
                                                </option>
                                            ))}
                                        </select>
                                    </div>
                                </div>

                                <div
                                    onDrop={handleDrop}
                                    onDragOver={handleDragOver}
                                    onDragEnter={handleDragEnter}
                                    onDragLeave={handleDragLeave}
                                    className={`upload-dropzone ${isDragOver ? "dragover" : ""}`}
                                >
                                    <div
                                        style={{
                                            marginBottom: 8,
                                            fontWeight: 700,
                                        }}
                                    >
                                        Drag & drop files here
                                    </div>
                                    <div
                                        className="small-muted"
                                        style={{ marginBottom: 12 }}
                                    >
                                        or
                                    </div>
                                    <div
                                        style={{
                                            display: "flex",
                                            justifyContent: "center",
                                            gap: 8,
                                        }}
                                    >
                                        <input
                                            id="orch-upload-input"
                                            type="file"
                                            multiple
                                            style={{ display: "none" }}
                                            onChange={handleFileInputChange}
                                        />
                                        <label
                                            htmlFor="orch-upload-input"
                                            className="orch-btn"
                                        >
                                            Choose files
                                        </label>
                                        <button
                                            className="orch-btn ghost"
                                            onClick={() => setUploadFiles([])}
                                            disabled={
                                                !(
                                                    uploadFiles &&
                                                    uploadFiles.length
                                                )
                                            }
                                        >
                                            Clear
                                        </button>
                                    </div>
                                </div>

                                {uploadFiles && uploadFiles.length > 0 && (
                                    <div className="upload-file-list">
                                        {uploadFiles.map((f, idx) => (
                                            <div
                                                key={idx}
                                                className="upload-file-item"
                                            >
                                                <div className="upload-file-name">
                                                    {f.name}
                                                </div>
                                                <div
                                                    style={{
                                                        display: "flex",
                                                        gap: 8,
                                                        alignItems: "center",
                                                    }}
                                                >
                                                    <div className="upload-file-size">
                                                        {f.size < 1024
                                                            ? `${f.size} B`
                                                            : `${(f.size / 1024).toFixed(1)} KB`}
                                                    </div>
                                                    <button
                                                        className="orch-btn tiny ghost"
                                                        onClick={() =>
                                                            removeFile(idx)
                                                        }
                                                    >
                                                        Remove
                                                    </button>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}

                                <div
                                    style={{
                                        display: "flex",
                                        justifyContent: "flex-end",
                                        gap: 8,
                                        marginTop: 8,
                                    }}
                                >
                                    <button
                                        className="orch-btn ghost"
                                        onClick={() => {
                                            setShowUploadModal(false);
                                            setUploadFiles(null);
                                        }}
                                    >
                                        Cancel
                                    </button>
                                    <button
                                        className="orch-btn primary"
                                        onClick={handleUpload}
                                        disabled={
                                            uploadLoading ||
                                            !selectedClient ||
                                            !(uploadFiles && uploadFiles.length)
                                        }
                                    >
                                        {uploadLoading
                                            ? "Uploading..."
                                            : "Upload"}
                                    </button>
                                </div>
                            </div>
                        </motion.div>
                    </div>,
                    document.body,
                )}

            {/* Explorer Modal */}
            {showExplorerModal &&
                createPortal(
                    <div
                        className="mode-modal-overlay"
                        style={{ zIndex: 1300 }}
                    >
                        <motion.div
                            key="explorer-modal"
                            variants={modalVariants}
                            initial="initial"
                            animate="animate"
                            exit="exit"
                            style={{
                                width:
                                    previewData || previewLoading
                                        ? "90vw"
                                        : 800,
                                height:
                                    previewData || previewLoading
                                        ? "90vh"
                                        : "auto",
                                maxWidth: "95vw",
                                background: "var(--surface)",
                                borderRadius: 16,
                                overflow: "hidden",
                                boxShadow: "0 20px 60px rgba(0,0,0,0.15)",
                                display: "flex",
                                flexDirection: "column",
                            }}
                        >
                            <div
                                className="explorer-container"
                                style={{
                                    height:
                                        previewData || previewLoading
                                            ? "100%"
                                            : 500,
                                    display: "flex",
                                    flexDirection: "column",
                                }}
                            >
                                <div className="explorer-header">
                                    <button
                                        className="orch-btn ghost tiny"
                                        onClick={navigateUp}
                                        disabled={
                                            !explorerCurrentPath || previewData
                                        }
                                        aria-label="Up"
                                    >
                                        <FiCornerUpLeft />
                                    </button>
                                    <div className="explorer-breadcrumb">
                                        {renderExplorerBreadcrumbs()}
                                    </div>
                                    <div style={{ display: "flex", gap: 8 }}>
                                        {explorerPurpose === "pick" && (
                                            <button
                                                className="orch-btn primary tiny"
                                                onClick={() => {
                                                    if (explorerOnSelect)
                                                        explorerOnSelect(
                                                            explorerCurrentPath,
                                                        );
                                                    setShowExplorerModal(false);
                                                }}
                                            >
                                                Select Folder
                                            </button>
                                        )}
                                        <button
                                            className="orch-btn ghost tiny"
                                            onClick={() => {
                                                setShowExplorerModal(false);
                                                setPreviewData(null);
                                            }}
                                            aria-label="Close"
                                        >
                                            <FiX />
                                        </button>
                                    </div>
                                </div>
                                {!previewData && !previewLoading && (
                                    <div className="explorer-content">
                                        {explorerLoading ? (
                                            <div className="loading-overlay">
                                                <div
                                                    className="skeleton-circle"
                                                    style={{
                                                        width: 40,
                                                        height: 40,
                                                        marginBottom: 16,
                                                    }}
                                                ></div>
                                            </div>
                                        ) : explorerItems.folders.length ===
                                              0 &&
                                          explorerItems.files.length === 0 ? (
                                            <div className="empty-state">
                                                <FiFolder
                                                    size={48}
                                                    style={{
                                                        opacity: 0.3,
                                                        marginBottom: 16,
                                                    }}
                                                />
                                                <div
                                                    style={{ fontWeight: 600 }}
                                                >
                                                    Empty Folder
                                                </div>
                                            </div>
                                        ) : (
                                            <div className="explorer-grid">
                                                {explorerItems.folders.map(
                                                    (f) => (
                                                        <div
                                                            key={f.path}
                                                            className="explorer-item"
                                                            onClick={() =>
                                                                navigateToFolder(
                                                                    f.path,
                                                                )
                                                            }
                                                        >
                                                            <div
                                                                className="explorer-item-icon"
                                                                style={{
                                                                    color: "#fed7aa",
                                                                }}
                                                            >
                                                                <FiFolder />
                                                            </div>
                                                            <div className="explorer-item-name">
                                                                {f.name}
                                                            </div>
                                                        </div>
                                                    ),
                                                )}
                                                {explorerItems.files.map(
                                                    (f) => (
                                                        <div
                                                            key={f.path}
                                                            className={`explorer-item ${f.path === previewData?.path ? "selected" : ""}`}
                                                            onClick={() =>
                                                                previewFile(
                                                                    f.path,
                                                                )
                                                            }
                                                        >
                                                            <div
                                                                className="explorer-item-icon"
                                                                style={{
                                                                    color: "#94a3b8",
                                                                }}
                                                            >
                                                                <FiFileText />
                                                            </div>
                                                            <div className="explorer-item-name">
                                                                {f.name}
                                                            </div>
                                                            <div
                                                                className="small-muted"
                                                                style={{
                                                                    fontSize: 9,
                                                                    marginTop: 4,
                                                                }}
                                                            >
                                                                {f.size < 1024
                                                                    ? `${f.size} B`
                                                                    : `${(f.size / 1024).toFixed(1)} KB`}
                                                            </div>
                                                        </div>
                                                    ),
                                                )}
                                            </div>
                                        )}
                                    </div>
                                )}
                                {(previewData || previewLoading) &&
                                    renderPreviewContent()}
                            </div>
                        </motion.div>
                    </div>,
                    document.body,
                )}

            {/* DQ Mode Modal */}
            {showModeModal &&
                createPortal(
                    <div
                        className="mode-modal-overlay"
                        style={{ zIndex: 1200 }}
                    >
                        <motion.div
                            key="mode-modal"
                            variants={modalVariants}
                            initial="initial"
                            animate="animate"
                            exit="exit"
                            className="mode-modal-card"
                        >
                            <div
                                style={{
                                    display: "flex",
                                    justifyContent: "space-between",
                                    alignItems: "center",
                                    marginBottom: 20,
                                }}
                            >
                                <h3 style={{ margin: 0 }}>Select AI DQ Mode</h3>
                                <button
                                    className="orch-btn ghost tiny"
                                    onClick={() => setShowModeModal(false)}
                                >
                                    <FiX />
                                </button>
                            </div>
                            {!showCustomPrompt ? (
                                <>
                                    <p
                                        className="small-muted"
                                        style={{ marginBottom: 24 }}
                                    >
                                        Choose how the AI should analyze your
                                        dataset for DQ rules.
                                    </p>
                                    <div className="mode-card-container dq-mode-grid">
                                        <div
                                            className="mode-card"
                                            onClick={() => {
                                                previewDq(
                                                    pendingDqDataset,
                                                    "general",
                                                );
                                                setShowModeModal(false);
                                            }}
                                        >
                                            <div
                                                className="mode-card-icon"
                                                style={{
                                                    background:
                                                        "rgba(59, 130, 246, 0.1)",
                                                    color: "#3b82f6",
                                                }}
                                            >
                                                <FiBarChart2 />
                                            </div>
                                            <div className="mode-card-title dq-mode-title">
                                                General
                                            </div>
                                            <div className="mode-card-desc">
                                                Basic checks suitable for most
                                                datasets.
                                            </div>
                                        </div>
                                        <div
                                            className="mode-card"
                                            onClick={() => {
                                                previewDq(
                                                    pendingDqDataset,
                                                    "life_science",
                                                );
                                                setShowModeModal(false);
                                            }}
                                        >
                                            <div
                                                className="mode-card-icon"
                                                style={{
                                                    background:
                                                        "rgba(16, 185, 129, 0.1)",
                                                    color: "#10b981",
                                                }}
                                            >
                                                <FiActivity />
                                            </div>
                                            <div className="mode-card-title dq-mode-title">
                                                Clinical
                                            </div>
                                            <div className="mode-card-desc">
                                                GxP-aware healthcare validation.
                                            </div>
                                        </div>
                                        <div
                                            className="mode-card"
                                            onClick={() => {
                                                previewDq(
                                                    pendingDqDataset,
                                                    "commercial",
                                                );
                                                setShowModeModal(false);
                                            }}
                                        >
                                            <div
                                                className="mode-card-icon"
                                                style={{
                                                    background:
                                                        "rgba(245, 158, 11, 0.1)",
                                                    color: "#f59e0b",
                                                }}
                                            >
                                                <FiBarChart2 />
                                            </div>
                                            <div className="mode-card-title dq-mode-title">
                                                Commercial
                                            </div>
                                            <div className="mode-card-desc">
                                                Revenue and CRM data checks.
                                            </div>
                                        </div>
                                        <div
                                            className="mode-card"
                                            onClick={() => {
                                                previewDq(
                                                    pendingDqDataset,
                                                    "rnd",
                                                );
                                                setShowModeModal(false);
                                            }}
                                        >
                                            <div
                                                className="mode-card-icon"
                                                style={{
                                                    background:
                                                        "rgba(139, 92, 246, 0.1)",
                                                    color: "#8b5cf6",
                                                }}
                                            >
                                                <FiSearch />
                                            </div>
                                            <div className="mode-card-title dq-mode-title">
                                                R&D
                                            </div>
                                            <div className="mode-card-desc">
                                                Lab and research data rules.
                                            </div>
                                        </div>
                                        <div
                                            className="mode-card"
                                            onClick={() => {
                                                previewDq(
                                                    pendingDqDataset,
                                                    "manufacturing",
                                                );
                                                setShowModeModal(false);
                                            }}
                                        >
                                            <div
                                                className="mode-card-icon"
                                                style={{
                                                    background:
                                                        "rgba(236, 72, 153, 0.1)",
                                                    color: "#ec4899",
                                                }}
                                            >
                                                <FiSettings />
                                            </div>
                                            <div className="mode-card-title dq-mode-title">
                                                MFG
                                            </div>
                                            <div className="mode-card-desc">
                                                Manufacturing quality checks.
                                            </div>
                                        </div>
                                        <div
                                            className="mode-card"
                                            onClick={() => {
                                                previewDq(
                                                    pendingDqDataset,
                                                    "scm",
                                                );
                                                setShowModeModal(false);
                                            }}
                                        >
                                            <div
                                                className="mode-card-icon"
                                                style={{
                                                    background:
                                                        "rgba(20, 184, 166, 0.1)",
                                                    color: "#14b8a6",
                                                }}
                                            >
                                                <FiDownload />
                                            </div>
                                            <div className="mode-card-title dq-mode-title">
                                                SCM
                                            </div>
                                            <div className="mode-card-desc">
                                                Supply chain data integrity.
                                            </div>
                                        </div>
                                        <div
                                            className="mode-card custom-card"
                                            onClick={() =>
                                                setShowCustomPrompt(true)
                                            }
                                        >
                                            <div
                                                className="mode-card-icon"
                                                style={{
                                                    background:
                                                        "rgba(99, 102, 241, 0.1)",
                                                    color: "#6366f1",
                                                }}
                                            >
                                                <FiEdit2 />
                                            </div>
                                            <div className="mode-card-title dq-mode-title">
                                                Custom
                                            </div>
                                            <div className="mode-card-desc">
                                                Provide your own instructions.
                                            </div>
                                        </div>
                                    </div>
                                </>
                            ) : (
                                <div
                                    style={{
                                        display: "flex",
                                        flexDirection: "column",
                                        gap: 16,
                                    }}
                                >
                                    <p className="small-muted">
                                        Describe the DQ rules you want the AI to
                                        generate:
                                    </p>
                                    <textarea
                                        className="orch-input"
                                        rows={6}
                                        placeholder="e.g. Ensure all dates are in ISO format, validate email addresses..."
                                        value={customDqPrompt}
                                        onChange={(e) =>
                                            setCustomDqPrompt(e.target.value)
                                        }
                                        style={{
                                            resize: "vertical",
                                            fontSize: 14,
                                        }}
                                    />
                                    <div
                                        style={{
                                            display: "flex",
                                            justifyContent: "flex-end",
                                            gap: 8,
                                        }}
                                    >
                                        <button
                                            className="orch-btn ghost"
                                            onClick={() =>
                                                setShowCustomPrompt(false)
                                            }
                                        >
                                            Back
                                        </button>
                                        <button
                                            className="orch-btn primary"
                                            disabled={!customDqPrompt.trim()}
                                            onClick={() => {
                                                previewDq(
                                                    pendingDqDataset,
                                                    "custom",
                                                );
                                                setShowModeModal(false);
                                                setShowCustomPrompt(false);
                                            }}
                                        >
                                            Generate with Custom Prompt
                                        </button>
                                    </div>
                                </div>
                            )}
                        </motion.div>
                    </div>,
                    document.body,
                )}

            {/* Premium DQ Modal */}
            {showDQPanel &&
                editingConfigDataset &&
                createPortal(
                    <div
                        className="mode-modal-overlay"
                        style={{ zIndex: 1100 }}
                    >
                        <motion.div
                            key="dq-modal"
                            initial={{ opacity: 0, scale: 0.95 }}
                            animate={{
                                opacity: 1,
                                scale: 1,
                                transition: {
                                    type: "spring",
                                    damping: 25,
                                    stiffness: 300,
                                },
                            }}
                            exit={{ opacity: 0, scale: 0.95 }}
                            className="mode-modal-card"
                            style={{
                                width: 840,
                                maxWidth: "95vw",
                                padding: 0,
                                overflow: "hidden",
                                display: "flex",
                                flexDirection: "column",
                                maxHeight: "90vh",
                            }}
                        >
                            <div
                                className="dq-editor-header"
                                style={{
                                    padding: "24px 32px 16px",
                                    borderBottom: "1px solid var(--border)",
                                    margin: 0,
                                }}
                            >
                                <div>
                                    <div
                                        style={{
                                            fontWeight: 900,
                                            fontSize: 18,
                                            color: "var(--text1)",
                                        }}
                                    >
                                        {isSuggesting
                                            ? "AI DQ Suggestions"
                                            : "DQ Configuration"}
                                    </div>
                                    <div
                                        className="step-sub"
                                        style={{ fontSize: 13, marginTop: 4 }}
                                    >
                                        {formatDatasetLabel(
                                            editingConfigDataset,
                                        )}
                                    </div>
                                </div>
                                <div style={{ display: "flex", gap: 8 }}>
                                    <button
                                        className="orch-btn tiny ghost"
                                        onClick={() => {
                                            setPendingDqDataset(
                                                editingConfigDataset,
                                            );
                                            setShowModeModal(true);
                                        }}
                                    >
                                        <FiZap style={{ marginRight: 6 }} /> AI
                                        Suggest
                                    </button>
                                    <button
                                        className="orch-btn ghost tiny"
                                        onClick={() => {
                                            setEditingConfigDataset(null);
                                            setShowDQPanel(false);
                                            setDqError(null);
                                        }}
                                    >
                                        <FiX />
                                    </button>
                                </div>
                            </div>

                            {dqError && (
                                <div
                                    className="panel-error-alert"
                                    style={{ margin: "16px 32px 0" }}
                                >
                                    {dqError}
                                </div>
                            )}

                            <div
                                className="dq-editor-scroll"
                                style={{
                                    padding: "24px 32px",
                                    flex: 1,
                                    overflowY: "auto",
                                }}
                            >
                                {editingConfigLoading || dqLoading ? (
                                    <div
                                        style={{
                                            display: "flex",
                                            flexDirection: "column",
                                            gap: 12,
                                        }}
                                    >
                                        {[1, 2, 3].map((i) => (
                                            <div
                                                key={i}
                                                className="skeleton"
                                                style={{
                                                    height: 90,
                                                    borderRadius: 12,
                                                }}
                                            />
                                        ))}
                                    </div>
                                ) : editingConfigColumns.length === 0 ? (
                                    <div
                                        style={{
                                            textAlign: "center",
                                            padding: "60px 20px",
                                        }}
                                    >
                                        <FiClipboard
                                            style={{
                                                fontSize: 40,
                                                marginBottom: 16,
                                                color: "var(--text3)",
                                            }}
                                        />
                                        <div
                                            style={{
                                                fontWeight: 800,
                                                fontSize: 16,
                                            }}
                                        >
                                            No columns found
                                        </div>
                                        <p className="step-sub">
                                            Sync master config or use AI suggest
                                            to generate rules.
                                        </p>
                                    </div>
                                ) : (
                                    <div
                                        style={{
                                            display: "flex",
                                            flexDirection: "column",
                                            gap: 14,
                                        }}
                                    >
                                        {editingConfigColumns.map((col) => (
                                            <div
                                                key={col.column_name}
                                                className="dq-col-card"
                                                style={{
                                                    padding: 16,
                                                    background: "#fff",
                                                    border: "1px solid var(--border)",
                                                    borderRadius: 12,
                                                }}
                                            >
                                                <div
                                                    className="dq-col-header"
                                                    style={{
                                                        marginBottom: 12,
                                                        display: "flex",
                                                        justifyContent:
                                                            "space-between",
                                                        alignItems: "center",
                                                    }}
                                                >
                                                    <div
                                                        className="dq-col-name"
                                                        style={{
                                                            fontSize: 14,
                                                            fontWeight: 700,
                                                        }}
                                                    >
                                                        {col.column_name}
                                                        <span
                                                            className="source-badge"
                                                            style={{
                                                                background:
                                                                    "var(--surface2)",
                                                                color: "var(--text2)",
                                                                marginLeft: 8,
                                                                padding:
                                                                    "2px 6px",
                                                                borderRadius: 4,
                                                                fontSize: 10,
                                                            }}
                                                        >
                                                            {
                                                                col.expected_data_type
                                                            }
                                                        </span>
                                                    </div>
                                                    <div
                                                        style={{
                                                            display: "flex",
                                                            gap: 6,
                                                            alignItems:
                                                                "center",
                                                        }}
                                                    >
                                                        <button
                                                            className={`orch-btn tiny ${col.is_active ? "primary" : "ghost"}`}
                                                            onClick={() =>
                                                                toggleColumnActive(
                                                                    col.column_name,
                                                                )
                                                            }
                                                            style={{
                                                                padding:
                                                                    "4px 10px",
                                                                fontSize: 10,
                                                                borderRadius: 8,
                                                            }}
                                                        >
                                                            {col.is_active
                                                                ? "ACTIVE"
                                                                : "OFF"}
                                                        </button>
                                                        <FluentSelect
                                                            value={
                                                                col.severity ||
                                                                "ERROR"
                                                            }
                                                            onChange={(e) =>
                                                                changeColumnSeverity(
                                                                    col.column_name,
                                                                    e.target
                                                                        .value,
                                                                )
                                                            }
                                                            options={[
                                                                {
                                                                    value: "ERROR",
                                                                    label: "ERR",
                                                                },
                                                                {
                                                                    value: "WARN",
                                                                    label: "WRN",
                                                                },
                                                                {
                                                                    value: "INFO",
                                                                    label: "INF",
                                                                },
                                                            ]}
                                                            style={{
                                                                minWidth: 70,
                                                            }}
                                                        />
                                                    </div>
                                                </div>
                                                <div className="dq-col-rule">
                                                    <input
                                                        className="orch-input"
                                                        placeholder="e.g. NOT_NULL, REGEX(^[A-Z])"
                                                        value={
                                                            editingRuleDrafts[
                                                                col.column_name
                                                            ] !== undefined
                                                                ? editingRuleDrafts[
                                                                      col
                                                                          .column_name
                                                                  ]
                                                                : col.dq_rules ||
                                                                  ""
                                                        }
                                                        onChange={(e) =>
                                                            setEditingRuleDrafts(
                                                                (prev) => ({
                                                                    ...prev,
                                                                    [col.column_name]:
                                                                        e.target
                                                                            .value,
                                                                }),
                                                            )
                                                        }
                                                        style={{
                                                            background:
                                                                "var(--surface2)",
                                                            borderColor:
                                                                "transparent",
                                                            width: "100%",
                                                        }}
                                                    />
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>

                            <div
                                className="step-footer"
                                style={{
                                    padding: "20px 32px",
                                    background: "var(--surface2)",
                                    marginTop: 0,
                                    borderTop: "1px solid var(--border)",
                                    display: "flex",
                                    justifyContent: "space-between",
                                    alignItems: "center",
                                    borderBottomLeftRadius: 20,
                                    borderBottomRightRadius: 20,
                                }}
                            >
                                <div
                                    style={{
                                        fontSize: 12,
                                        color: "var(--text3)",
                                        fontWeight: 600,
                                    }}
                                >
                                    {!editingConfigLoading &&
                                        !dqLoading &&
                                        `${editingConfigColumns.length} columns defined`}
                                </div>
                                <div style={{ display: "flex", gap: 12 }}>
                                    <button
                                        className="orch-btn ghost"
                                        onClick={() => {
                                            setEditingConfigDataset(null);
                                            setShowDQPanel(false);
                                        }}
                                    >
                                        Cancel
                                    </button>
                                    <button
                                        className="orch-btn primary"
                                        onClick={() => saveDqConfig()}
                                        disabled={
                                            editingConfigSaving ||
                                            editingConfigLoading ||
                                            dqLoading
                                        }
                                        style={{ fontWeight: 800 }}
                                    >
                                        {editingConfigSaving
                                            ? "Applying..."
                                            : "Apply DQ Rules"}
                                    </button>
                                </div>
                            </div>
                        </motion.div>
                    </div>,
                    document.body,
                )}
        </div>
    );
}
