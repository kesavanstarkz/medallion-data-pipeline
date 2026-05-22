import React, { useEffect, useMemo, useRef, useState } from "react";
import {
    FiActivity,
    FiCheck,
    FiCloud,
    FiCpu,
    FiDatabase,
    FiFile,
    FiFolder,
    FiLink,
    FiSearch,
    FiSettings,
    FiZap,
    FiRefreshCw,
    FiCopy,
    FiEdit2,
    FiPlus,
    FiAlertCircle,
    FiUploadCloud,
    FiChevronDown,
    FiChevronRight,
    FiGitBranch,
    FiClock,
    FiLayers,
    FiEye,
    FiSave,
    FiShield,
} from "react-icons/fi";
import CloudPortalScanModal from "./orchestration/CloudPortalScanModal";
import PipelineFlowCanvas from "./orchestration/PipelineFlowCanvas";
import { useApi, apiUrl } from "../hooks/useApi";
import {
    normalizeFabricWorkspace,
    normalizeFabricPipeline,
} from "../utils/fabricContext";
import DynamicPreviewTable from "./DynamicPreviewTable";
import "reactflow/dist/style.css";
import "./PipelineIntelligence.css";

const STRATEGIES = [
    {
        id: "REUSE",
        label: "Reuse Existing",
        icon: <FiZap />,
        desc: "Use orchestration as-is, updating only metadata and parameters.",
    },
    {
        id: "CLONE",
        label: "Clone Pipeline",
        icon: <FiCopy />,
        desc: "Duplicate the pipeline within the workspace for this execution.",
    },
    {
        id: "MODIFY_SOURCE",
        label: "Modify Source",
        icon: <FiEdit2 />,
        desc: "Replace the pipeline source connector and configuration.",
    },
    {
        id: "MODIFY_SINK",
        label: "Modify Sink",
        icon: <FiRefreshCw />,
        desc: "Replace the pipeline sink connector and configuration.",
    },
    {
        id: "CREATE_NEW",
        label: "Create New",
        icon: <FiPlus />,
        desc: "Deploy a completely new pipeline item from an external package.",
    },
];

const TARGETS = [
    {
        id: "aws",
        sourceType: "AWS",
        label: "AWS Platform",
        icon: <FiCloud />,
        scan: true,
    },
    {
        id: "azure",
        sourceType: "AZURE",
        label: "Azure Platform",
        icon: <FiCloud />,
        scan: true,
    },
    {
        id: "fabric",
        sourceType: "FABRIC",
        label: "Microsoft Fabric",
        icon: <FiZap />,
        scan: true,
    },
    {
        id: "s3",
        sourceType: "S3",
        label: "Amazon S3",
        icon: <FiDatabase />,
        scan: true,
    },
    {
        id: "adls",
        sourceType: "ADLS",
        label: "Azure Data Lake",
        icon: <FiDatabase />,
        scan: true,
    },
    {
        id: "api",
        sourceType: "REST_API",
        label: "REST API",
        icon: <FiLink />,
        scan: false,
    },
    {
        id: "local",
        sourceType: "LOCAL",
        label: "Local Files",
        icon: <FiFolder />,
        scan: false,
    },
];

const HIDE_TEMP_INTELLIGENCE_ACTIONS = true;

function JsonBlock({ value }) {
    return (
        <pre className="pi-json">{JSON.stringify(value || {}, null, 2)}</pre>
    );
}

function highlightJsonLine(line, searchTerm) {
    const escaped = line
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");

    let html = escaped
        .replace(/(".*?")(\s*:)/g, '<span class="pi-json-key">$1</span>$2')
        .replace(/:\s(".*?")/g, ': <span class="pi-json-string">$1</span>')
        .replace(/\b(true|false)\b/g, '<span class="pi-json-boolean">$1</span>')
        .replace(/\b(null)\b/g, '<span class="pi-json-null">$1</span>')
        .replace(
            /:\s(-?\d+(?:\.\d+)?)/g,
            ': <span class="pi-json-number">$1</span>',
        );

    if (searchTerm) {
        const safeSearch = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        html = html.replace(new RegExp(safeSearch, "gi"), "<mark>$&</mark>");
    }
    return html;
}

function SearchableJsonPanel({ title, value, defaultOpen = false }) {
    const [open, setOpen] = useState(defaultOpen);
    const [search, setSearch] = useState("");
    const serialized = useMemo(
        () => JSON.stringify(value || {}, null, 2),
        [value],
    );
    const lines = useMemo(() => serialized.split("\n"), [serialized]);

    return (
        <div className="pi-json-panel">
            <button
                className="pi-json-panel-header"
                onClick={() => setOpen((state) => !state)}
            >
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
                            <div
                                key={`${title}-${index}`}
                                dangerouslySetInnerHTML={{
                                    __html: highlightJsonLine(line, search),
                                }}
                            />
                        ))}
                    </pre>
                </div>
            )}
        </div>
    );
}

function getEvidenceValue(value) {
    if (value == null) return null;
    if (Array.isArray(value))
        return value.map(getEvidenceValue).filter((item) => item != null);
    if (typeof value === "object") {
        if (Object.prototype.hasOwnProperty.call(value, "value"))
            return value.value;
        return null;
    }
    return value;
}

function renderValue(value, fallback = "Not present") {
    const resolved = getEvidenceValue(value);
    if (resolved == null || resolved === "") return fallback;
    if (Array.isArray(resolved)) {
        const printable = resolved
            .filter((item) => item != null && item !== "")
            .map((item) =>
                typeof item === "object" ? JSON.stringify(item) : String(item),
            );
        return printable.length ? printable.join(", ") : fallback;
    }
    if (typeof resolved === "object") return JSON.stringify(resolved);
    return String(resolved);
}

function renderEvidenceMeta(value) {
    if (
        !value ||
        typeof value !== "object" ||
        Array.isArray(value) ||
        !Object.prototype.hasOwnProperty.call(value, "status")
    ) {
        return null;
    }
    return (
        <div className="pi-evidence-meta">
            <span>{value.status}</span>
            {typeof value.confidence === "number" && (
                <span>{Math.round(value.confidence * 100)}%</span>
            )}
            {value.evidence && <span>{value.evidence}</span>}
        </div>
    );
}

function renderListItems(items, formatter) {
    if (!Array.isArray(items) || !items.length) return null;
    return items.map((item, index) => (
        <div key={index}>{formatter(item, index)}</div>
    ));
}

function prettyLabel(value) {
    return String(value || "")
        .replace(/_/g, " ")
        .replace(/\b\w/g, (char) => char.toUpperCase());
}

function compactValue(value) {
    if (value == null || value === "") return "Not captured";
    if (typeof value === "boolean") return value ? "Yes" : "No";
    if (Array.isArray(value))
        return value.length ? value.join(", ") : "Not captured";
    if (typeof value === "object") return JSON.stringify(value);
    return String(value);
}

function Tag({ active, children }) {
    return (
        <span className={`pi-tag ${active === false ? "inactive" : "active"}`}>
            {children}
        </span>
    );
}

function hasApiScanDetails(apiSources = []) {
    return (apiSources || []).some((source) => {
        const endpoints = Array.isArray(source.endpoints)
            ? source.endpoints
            : String(source.endpoints || "")
                  .split(",")
                  .map((item) => item.trim())
                  .filter(Boolean);
        return !!source.base_url && endpoints.length > 0;
    });
}

function CollapsibleCard({ title, icon, children, defaultOpen = true }) {
    const [isOpen, setIsOpen] = useState(defaultOpen);
    return (
        <div className={`pi-card pi-collapsible ${isOpen ? "open" : "closed"}`}>
            <div
                className="pi-card-title collapsible-header"
                onClick={() => setIsOpen(!isOpen)}
                style={{
                    cursor: "pointer",
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                }}
            >
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    {icon} {title}
                </div>
                {isOpen ? <FiChevronDown /> : <FiChevronRight />}
            </div>
            {isOpen && (
                <div className="collapsible-content animate-in">{children}</div>
            )}
        </div>
    );
}

export default function PipelineIntelligence({
    clientName,
    initialData,
    clientSourceTypes = [],
    currentSourceType = "",
    apiSources = [],
    fabricDiscoveryData = null,
    fabricMode = "DISCOVERY",
    selectedPlatform = "",
    selectedWorkspace = null,
    setSelectedWorkspace = () => {},
    selectedPipeline = null,
    setSelectedPipeline = () => {},
    selectedDeploymentStrategy = null,
    setSelectedDeploymentStrategy = () => {},
    onScanComplete,
    onConfirm,
}) {
    const [data, setData] = useState(initialData || null);
    const [loading, setLoading] = useState(false);
    const [scanInProgress, setScanInProgress] = useState(false);
    const [analyzing, setAnalyzing] = useState(false);
    const [error, setError] = useState(null);
    const [target, setTarget] = useState(
        initialData?.ingestion_details?.target || "aws",
    );
    const { call } = useApi();
    const [useCloudLlm, setUseCloudLlm] = useState(true);
    const [showCloudScanModal, setShowCloudScanModal] = useState(false);
    const [scanResults, setScanResults] = useState(null);
    const [deploymentStrategy, setDeploymentStrategyState] = useState(
        selectedDeploymentStrategy || initialData?.deploymentStrategy || null,
    );
    const setDeploymentStrategy = (strategy) => {
        setDeploymentStrategyState(strategy);
        setSelectedDeploymentStrategy(strategy);
    };
    const [runtimeAnalysis, setRuntimeAnalysis] = useState(null);
    const [runtimeLoading, setRuntimeLoading] = useState(false);
    const [runtimeError, setRuntimeError] = useState(null);
    const [runtimePermissionDetail, setRuntimePermissionDetail] =
        useState(null);
    const [fabricAccessToken, setFabricAccessToken] = useState(
        initialData?.__fabric_access_token || null,
    );
    const [fabricTokenValidation, setFabricTokenValidation] = useState(
        initialData?.__fabric_token_validation || null,
    );
    const [runtimePreview, setRuntimePreview] = useState(null);
    const [runtimePreviewLoading, setRuntimePreviewLoading] = useState(false);
    const [runtimePreviewError, setRuntimePreviewError] = useState(null);
    const [runtimeActionMessage, setRuntimeActionMessage] = useState("");
    const [runtimeSaveLoading, setRuntimeSaveLoading] = useState(false);
    const [lastPreviewPayload, setLastPreviewPayload] = useState(null);

    // Data Normalization Helper
    const normalizePreviewData = (data) => {
        if (!data) return null;

        let rows = [];
        let columns = [];

        // 1. Extract rows
        if (data.rows && Array.isArray(data.rows)) rows = data.rows;
        else if (data.sample_rows && Array.isArray(data.sample_rows))
            rows = data.sample_rows;
        else if (Array.isArray(data)) rows = data;
        else if (data.data && Array.isArray(data.data)) rows = data.data;
        else if (data.value && Array.isArray(data.value)) rows = data.value;

        // Helper for type inference
        const inferType = (val) => {
            if (val === null || val === undefined || val === "")
                return "string";
            if (typeof val === "number")
                return Number.isInteger(val) ? "integer" : "float";
            if (typeof val === "boolean") return "boolean";
            const str = String(val).trim();
            if (/^-?\d+$/.test(str)) return "integer";
            if (/^-?\d*\.\d+$/.test(str)) return "float";
            if (/^(true|false|yes|no)$/i.test(str)) return "boolean";
            if (!isNaN(Date.parse(str))) {
                if (str.length > 10) return "timestamp";
                return "date";
            }
            return "string";
        };

        // 2. Extract columns by priority
        // Priority 1: schema_discovery.columns
        if (
            data.schema_discovery?.columns &&
            Array.isArray(data.schema_discovery.columns)
        ) {
            columns = data.schema_discovery.columns.map((col) => {
                let type = (
                    col.data_type ||
                    col.type ||
                    "string"
                ).toLowerCase();
                const colName = col.column_name || col.name || col.displayName;
                if (type === "unknown" && rows.length > 0) {
                    type = inferType(rows[0][colName]);
                }
                return {
                    name: colName,
                    type: type,
                    nullable: col.nullable ?? true,
                };
            });
        }
        // Priority 2: data.columns
        else if (data.columns && Array.isArray(data.columns)) {
            columns = data.columns.map((col) => {
                const colName =
                    typeof col === "string"
                        ? col
                        : col.name || col.column_name || col.displayName;
                let type =
                    typeof col === "object"
                        ? col.type || col.data_type || "string"
                        : "string";
                if (
                    (!type || type === "string" || type === "unknown") &&
                    rows.length > 0
                ) {
                    type = inferType(rows[0][colName]);
                }
                return { name: colName, type, nullable: true };
            });
        }
        // Priority 3: Infer from first row
        else if (rows.length > 0) {
            const firstRow = rows[0];
            columns = Object.keys(firstRow).map((key) => ({
                name: key,
                type: inferType(firstRow[key]),
                nullable: true,
            }));
        }

        return { rows, columns, row_count: rows.length };
    };

    // Save to Target state
    const [showSaveTargetModal, setShowSaveTargetModal] = useState(false);
    const [saveTargetLoading, setSaveTargetLoading] = useState(false);
    const [saveTargetMode, setSaveTargetMode] = useState("Save Full Data");
    const [saveTargetTableName, setSaveTargetTableName] = useState("");
    const [saveTargetSchemaName, setSaveTargetSchemaName] = useState("");
    const [saveTargetPK, setSaveTargetPK] = useState("");
    const [saveTargetPartitionKey, setSaveTargetPartitionKey] = useState("");
    const [saveTargetBatchSize, setSaveTargetBatchSize] = useState(1000);
    const [availableTargets, setAvailableTargets] = useState([]);
    const [selectedTargetId, setSelectedTargetId] = useState("");
    const [saveProgress, setSaveProgress] = useState([]);

    const selectedWorkspaceRef = useRef(selectedWorkspace);
    const selectedPipelineRef = useRef(selectedPipeline);
    const runtimeCaptureRequestRef = useRef(0);
    const runtimePreviewRequestRef = useRef(0);

    const allowedTargets = useMemo(
        () => TARGETS.filter((item) => item.sourceType === "FABRIC"),
        [],
    );
    const selectedTarget = allowedTargets.find((item) => item.id === target);
    const apiDetailsAvailable = hasApiScanDetails(apiSources);
    const selectedRequiresScan = selectedTarget?.sourceType
        ? ["AWS", "AZURE", "FABRIC", "S3", "ADLS"].includes(
              selectedTarget.sourceType,
          ) ||
          (selectedTarget.sourceType === "REST_API" && apiDetailsAvailable)
        : false;

    useEffect(() => {
        if (initialData) {
            setData(initialData);
            if (initialData.__fabric_access_token)
                setFabricAccessToken(initialData.__fabric_access_token);
            if (initialData.__fabric_token_validation)
                setFabricTokenValidation(initialData.__fabric_token_validation);
        } else {
            // Try restoring from sessionStorage if no initialData
            const storedToken = sessionStorage.getItem("fabric_access_token");
            if (storedToken && !fabricAccessToken) {
                setFabricAccessToken(storedToken);
            }
        }
    }, [initialData]);

    // Sync local state to prop changes (e.g. from Stepper restoration)
    useEffect(() => {
        if (
            fabricAccessToken &&
            !sessionStorage.getItem("fabric_access_token")
        ) {
            sessionStorage.setItem("fabric_access_token", fabricAccessToken);
        }
    }, [fabricAccessToken]);

    useEffect(() => {
        selectedWorkspaceRef.current = selectedWorkspace;
        console.log(
            "Current selected workspace:",
            selectedWorkspace?.id || null,
        );
    }, [selectedWorkspace]);

    useEffect(() => {
        selectedPipelineRef.current = selectedPipeline;
    }, [selectedPipeline]);

    useEffect(() => {
        if (
            allowedTargets.length > 0 &&
            !allowedTargets.some((t) => t.id === target)
        ) {
            setTarget(allowedTargets[0].id);
        }
    }, [allowedTargets, target]);

    useEffect(() => {
        if (selectedDeploymentStrategy && selectedDeploymentStrategy !== deploymentStrategy) {
            setDeploymentStrategyState(selectedDeploymentStrategy);
        }
    }, [selectedDeploymentStrategy, deploymentStrategy]);

    useEffect(() => {
        runtimeCaptureRequestRef.current += 1;
        runtimePreviewRequestRef.current += 1;
        setRuntimeAnalysis(null);
        setRuntimePreview(null);
        setRuntimeError(null);
        setRuntimePreviewError(null);
        setRuntimePermissionDetail(null);
        setRuntimeActionMessage("");
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
        setRuntimeActionMessage("");
        setRuntimeLoading(false);
        setRuntimePreviewLoading(false);
        setRuntimeSaveLoading(false);
    }, [selectedPipeline?.id]);

    const resolveConnectionWorkspaceId = (connection = {}) =>
        connection.workspaceId ||
        connection.workspace_id ||
        connection.workspace?.id ||
        connection.artifact?.workspaceId ||
        connection.artifact?.workspace_id ||
        null;

    const validateRuntimeWorkspaceSelection = (connection = {}) => {
        const activeWorkspaceId = selectedWorkspaceRef.current?.id || null;
        if (!activeWorkspaceId) {
            throw new Error(
                "Select a Fabric workspace before using the runtime source.",
            );
        }

        const artifactWorkspaceId = resolveConnectionWorkspaceId(connection);

        if (!connection.artifact_id) {
            console.warn(
                "Runtime source is missing an artifact ID. Some actions may be limited.",
            );
            // We no longer throw here to prevent UI "crashes", but we will disable relevant buttons in the UI.
        }

        return {
            activeWorkspaceId,
            artifactWorkspaceId,
        };
    };

    const buildRuntimePreviewPayload = (
        connection = {},
        schemaDiscovery = {},
    ) => {
        const { activeWorkspaceId, artifactWorkspaceId } =
            validateRuntimeWorkspaceSelection(connection);
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

        console.log("Runtime preview validation:", {
            activeWorkspaceId,
            artifactWorkspaceId,
            artifactId: connection.artifact_id,
            selectedPipelineId: selectedPipelineRef.current?.id || null,
        });
        console.log("Payload workspace:", payload.workspaceId);

        return payload;
    };

    const handleAnalyzePipeline = async (workspace, pipeline) => {
        if (scanInProgress || analyzing) return;
        setAnalyzing(true);
        setError(null);
        const normalizedWorkspace = normalizeFabricWorkspace(workspace);
        const normalizedPipeline = normalizeFabricPipeline(
            pipeline,
            normalizedWorkspace?.workspace_id || normalizedWorkspace?.id,
        );
        setSelectedWorkspace(normalizedWorkspace);
        setSelectedPipeline(normalizedPipeline);

        const workspaceId =
            normalizedWorkspace?.workspace_id || normalizedWorkspace?.id;
        const pipelineItemId =
            normalizedPipeline?.pipeline_item_id || normalizedPipeline?.id;
        const workspaceName = normalizedWorkspace?.workspace_name || "";
        const pipelineName = normalizedPipeline?.pipeline_name || "";

        try {
            const headers = { "Content-Type": "application/json" };
            if (fabricAccessToken)
                headers.Authorization = `Bearer ${fabricAccessToken}`;
            const response = await fetch(apiUrl("/discovery/analyze"), {
                method: "POST",
                headers,
                body: JSON.stringify({
                    client_name: clientName,
                    platform: "FABRIC",
                    source_type: "FABRIC",
                    payload: {
                        workspace_id: workspaceId,
                        workspace_name: workspaceName,
                        pipeline_id: pipelineItemId,
                        pipeline_item_id: pipelineItemId,
                        pipeline_name: pipelineName,
                    },
                    use_cloud_llm: useCloudLlm,
                }),
            });
            if (!response.ok) throw new Error("Analysis failed");
            const result = await response.json();
            const finalResult = {
                ...result,
                scan_status: "success",
                scan_completed: true,
            };
            setData(finalResult);
            onScanComplete?.(finalResult);
        } catch (e) {
            setError(
                "Pipeline analysis failed. Could not extract intelligence metadata.",
            );
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
            const response = await fetch(apiUrl("/discovery/api-scan"), {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ client_name: clientName }),
            });
            if (!response.ok) throw new Error("API scan failed");
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
    const runtimeGraph = runtimeAnalysis?.execution_graph || {
        nodes: [],
        edges: [],
    };
    const runtimeGraphNodes = useMemo(
        () =>
            (runtimeGraph.nodes || []).map((node, index) => ({
                id: node.id,
                data: {
                    label: `${node.label} [${node.status || "Pending"}${node.duration_ms ? ` • ${node.duration_ms} ms` : ""}]`,
                },
                position: {
                    x: 80 + index * 220,
                    y: index % 2 === 0 ? 90 : 210,
                },
                style: {
                    borderRadius: 12,
                    padding: 10,
                    border: "1px solid #cbd5e1",
                    background: "#fff",
                    fontSize: 12,
                    fontWeight: 700,
                    width: 220,
                },
            })),
        [runtimeGraph.nodes],
    );
    const runtimeGraphEdges = useMemo(
        () =>
            (runtimeGraph.edges || []).map((edge) => ({
                id: edge.id,
                source: edge.source,
                target: edge.target,
                animated: false,
                label: (edge.condition || []).join(", ") || undefined,
                style: { stroke: "#64748b" },
                labelStyle: { fontSize: 10, fill: "#64748b" },
            })),
        [runtimeGraph.edges],
    );
    const runtimeDiscovery = runtimeAnalysis?.runtime_source_discovery || {};
    const runtimeSourceConnection = runtimeDiscovery.source_connection || {};
    const runtimeTargetConnection = runtimeDiscovery.target_connection || {};
    const runtimeSchemaDiscovery = runtimeDiscovery.schema_discovery || {};
    const runtimeStatistics = runtimeDiscovery.runtime_statistics || {};

    const saveSchema = useMemo(() => {
        // 1. Try currently previewed data (direct or normalized)
        if (runtimePreview?.columns?.length > 0) {
            return runtimePreview.columns;
        }
        // 2. Try discovered runtime schema
        if (runtimeSchemaDiscovery?.columns?.length > 0) {
            return runtimeSchemaDiscovery.columns.map((c) => ({
                name: c.column_name || c.name,
                type: (c.data_type || c.type || "string").toLowerCase(),
                nullable: c.nullable ?? true,
            }));
        }
        // 3. Fallback to initial scan data structures
        const scanColumns =
            data?.columns ||
            data?.reformatted_config?.columns ||
            data?.schema ||
            data?.reformatted_config?.schema;
        if (
            scanColumns &&
            Array.isArray(scanColumns) &&
            scanColumns.length > 0
        ) {
            return scanColumns
                .map((c) => {
                    const name =
                        typeof c === "string"
                            ? c
                            : c.name || c.column_name || c.displayName;
                    const type =
                        typeof c === "object"
                            ? c.type || c.data_type || "string"
                            : "string";
                    return {
                        name,
                        type: String(type).toLowerCase(),
                        nullable: true,
                    };
                })
                .filter((c) => c.name);
        }
        return [];
    }, [runtimePreview, runtimeSchemaDiscovery, data]);

    const handleRunRuntimeIntelligence = async () => {
        const activeWorkspaceId = selectedWorkspaceRef.current?.id;
        const activePipelineId = selectedPipelineRef.current?.id;
        if (!activeWorkspaceId || !activePipelineId) {
            setRuntimeError(
                "Select and analyze a Fabric pipeline before running runtime capture.",
            );
            return;
        }
        if (!fabricAccessToken) {
            setRuntimeError(
                "Fabric runtime capture requires an active Microsoft Fabric SSO token from the current session.",
            );
            return;
        }

        const requestId = ++runtimeCaptureRequestRef.current;
        setRuntimeLoading(true);
        setRuntimeError(null);
        setRuntimePermissionDetail(null);
        setRuntimeAnalysis(null);
        setRuntimePreview(null);
        setRuntimePreviewError(null);
        setRuntimeActionMessage("");
        try {
            const response = await fetch(
                apiUrl("/discovery/fabric-runtime-intelligence"),
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        Authorization: `Bearer ${fabricAccessToken}`,
                    },
                    body: JSON.stringify({
                        client_name: clientName,
                        workspace_id: activeWorkspaceId,
                        pipeline_id: activePipelineId,
                        existing_analysis: data || {},
                    }),
                },
            );
            const payload = await response.json();
            if (requestId !== runtimeCaptureRequestRef.current) return;
            if (!response.ok) {
                if (payload?.detail && typeof payload.detail === "object") {
                    setRuntimePermissionDetail(payload.detail);
                    throw new Error(
                        payload.detail.message ||
                            "Runtime intelligence capture failed.",
                    );
                }
                throw new Error(
                    payload.detail ||
                        payload.message ||
                        "Runtime intelligence capture failed.",
                );
            }
            if (
                selectedWorkspaceRef.current?.id !== activeWorkspaceId ||
                selectedPipelineRef.current?.id !== activePipelineId
            ) {
                console.log("Ignoring stale runtime capture response", {
                    requestWorkspaceId: activeWorkspaceId,
                    currentWorkspaceId:
                        selectedWorkspaceRef.current?.id || null,
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
                        ...payload.reformatted_config,
                    },
                }));
            }
        } catch (runtimeCaptureError) {
            if (requestId !== runtimeCaptureRequestRef.current) return;
            setRuntimeError(
                runtimeCaptureError?.message ||
                    "Runtime intelligence capture failed.",
            );
        } finally {
            if (requestId === runtimeCaptureRequestRef.current) {
                setRuntimeLoading(false);
            }
        }
    };

    const handlePreviewRuntimeSource = async () => {
        if (
            !runtimeSourceConnection ||
            Object.keys(runtimeSourceConnection).length === 0
        )
            return;
        const requestId = ++runtimePreviewRequestRef.current;
        setRuntimePreviewLoading(true);
        setRuntimePreviewError(null);
        try {
            const payloadBody = buildRuntimePreviewPayload(
                runtimeSourceConnection,
                runtimeSchemaDiscovery,
            );
            const response = await fetch(
                apiUrl("/discovery/fabric-runtime-source-preview"),
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        ...(fabricAccessToken
                            ? { Authorization: `Bearer ${fabricAccessToken}` }
                            : {}),
                    },
                    body: JSON.stringify(payloadBody),
                },
            );
            const payload = await response.json();
            if (requestId !== runtimePreviewRequestRef.current) return;
            if (!response.ok) {
                const detail = payload.detail;
                throw new Error(
                    typeof detail === "object"
                        ? detail.message || JSON.stringify(detail)
                        : detail || "Failed to preview runtime source.",
                );
            }
            setLastPreviewPayload(payload);
            setRuntimePreview(normalizePreviewData(payload));
            return payload; // Return for async callers
        } catch (previewError) {
            if (requestId !== runtimePreviewRequestRef.current)
                throw previewError;
            setRuntimePreview(null);
            setRuntimePreviewError(
                previewError?.message || "Failed to preview runtime source.",
            );
            throw previewError;
        } finally {
            if (requestId === runtimePreviewRequestRef.current) {
                setRuntimePreviewLoading(false);
            }
        }
    };

    const handlePreviewRuntimeTarget = async () => {
        if (
            !runtimeTargetConnection ||
            Object.keys(runtimeTargetConnection).length === 0
        )
            return;
        const requestId = ++runtimePreviewRequestRef.current;
        setRuntimePreviewLoading(true);
        setRuntimePreviewError(null);
        try {
            const payloadBody = buildRuntimePreviewPayload(
                runtimeTargetConnection,
                runtimeSchemaDiscovery,
            );
            const response = await fetch(
                apiUrl("/discovery/fabric-runtime-source-preview"),
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        ...(fabricAccessToken
                            ? { Authorization: `Bearer ${fabricAccessToken}` }
                            : {}),
                    },
                    body: JSON.stringify(payloadBody),
                },
            );
            const payload = await response.json();
            if (requestId !== runtimePreviewRequestRef.current) return;
            if (!response.ok) {
                const detail = payload.detail;
                throw new Error(
                    typeof detail === "object"
                        ? detail.message || JSON.stringify(detail)
                        : detail || "Failed to preview runtime target.",
                );
            }
            setLastPreviewPayload(payload);
            setRuntimePreview(normalizePreviewData(payload));
        } catch (previewError) {
            if (requestId !== runtimePreviewRequestRef.current) return;
            setRuntimePreview(null);
            setRuntimePreviewError(
                previewError?.message || "Failed to preview runtime target.",
            );
        } finally {
            if (requestId === runtimePreviewRequestRef.current) {
                setRuntimePreviewLoading(false);
            }
        }
    };

    const _preparePromotedData = (discovery, connection, previewPayload) => {
        if (!discovery || !Object.keys(discovery).length) return data;
        const { activeWorkspaceId, artifactWorkspaceId } =
            validateRuntimeWorkspaceSelection(connection);

        // If we have pivoted to ADLS, use that metadata; otherwise fallback to detected connection
        const finalSourceType = previewPayload?.source_type || "ADLS";
        const finalSourcePath =
            previewPayload?.source_path ||
            connection.full_path ||
            connection.folder_path ||
            connection.file_name ||
            "";
        const finalFolderPath = previewPayload?.folder_path || "";
        const finalFileName = previewPayload?.file_name || "";
        const sourceFormat = connection.format
            ? [connection.format]
            : data?.file_types || ["CSV"];

        const stagingTable = previewPayload?.staging_table || "";

        return {
            ...(data || {}),
            runtime_source_discovery: {
                ...discovery,
                source_connection: {
                    ...connection,
                    workspace_id: artifactWorkspaceId || activeWorkspaceId,
                },
            },
            ingestion_details: {
                ...(data?.ingestion_details || {}),
                source_type: finalSourceType,
                target: "fabric",
            },
            reformatted_config: {
                ...(data?.reformatted_config || {}),
                source_type: finalSourceType,
                source_path: finalSourcePath,
                folder_path: finalFolderPath,
                source_object: finalFileName,
                staging_table: stagingTable,
                pipeline_name:
                    selectedPipeline?.name ||
                    data?.reformatted_config?.pipeline_name,
                source: {
                    artifact_id: selectedPipeline?.id || connection.artifact_id,
                },
                runtime_ingestion_config: discovery.ingestion_config,
                file_types: sourceFormat,
            },
            source_type: finalSourceType,
            source_path: finalSourcePath,
            folder_path: finalFolderPath,
            source_object: finalFileName,
            discovery_mode: "FABRIC_RUNTIME",
            staging_table: stagingTable,
            file_types: sourceFormat,
        };
    };

    const handleUseRuntimeSource = () => {
        const merged = _preparePromotedData(
            runtimeDiscovery,
            runtimeSourceConnection,
            lastPreviewPayload,
        );
        setData(merged);
        onScanComplete?.(merged);
        setRuntimeActionMessage(
            "Runtime source was applied as the reusable source configuration for the next orchestration steps.",
        );
    };

    const handleSaveRuntimeSource = async () => {
        if (!runtimeDiscovery || !Object.keys(runtimeDiscovery).length) return;
        setRuntimeSaveLoading(true);
        setRuntimeActionMessage("");
        try {
            const { activeWorkspaceId, artifactWorkspaceId } =
                validateRuntimeWorkspaceSelection(runtimeSourceConnection);
            const response = await fetch(
                apiUrl("/discovery/fabric-runtime-source-save"),
                {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        client_name: clientName,
                        workspace_id: artifactWorkspaceId || activeWorkspaceId,
                        pipeline_id: selectedPipelineRef.current?.id || null,
                        runtime_source_discovery: {
                            ...runtimeDiscovery,
                            source_connection: {
                                ...runtimeSourceConnection,
                                workspace_id:
                                    artifactWorkspaceId || activeWorkspaceId,
                                artifact_id:
                                    selectedPipeline?.id ||
                                    runtimeSourceConnection.artifact_id,
                            },
                        },
                    }),
                },
            );
            const payload = await response.json();
            if (!response.ok)
                throw new Error(
                    payload.detail || "Failed to save runtime source.",
                );
            setRuntimeActionMessage(
                `Saved reusable source ${payload.source_object || payload.dataset_id} to the registry.`,
            );
        } catch (saveError) {
            setRuntimeActionMessage(
                saveError?.message || "Failed to save runtime source.",
            );
        } finally {
            setRuntimeSaveLoading(false);
        }
    };

    const handleGenerateIngestionConfig = () => {
        if (!runtimeDiscovery?.ingestion_config) return;

        const configStr = JSON.stringify(
            runtimeDiscovery.ingestion_config,
            null,
            2,
        );
        const blob = new Blob([configStr], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = `ingestion_config_${selectedPipeline?.name || "source"}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);

        setRuntimeActionMessage(
            "Reusable ingestion config was generated and downloaded.",
        );
    };

    const handleBuildPipelineFromSource = async () => {
        setRuntimeActionMessage("Checking data staging status...");

        // Ensure we have a staging table in NeonDB (Preview step)
        let currentPayload = lastPreviewPayload;
        if (!currentPayload || !currentPayload.staging_table) {
            setRuntimeActionMessage(
                "Staging data in NeonDB for preview/export...",
            );
            try {
                currentPayload = await handlePreviewRuntimeSource();
            } catch (err) {
                setRuntimeActionMessage("Failed to stage data: " + err.message);
                return;
            }
        }

        setRuntimeActionMessage(
            "Exporting runtime data to ADLS for orchestration...",
        );
        try {
            const promoteRes = await call(
                "/discovery/fabric-runtime-promote-v2",
                "POST",
                {
                    client_name: clientName,
                    pipeline_name: selectedPipeline?.name || "fabric_pipeline",
                    staging_table: currentPayload.staging_table,
                },
            );

            if (promoteRes.status !== "SUCCESS")
                throw new Error("Promotion failed");

            setRuntimeActionMessage(
                "Data exported to ADLS. Registering orchestration source...",
            );

            // Pivot to ADLS source - this reuses the existing ADLS orchestration path
            const adlsSource = {
                ...currentPayload,
                source_type: "ADLS",
                source_path: promoteRes.source_path,
                folder_path: promoteRes.folder_path,
                file_name: promoteRes.file_name,
                file_format: promoteRes.file_format,
                staging_table: "", // Clear staging table as we are now file-based
            };

            const merged = _preparePromotedData(
                runtimeDiscovery,
                runtimeSourceConnection,
                adlsSource,
            );
            setData(merged);
            onScanComplete?.(merged);

            try {
                await call("/orchestrate/save-master-config", "POST", {
                    client_name: clientName,
                    reformatted_config: merged.reformatted_config,
                });
            } catch (saveErr) {
                console.warn(
                    "Failed to auto-persist master config, proceeding anyway:",
                    saveErr,
                );
            }

            setRuntimeActionMessage(
                "Success! Pipeline source promoted to ADLS. Redirecting to orchestration flow...",
            );
            setTimeout(() => {
                onConfirm?.({
                    ...merged,
                    deploymentStrategy: deploymentStrategy || "REUSE",
                    platform: "FABRIC",
                });
            }, 1500);
        } catch (err) {
            setRuntimeActionMessage(
                "Failed to promote to ADLS: " + err.message,
            );
            console.error("Promotion error:", err);
        }
    };

    const handleUseRuntimeTarget = () => {
        if (!runtimeDiscovery || !Object.keys(runtimeDiscovery).length) return;
        const { activeWorkspaceId, artifactWorkspaceId } =
            validateRuntimeWorkspaceSelection(runtimeTargetConnection);
        const targetPath =
            runtimeTargetConnection.full_path ||
            runtimeTargetConnection.folder_path ||
            runtimeTargetConnection.file_name ||
            "";

        const merged = {
            ...(data || {}),
            ingestion_details: {
                ...(data?.ingestion_details || {}),
                target_type: "FABRIC",
                target: "fabric",
                target_path: targetPath,
            },
            reformatted_config: {
                ...(data?.reformatted_config || {}),
                target_type: "FABRIC",
                target_path: targetPath,
            },
        };
        setData(merged);
        onScanComplete?.(merged);
        setRuntimeActionMessage("Runtime target configuration applied.");
    };

    const handleSaveRuntimeTarget = async () => {
        // Re-use handleSaveRuntimeSource but it already saves both source and target in the backend
        await handleSaveRuntimeSource();
        setRuntimeActionMessage("Runtime target metadata saved to registry.");
    };

    const handleGenerateTargetConfig = () => {
        if (!runtimeTargetConnection) return;
        const configStr = JSON.stringify(runtimeTargetConnection, null, 2);
        const blob = new Blob([configStr], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = `target_config_${selectedPipeline?.name || "target"}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        setRuntimeActionMessage("Target connection config generated.");
    };

    const handleBuildPipelineFromTarget = () => {
        handleUseRuntimeTarget();
        setRuntimeActionMessage(
            "Runtime target promoted. Navigating to orchestration...",
        );
        setTimeout(() => {
            onConfirm({
                ...data,
                deploymentStrategy: deploymentStrategy || "REUSE",
                selectedWorkspace,
                selectedPipeline,
            });
        }, 1500);
    };

    const handleSaveToTarget = async () => {
        if (!clientName) {
            setRuntimePreviewError("No client selected.");
            return;
        }
        if (!runtimeSourceConnection?.artifact_id) {
            setRuntimePreviewError("No source selected.");
            return;
        }

        setRuntimePreviewError(null);
        setSaveTargetLoading(true);
        try {
            const response = await fetch(
                apiUrl(`/config/targets/${clientName}`),
            );
            const targets = await response.json();
            setAvailableTargets(targets || []);

            if (!targets || targets.length === 0) {
                setRuntimePreviewError(
                    `No target configured for client '${clientName}'.`,
                );
                return;
            }

            setSelectedTargetId(targets[0].target_id);
            setSaveTargetTableName(
                selectedPipeline?.name?.replace(/\s+/g, "_") || "target_table",
            );
            setShowSaveTargetModal(true);
        } catch (e) {
            setRuntimePreviewError("Failed to fetch target configurations.");
        } finally {
            setSaveTargetLoading(false);
        }
    };

    const executeSaveToTarget = async () => {
        setSaveTargetLoading(true);
        setSaveProgress(["Connecting to target...", "Validating schema..."]);

        try {
            const payload = {
                client_name: clientName,
                source_name: selectedPipeline?.name,
                target_id: selectedTargetId,
                schema: saveSchema,
                rows: runtimePreview?.rows || [],
                save_mode: saveTargetMode,
                table_name: saveTargetTableName,
                schema_name: saveTargetSchemaName,
                batch_size: saveTargetBatchSize,
                primary_key: saveTargetPK,
                partition_key: saveTargetPartitionKey,
            };

            setSaveProgress((prev) => [
                ...prev,
                "Creating table...",
                "Writing data...",
            ]);

            const response = await fetch(apiUrl("/config/targets/save-data"), {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });

            const result = await response.json();

            if (response.ok) {
                setSaveProgress((prev) => [...prev, "Finalizing..."]);
                setTimeout(() => {
                    setShowSaveTargetModal(false);
                    setRuntimeActionMessage(
                        `Data successfully saved to target. Rows: ${result.rows_written || 0}, Time: ${result.execution_time}`,
                    );
                }, 1000);
            } else {
                setRuntimePreviewError(
                    result.detail || "Target connection failed.",
                );
                setShowSaveTargetModal(false);
            }
        } catch (e) {
            setRuntimePreviewError("An error occurred during save operation.");
            setShowSaveTargetModal(false);
        } finally {
            setSaveTargetLoading(false);
        }
    };

    return (
        <div className="pipeline-intelligence-container">
            <div className="pi-header">
                <h2>Microsoft Fabric Intelligence</h2>
                <p className="step-sub">
                    Authenticate to Azure, scan Fabric workspaces, inspect
                    data pipelines, and choose a reuse strategy.
                </p>
            </div>

            <div className="pi-target-grid">
                {allowedTargets.map((item) => (
                    <button
                        key={item.id}
                        className={`pi-target-card ${target === item.id ? "selected" : ""}`}
                        onClick={() => {
                            setTarget(item.id);
                            setData(null);
                            setScanResults(null);
                        }}
                        disabled={loading || scanInProgress || analyzing}
                    >
                        <span className="pi-target-icon">{item.icon}</span>
                        <span>{item.label}</span>
                    </button>
                ))}
            </div>

            <div className="pi-scan-trigger">
                {selectedTarget?.sourceType !== "LOCAL" && (
                    <label
                        className="pi-checkbox-row"
                        style={{ marginBottom: 20 }}
                    >
                        <input
                            type="checkbox"
                            checked={useCloudLlm}
                            onChange={(e) => setUseCloudLlm(e.target.checked)}
                            disabled={loading || scanInProgress || analyzing}
                        />
                        <span>
                            Use GPT API to extract ingestion, source, and DQ
                            rules
                        </span>
                    </label>
                )}

                {selectedTarget?.sourceType !== "REST_API" && (
                    <button
                        className="pi-btn-confirm"
                        onClick={() => setShowCloudScanModal(true)}
                        disabled={
                            loading ||
                            scanInProgress ||
                            analyzing ||
                            !selectedRequiresScan
                        }
                    >
                        <FiSearch /> Run Framework Scan
                    </button>
                )}

                {selectedTarget?.sourceType === "REST_API" &&
                    apiDetailsAvailable && (
                        <button
                            className="pi-btn-confirm"
                            onClick={handleManualApiScan}
                            disabled={loading || scanInProgress || analyzing}
                        >
                            <FiSearch /> Scan REST API
                        </button>
                    )}
            </div>

            {selectedPlatform === "FABRIC" &&
                scanResults &&
                !data &&
                !loading &&
                !analyzing && (
                    <div className="fabric-explorer-section animate-in">
                        <div className="fabric-explorer-header">
                            <h3 className="fabric-explorer-title">
                                <FiFolder color="#2563eb" /> Discovered Fabric
                                Workspaces
                            </h3>
                            <span className="fabric-explorer-count">
                                {scanResults.length} Workspaces Found
                            </span>
                        </div>

                        {scanResults.length > 0 ? (
                            <div className="workspace-grid">
                                {scanResults.map((ws) => (
                                    <div key={ws.id} className="workspace-card">
                                        <div className="workspace-card-header">
                                            <div className="workspace-info">
                                                <div className="workspace-icon">
                                                    <FiDatabase
                                                        color="#2563eb"
                                                        size={18}
                                                    />
                                                </div>
                                                <div className="workspace-text">
                                                    <div
                                                        className="workspace-name"
                                                        title={
                                                            ws.name ||
                                                            ws.displayName
                                                        }
                                                    >
                                                        {ws.name ||
                                                            ws.displayName}
                                                    </div>
                                                    <div className="workspace-id">
                                                        ID: {ws.id}
                                                    </div>
                                                </div>
                                            </div>
                                            <div className="pipeline-count-tag">
                                                {
                                                    (
                                                        ws.pipelines ||
                                                        ws.data_pipelines ||
                                                        []
                                                    ).length
                                                }{" "}
                                                Pipelines
                                            </div>
                                        </div>
                                        <div className="pipeline-list">
                                            {(
                                                ws.pipelines ||
                                                ws.data_pipelines ||
                                                []
                                            ).map((pl) => (
                                                <div
                                                    key={pl.id}
                                                    className="pipeline-item"
                                                >
                                                    <div className="pipeline-info">
                                                        <FiActivity
                                                            color="#6366f1"
                                                            size={16}
                                                        />
                                                        <div
                                                            className="pipeline-name"
                                                            title={
                                                                pl.name ||
                                                                pl.displayName
                                                            }
                                                        >
                                                            {pl.name ||
                                                                pl.displayName}
                                                        </div>
                                                    </div>
                                                    <button
                                                        className="orch-btn primary tiny"
                                                        onClick={() =>
                                                            handleAnalyzePipeline(
                                                                ws,
                                                                pl,
                                                            )
                                                        }
                                                    >
                                                        Analyze
                                                    </button>
                                                </div>
                                            ))}
                                            {!(
                                                ws.pipelines ||
                                                ws.data_pipelines ||
                                                []
                                            ).length && (
                                                <div className="empty-pipelines">
                                                    No pipelines discovered in
                                                    this workspace.
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        ) : (
                            <div className="empty-discovery">
                                <FiSearch size={40} color="#94a3b8" />
                                <div className="empty-discovery-title">
                                    No Workspaces Discovered
                                </div>
                                <p className="empty-discovery-sub">
                                    The scan completed but no accessible Fabric
                                    workspaces were found.
                                </p>
                            </div>
                        )}
                    </div>
                )}

            {analyzing && (
                <div className="pi-loading" style={{ marginTop: 30 }}>
                    <div className="pi-spinner" />
                    <p>
                        Extracting deep intelligence from{" "}
                        <strong>{selectedPipeline?.name}</strong>...
                    </p>
                </div>
            )}

            {loading && (
                <div className="pi-loading">
                    <div className="pi-spinner" />
                    <p>Scanning live environment...</p>
                </div>
            )}

            {error && (
                <div className="pi-error">
                    <FiAlertCircle /> {error}
                </div>
            )}

            {/* INTELLIGENCE & STRATEGY */}
            {data && !analyzing && !loading && (
                <>
                    {selectedPlatform === "FABRIC" && (
                        <div
                            className="pi-card pi-wide"
                            style={{
                                border: "1px solid #3b82f6",
                                background: "rgba(59, 130, 246, 0.05)",
                                marginTop: 24,
                            }}
                        >
                            <div
                                className="pi-card-title"
                                style={{ color: "#2563eb" }}
                            >
                                <FiSettings /> PIPELINE REUSE STRATEGY
                            </div>
                            <div className="pi-strategy-grid">
                                {STRATEGIES.map((s) => (
                                    <button
                                        key={s.id}
                                        className={`pi-strategy-card ${deploymentStrategy === s.id ? "selected" : ""}`}
                                        onClick={() =>
                                            setDeploymentStrategy(s.id)
                                        }
                                    >
                                        <div className="pi-strategy-icon">
                                            {s.icon}
                                        </div>
                                        <div className="pi-strategy-info">
                                            <div className="pi-strategy-label">
                                                {s.label}
                                            </div>
                                            <div className="pi-strategy-desc">
                                                {s.desc}
                                            </div>
                                        </div>
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}

                    {/* SAVE TO TARGET MODAL */}
                    {showSaveTargetModal && (
                        <div className="pi-modal-overlay">
                            <div
                                className="pi-modal-content"
                                style={{ maxWidth: 600 }}
                            >
                                <div className="pi-modal-header">
                                    <h3>
                                        <FiDatabase /> Save Data to Target
                                    </h3>
                                    <button
                                        className="pi-modal-close"
                                        onClick={() =>
                                            setShowSaveTargetModal(false)
                                        }
                                    >
                                        &times;
                                    </button>
                                </div>
                                <div className="pi-modal-body">
                                    {/* DETECTED COLUMNS DEBUG */}
                                    <div
                                        className="pi-alert info"
                                        style={{
                                            marginBottom: 20,
                                            background: "#f0f9ff",
                                            borderColor: "#bae6fd",
                                            color: "#0369a1",
                                        }}
                                    >
                                        <div
                                            style={{
                                                fontWeight: 800,
                                                marginBottom: 8,
                                                display: "flex",
                                                alignItems: "center",
                                                gap: 6,
                                            }}
                                        >
                                            <FiLayers /> Detected Schema:
                                        </div>
                                        <div
                                            style={{
                                                display: "grid",
                                                gridTemplateColumns: "1fr 1fr",
                                                gap: 8,
                                            }}
                                        >
                                            {saveSchema.map((c) => (
                                                <div
                                                    key={c.name}
                                                    style={{
                                                        display: "flex",
                                                        alignItems: "center",
                                                        gap: 8,
                                                        fontSize: 11,
                                                        background: "#fff",
                                                        padding: "4px 8px",
                                                        borderRadius: 6,
                                                        border: "1px solid #e0f2fe",
                                                    }}
                                                >
                                                    <FiCheck color="#10b981" />
                                                    <span
                                                        style={{
                                                            fontWeight: 700,
                                                        }}
                                                    >
                                                        {c.name}
                                                    </span>
                                                    <span
                                                        style={{
                                                            opacity: 0.6,
                                                            fontSize: 10,
                                                        }}
                                                    >
                                                        (
                                                        {String(
                                                            c.type,
                                                        ).toUpperCase()}
                                                        )
                                                    </span>
                                                </div>
                                            ))}
                                            {saveSchema.length === 0 && (
                                                <div
                                                    style={{
                                                        gridColumn: "1/-1",
                                                        display: "flex",
                                                        flexDirection: "column",
                                                        gap: 10,
                                                        width: "100%",
                                                    }}
                                                >
                                                    <span
                                                        style={{
                                                            fontStyle: "italic",
                                                            fontSize: 13,
                                                            color: "#ef4444",
                                                        }}
                                                    >
                                                        None detected. No schema
                                                        could be inferred from
                                                        capture or scan results.
                                                    </span>
                                                    <button
                                                        className="pi-btn-confirm"
                                                        style={{
                                                            display: HIDE_TEMP_INTELLIGENCE_ACTIONS ? "none" : undefined,
                                                            width: "fit-content",
                                                            padding: "6px 14px",
                                                            fontSize: 12,
                                                            background:
                                                                "#3b82f6",
                                                        }}
                                                        onClick={
                                                            handlePreviewRuntimeSource
                                                        }
                                                        disabled={
                                                            runtimePreviewLoading
                                                        }
                                                    >
                                                        <FiEye />{" "}
                                                        {runtimePreviewLoading
                                                            ? "Fetching Live Schema..."
                                                            : "Preview Data Now to Extract Schema"}
                                                    </button>
                                                </div>
                                            )}
                                        </div>
                                    </div>

                                    <div
                                        className="pi-kv-grid"
                                        style={{ marginBottom: 20 }}
                                    >
                                        <div>
                                            <strong>Selected Client:</strong>{" "}
                                            {clientName}
                                        </div>
                                        <div>
                                            <strong>Selected Source:</strong>{" "}
                                            {selectedPipeline?.name}
                                        </div>
                                        <div>
                                            <strong>Target:</strong>
                                            <select
                                                className="orch-input"
                                                value={selectedTargetId}
                                                onChange={(e) =>
                                                    setSelectedTargetId(
                                                        e.target.value,
                                                    )
                                                }
                                                style={{
                                                    padding: "4px 8px",
                                                    marginLeft: 10,
                                                    width: "auto",
                                                }}
                                            >
                                                {availableTargets.map((t) => (
                                                    <option
                                                        key={t.target_id}
                                                        value={t.target_id}
                                                    >
                                                        {t.target_name} (
                                                        {t.target_type})
                                                    </option>
                                                ))}
                                            </select>
                                        </div>
                                    </div>

                                    <div className="pi-form-section">
                                        <label className="pi-form-label">
                                            Choose Save Mode
                                        </label>
                                        <div
                                            className="pi-radio-group"
                                            style={{
                                                display: "grid",
                                                gridTemplateColumns: "1fr 1fr",
                                                gap: 10,
                                            }}
                                        >
                                            {[
                                                "Save Full Data",
                                                "Save Schema Only",
                                                "Create Table Only",
                                                "Append to Existing Table",
                                                "Overwrite Existing Table",
                                                "Upsert / Merge",
                                            ].map((mode) => (
                                                <label
                                                    key={mode}
                                                    className="pi-radio-label"
                                                >
                                                    <input
                                                        type="radio"
                                                        name="saveMode"
                                                        checked={
                                                            saveTargetMode ===
                                                            mode
                                                        }
                                                        onChange={() =>
                                                            setSaveTargetMode(
                                                                mode,
                                                            )
                                                        }
                                                    />
                                                    <span>{mode}</span>
                                                </label>
                                            ))}
                                        </div>
                                    </div>

                                    <div
                                        className="pi-grid"
                                        style={{ marginTop: 20 }}
                                    >
                                        <div className="pi-form-field">
                                            <label>Target Table Name</label>
                                            <input
                                                type="text"
                                                className="orch-input"
                                                value={saveTargetTableName}
                                                onChange={(e) =>
                                                    setSaveTargetTableName(
                                                        e.target.value,
                                                    )
                                                }
                                            />
                                        </div>
                                        <div className="pi-form-field">
                                            <label>Schema Name</label>
                                            <input
                                                type="text"
                                                className="orch-input"
                                                placeholder="dbo / public"
                                                value={saveTargetSchemaName}
                                                onChange={(e) =>
                                                    setSaveTargetSchemaName(
                                                        e.target.value,
                                                    )
                                                }
                                            />
                                        </div>
                                        <div className="pi-form-field">
                                            <label>Batch Size</label>
                                            <input
                                                type="number"
                                                className="orch-input"
                                                value={saveTargetBatchSize}
                                                onChange={(e) =>
                                                    setSaveTargetBatchSize(
                                                        parseInt(
                                                            e.target.value,
                                                        ),
                                                    )
                                                }
                                            />
                                        </div>
                                        {saveTargetMode ===
                                            "Upsert / Merge" && (
                                            <div className="pi-form-field">
                                                <label>
                                                    Primary Key (for upsert)
                                                </label>
                                                <input
                                                    type="text"
                                                    className="orch-input"
                                                    placeholder="id"
                                                    value={saveTargetPK}
                                                    onChange={(e) =>
                                                        setSaveTargetPK(
                                                            e.target.value,
                                                        )
                                                    }
                                                />
                                            </div>
                                        )}
                                    </div>

                                    {saveTargetLoading && (
                                        <div
                                            className="pi-progress-section"
                                            style={{
                                                marginTop: 20,
                                                padding: 15,
                                                background: "#f8fafc",
                                                borderRadius: 8,
                                            }}
                                        >
                                            {saveProgress.map((p, i) => (
                                                <div
                                                    key={i}
                                                    style={{
                                                        fontSize: 12,
                                                        color:
                                                            i ===
                                                            saveProgress.length -
                                                                1
                                                                ? "#2563eb"
                                                                : "#64748b",
                                                        fontWeight:
                                                            i ===
                                                            saveProgress.length -
                                                                1
                                                                ? 700
                                                                : 400,
                                                        display: "flex",
                                                        alignItems: "center",
                                                        gap: 8,
                                                    }}
                                                >
                                                    {i ===
                                                    saveProgress.length - 1 ? (
                                                        <FiRefreshCw className="spinner" />
                                                    ) : (
                                                        <FiCheck color="#10b981" />
                                                    )}{" "}
                                                    {p}
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                                <div className="pi-modal-footer">
                                    <button
                                        className="orch-btn"
                                        onClick={() =>
                                            setShowSaveTargetModal(false)
                                        }
                                    >
                                        Cancel
                                    </button>
                                    <button
                                        className="orch-btn primary"
                                        onClick={executeSaveToTarget}
                                        disabled={
                                            saveTargetLoading ||
                                            !saveTargetTableName ||
                                            saveSchema.length === 0
                                        }
                                    >
                                        {saveTargetLoading
                                            ? "Processing..."
                                            : "Confirm & Save"}
                                    </button>
                                </div>
                            </div>
                        </div>
                    )}

                    <div className="pi-grid"></div>

                    {selectedPlatform === "FABRIC" && (
                        <div
                            className="pi-card pi-wide"
                            style={{ marginTop: 16 }}
                        >
                            <div className="pi-card-title">
                                <FiActivity /> Runtime Execution + Live Capture
                            </div>
                            <div
                                style={{
                                    display: "flex",
                                    justifyContent: "space-between",
                                    alignItems: "center",
                                    gap: 16,
                                    flexWrap: "wrap",
                                }}
                            >
                                <div className="pi-card-content">
                                    Execute the selected Fabric pipeline, poll
                                    live activity runs, capture actual runtime
                                    values, and append runtime intelligence
                                    below.
                                </div>
                                <button
                                    className="pi-btn-confirm"
                                    onClick={handleRunRuntimeIntelligence}
                                    disabled={
                                        runtimeLoading ||
                                        (!selectedWorkspace?.id &&
                                            !data?.workspace_id) ||
                                        (!selectedPipeline?.id &&
                                            !data?.pipeline_id)
                                    }
                                    title={
                                        (!selectedWorkspace?.id &&
                                            !data?.workspace_id) ||
                                        (!selectedPipeline?.id &&
                                            !data?.pipeline_id)
                                            ? "Please select and analyze a pipeline first"
                                            : ""
                                    }
                                >
                                    <FiActivity />{" "}
                                    {runtimeLoading
                                        ? "Running & Capturing..."
                                        : "Run & Capture Runtime Intelligence"}
                                </button>
                            </div>
                            {fabricTokenValidation && (
                                <div
                                    className="pi-alert warning"
                                    style={{ marginTop: 12 }}
                                >
                                    <div>
                                        <strong>Current scopes:</strong>{" "}
                                        {(fabricTokenValidation.scp || []).join(
                                            ", ",
                                        ) || "None"}
                                    </div>
                                    <div>
                                        <strong>Required scopes:</strong>{" "}
                                        {(
                                            fabricTokenValidation.required_scopes ||
                                            []
                                        ).join(", ") || "None"}
                                    </div>
                                    <div>
                                        <strong>Audience:</strong>{" "}
                                        {fabricTokenValidation.aud ||
                                            "Unavailable"}
                                    </div>
                                    {!!(
                                        fabricTokenValidation.missing_scopes ||
                                        []
                                    ).length && (
                                        <div>
                                            <strong>Missing scopes:</strong>{" "}
                                            {(
                                                fabricTokenValidation.missing_scopes ||
                                                []
                                            ).join(", ")}
                                        </div>
                                    )}
                                </div>
                            )}
                            {runtimeError && (
                                <div
                                    className="pi-error"
                                    style={{ marginTop: 12 }}
                                >
                                    <FiAlertCircle /> {runtimeError}
                                </div>
                            )}
                            {runtimePermissionDetail && (
                                <div
                                    className="pi-alert error"
                                    style={{ marginTop: 12 }}
                                >
                                    <div>
                                        <strong>
                                            {runtimePermissionDetail.message}
                                        </strong>
                                    </div>
                                    <div>
                                        <strong>Current scopes:</strong>{" "}
                                        {(
                                            runtimePermissionDetail.current_scopes ||
                                            []
                                        ).join(", ") || "None"}
                                    </div>
                                    <div>
                                        <strong>Required scopes:</strong>{" "}
                                        {(
                                            runtimePermissionDetail.required_scopes ||
                                            []
                                        ).join(", ") || "None"}
                                    </div>
                                    <div>
                                        <strong>Missing scopes:</strong>{" "}
                                        {(
                                            runtimePermissionDetail.missing_scopes ||
                                            []
                                        ).join(", ") || "None"}
                                    </div>
                                    <div>
                                        <strong>Audience:</strong>{" "}
                                        {runtimePermissionDetail.token_audience ||
                                            "Unavailable"}
                                    </div>
                                    <div>
                                        <strong>Tenant:</strong>{" "}
                                        {runtimePermissionDetail.tenant ||
                                            "Unavailable"}
                                    </div>
                                    {runtimePermissionDetail.admin_instructions && (
                                        <div
                                            style={{
                                                marginTop: 8,
                                                whiteSpace: "pre-wrap",
                                            }}
                                        >
                                            {
                                                runtimePermissionDetail.admin_instructions
                                            }
                                        </div>
                                    )}
                                </div>
                            )}
                        </div>
                    )}

                    {selectedPlatform === "FABRIC" && runtimeAnalysis && (
                        <div
                            className="pi-card pi-wide"
                            style={{ marginTop: 16 }}
                        >
                            <div className="pi-card-title">
                                <FiLayers /> Runtime Intelligence Capture
                            </div>
                            <div className="pi-grid">
                                <CollapsibleCard
                                    title="Runtime Execution Summary"
                                    icon={<FiActivity />}
                                >
                                    <div className="pi-kv-grid">
                                        <div>
                                            <strong>Run ID:</strong>{" "}
                                            {runtimeAnalysis.pipeline_run_id}
                                        </div>
                                        <div>
                                            <strong>Status:</strong>{" "}
                                            {runtimeAnalysis.execution_status}
                                        </div>
                                        <div>
                                            <strong>Activities:</strong>{" "}
                                            {runtimeAnalysis.runtime_metrics
                                                ?.total_activities ?? 0}
                                        </div>
                                        <div>
                                            <strong>Retries:</strong>{" "}
                                            {runtimeAnalysis.runtime_metrics
                                                ?.retry_count ?? 0}
                                        </div>
                                    </div>
                                </CollapsibleCard>
                                <CollapsibleCard
                                    title="Runtime Statistics"
                                    icon={<FiSettings />}
                                >
                                    <div className="pi-kv-grid">
                                        {Object.entries(runtimeStatistics).map(
                                            ([key, value]) => (
                                                <div key={key}>
                                                    <strong>
                                                        {prettyLabel(key)}:
                                                    </strong>{" "}
                                                    {compactValue(value)}
                                                </div>
                                            ),
                                        )}
                                    </div>
                                </CollapsibleCard>
                                <div className="pi-card">
                                    <div className="pi-card-title">
                                        Source Connection Card
                                    </div>
                                    <div className="pi-kv-grid">
                                        {Object.entries(
                                            runtimeSourceConnection,
                                        ).map(([key, value]) => (
                                            <div key={key}>
                                                <strong>
                                                    {prettyLabel(key)}:
                                                </strong>{" "}
                                                {compactValue(value)}
                                            </div>
                                        ))}
                                    </div>
                                    <div
                                        style={{
                                            display: "flex",
                                            gap: 10,
                                            flexWrap: "wrap",
                                            marginTop: 14,
                                        }}
                                    >
                                        <button
                                            className="pi-btn-confirm"
                                            style={HIDE_TEMP_INTELLIGENCE_ACTIONS ? { display: "none" } : undefined}
                                            onClick={handlePreviewRuntimeSource}
                                            disabled={
                                                runtimePreviewLoading ||
                                                !runtimeSourceConnection.artifact_id
                                            }
                                        >
                                            <FiEye />{" "}
                                            {runtimePreviewLoading
                                                ? "Loading Preview..."
                                                : "Preview Data"}
                                        </button>
                                        <button
                                            className="pi-btn-confirm"
                                            onClick={handleUseRuntimeSource}
                                            disabled={
                                                !runtimeSourceConnection.artifact_id
                                            }
                                        >
                                            <FiCheck /> Use Source
                                        </button>
                                        <button
                                            className="pi-btn-confirm"
                                            onClick={handleSaveRuntimeSource}
                                            disabled={
                                                runtimeSaveLoading ||
                                                !runtimeSourceConnection.artifact_id
                                            }
                                        >
                                            <FiSave />{" "}
                                            {runtimeSaveLoading
                                                ? "Saving..."
                                                : "Save Source"}
                                        </button>
                                        <button
                                            className="pi-btn-confirm"
                                            onClick={
                                                handleGenerateIngestionConfig
                                            }
                                        >
                                            <FiCopy /> Generate Ingestion Config
                                        </button>
                                        <button
                                            className="pi-btn-confirm"
                                            onClick={
                                                handleBuildPipelineFromSource
                                            }
                                            disabled={
                                                !runtimeSourceConnection.artifact_id
                                            }
                                        >
                                            <FiGitBranch /> Build Pipeline From
                                            Source
                                        </button>
                                        <button
                                            className="pi-btn-confirm"
                                            onClick={handleSaveToTarget}
                                            disabled={saveTargetLoading}
                                        >
                                            <FiDatabase />{" "}
                                            {saveTargetLoading
                                                ? "Validating..."
                                                : "Save to Target"}
                                        </button>
                                    </div>
                                    {!runtimeSourceConnection.artifact_id && (
                                        <div
                                            className="pi-alert warning"
                                            style={{
                                                marginTop: 12,
                                                fontSize: 12,
                                            }}
                                        >
                                            <FiAlertCircle />{" "}
                                            <strong>
                                                Artifact resolution failed:
                                            </strong>{" "}
                                            The runtime source is missing a
                                            Fabric artifact ID. Try identifying
                                            the Lakehouse name in the pipeline
                                            configuration or ensure the source
                                            is accessible.
                                        </div>
                                    )}
                                </div>
                                {runtimeDiscovery.resolution_diagnostics &&
                                    runtimeDiscovery.resolution_diagnostics
                                        .length > 0 && (
                                        <CollapsibleCard
                                            title="Artifact Resolution Diagnostics"
                                            icon={<FiSearch />}
                                            defaultOpen={false}
                                        >
                                            <div className="pi-list">
                                                {renderListItems(
                                                    runtimeDiscovery.resolution_diagnostics,
                                                    (item) => (
                                                        <div
                                                            style={{
                                                                fontSize: 11,
                                                                marginBottom: 4,
                                                            }}
                                                        >
                                                            <span
                                                                style={{
                                                                    fontWeight: 700,
                                                                    color:
                                                                        item.status ===
                                                                        "success"
                                                                            ? "#10b981"
                                                                            : "#f59e0b",
                                                                }}
                                                            >
                                                                [
                                                                {item.strategy
                                                                    .replace(
                                                                        /_/g,
                                                                        " ",
                                                                    )
                                                                    .toUpperCase()}
                                                                ]
                                                            </span>{" "}
                                                            {item.status.toUpperCase()}
                                                            {item.artifact_id && (
                                                                <span>
                                                                    {" "}
                                                                    • ID:{" "}
                                                                    <code
                                                                        style={{
                                                                            background:
                                                                                "#f1f5f9",
                                                                            padding:
                                                                                "2px 4px",
                                                                            borderRadius: 4,
                                                                        }}
                                                                    >
                                                                        {
                                                                            item.artifact_id
                                                                        }
                                                                    </code>
                                                                </span>
                                                            )}
                                                            {item.item_name && (
                                                                <span>
                                                                    {" "}
                                                                    • Match:{" "}
                                                                    <strong>
                                                                        {
                                                                            item.item_name
                                                                        }
                                                                    </strong>
                                                                </span>
                                                            )}
                                                        </div>
                                                    ),
                                                )}
                                            </div>
                                        </CollapsibleCard>
                                    )}
                                <div className="pi-card">
                                    <div className="pi-card-title">
                                        Target Connection Card
                                    </div>
                                    <div className="pi-kv-grid">
                                        {Object.entries(
                                            runtimeTargetConnection,
                                        ).map(([key, value]) => (
                                            <div key={key}>
                                                <strong>
                                                    {prettyLabel(key)}:
                                                </strong>{" "}
                                                {compactValue(value)}
                                            </div>
                                        ))}
                                    </div>
                                    <div
                                        style={{
                                            display: "flex",
                                            gap: 10,
                                            flexWrap: "wrap",
                                            marginTop: 14,
                                        }}
                                    >
                                        <button
                                            className="pi-btn-confirm"
                                            style={HIDE_TEMP_INTELLIGENCE_ACTIONS ? { display: "none" } : undefined}
                                            onClick={handlePreviewRuntimeTarget}
                                            disabled={
                                                runtimePreviewLoading ||
                                                !runtimeTargetConnection.artifact_id
                                            }
                                        >
                                            <FiEye />{" "}
                                            {runtimePreviewLoading
                                                ? "Loading Preview..."
                                                : "Preview Data"}
                                        </button>
                                        <button
                                            className="pi-btn-confirm"
                                            style={HIDE_TEMP_INTELLIGENCE_ACTIONS ? { display: "none" } : undefined}
                                            onClick={handleUseRuntimeTarget}
                                            disabled={
                                                !runtimeTargetConnection.artifact_id
                                            }
                                        >
                                            <FiCheck /> Use Target
                                        </button>
                                        <button
                                            className="pi-btn-confirm"
                                            style={HIDE_TEMP_INTELLIGENCE_ACTIONS ? { display: "none" } : undefined}
                                            onClick={handleSaveRuntimeTarget}
                                            disabled={
                                                runtimeSaveLoading ||
                                                !runtimeTargetConnection.artifact_id
                                            }
                                        >
                                            <FiSave />{" "}
                                            {runtimeSaveLoading
                                                ? "Saving..."
                                                : "Save Target"}
                                        </button>
                                        <button
                                            className="pi-btn-confirm"
                                            style={HIDE_TEMP_INTELLIGENCE_ACTIONS ? { display: "none" } : undefined}
                                            onClick={handleGenerateTargetConfig}
                                        >
                                            <FiCopy /> Generate Target Config
                                        </button>
                                        <button
                                            className="pi-btn-confirm"
                                            style={HIDE_TEMP_INTELLIGENCE_ACTIONS ? { display: "none" } : undefined}
                                            onClick={
                                                handleBuildPipelineFromTarget
                                            }
                                            disabled={
                                                !runtimeTargetConnection.artifact_id
                                            }
                                        >
                                            <FiGitBranch /> Build Pipeline From
                                            Target
                                        </button>
                                    </div>
                                </div>
                                <CollapsibleCard
                                    title="Schema Discovery"
                                    icon={<FiLayers />}
                                >
                                    <div className="pi-kv-grid">
                                        <div>
                                            <strong>Columns:</strong>{" "}
                                            {
                                                (
                                                    runtimeSchemaDiscovery.columns ||
                                                    []
                                                ).length
                                            }
                                        </div>
                                        <div>
                                            <strong>Timestamp Columns:</strong>{" "}
                                            {compactValue(
                                                runtimeSchemaDiscovery.timestamp_columns,
                                            )}
                                        </div>
                                        <div>
                                            <strong>Nullable Columns:</strong>{" "}
                                            {compactValue(
                                                runtimeSchemaDiscovery.nullable_columns,
                                            )}
                                        </div>
                                        <div>
                                            <strong>
                                                Primary Key Candidates:
                                            </strong>{" "}
                                            {compactValue(
                                                runtimeSchemaDiscovery.primary_key_candidates,
                                            )}
                                        </div>
                                    </div>
                                </CollapsibleCard>
                                <CollapsibleCard
                                    title="Activity Intelligence"
                                    icon={<FiActivity />}
                                    defaultOpen={false}
                                >
                                    <div className="pi-list">
                                        {renderListItems(
                                            runtimeDiscovery.activity_intelligence ||
                                                [],
                                            (item) => item,
                                        )}
                                    </div>
                                </CollapsibleCard>
                                <CollapsibleCard
                                    title="DQ Recommendation Engine"
                                    icon={<FiShield />}
                                    defaultOpen={false}
                                >
                                    <div className="pi-list">
                                        {renderListItems(
                                            runtimeDiscovery.dq_recommendations ||
                                                [],
                                            (item) =>
                                                `${item.rule}: ${item.reason}${item.column ? ` [${item.column}]` : ""}`,
                                        )}
                                    </div>
                                </CollapsibleCard>
                                <div className="pi-card">
                                    <div className="pi-card-title">
                                        Lineage Visualization
                                    </div>
                                    <div className="pi-list">
                                        {renderListItems(
                                            runtimeDiscovery.lineage_summary ||
                                                [],
                                            (item) =>
                                                `${item.source_label} → ${item.activity_label} → ${item.target_label}`,
                                        )}
                                    </div>
                                </div>
                                <div className="pi-card">
                                    <div className="pi-card-title">
                                        Runtime Activity Tracker
                                    </div>
                                    <div className="pi-list">
                                        {renderListItems(
                                            runtimeAnalysis.runtime_activity_tracker ||
                                                [],
                                            (item) =>
                                                `${item.activity_name} • ${item.activity_type} • ${item.status}${item.duration_ms ? ` • ${item.duration_ms} ms` : ""}`,
                                        )}
                                    </div>
                                </div>
                                <div className="pi-card">
                                    <div className="pi-card-title">
                                        Ingestion Config Generator
                                    </div>
                                    <JsonBlock
                                        value={
                                            runtimeDiscovery.ingestion_config ||
                                            {}
                                        }
                                    />
                                </div>
                            </div>

                            {(runtimeActionMessage || runtimePreviewError) && (
                                <div
                                    className={`pi-alert ${runtimePreviewError ? "error" : "warning"}`}
                                    style={{ marginTop: 16 }}
                                >
                                    <div>
                                        {runtimePreviewError ||
                                            runtimeActionMessage}
                                    </div>
                                </div>
                            )}

                            <div
                                className="pi-card pi-wide"
                                style={{ marginTop: 16 }}
                            >
                                <div className="pi-card-title">
                                    <FiFile /> Dynamic Runtime Preview
                                </div>
                                <DynamicPreviewTable
                                    initialData={runtimePreview}
                                    workspaceId={
                                        runtimeSourceConnection.workspace_id
                                    }
                                    lakehouseId={
                                        runtimeSourceConnection.artifact_id
                                    }
                                    onDataChange={(newData) =>
                                        setRuntimePreview(newData)
                                    }
                                />
                            </div>

                            <div
                                className="pi-card pi-wide"
                                style={{ marginTop: 16 }}
                            >
                                <div className="pi-card-title">
                                    <FiDatabase /> Schema Discovery Table
                                </div>
                                {(runtimeSchemaDiscovery.columns || [])
                                    .length ? (
                                    <div style={{ overflowX: "auto" }}>
                                        <table
                                            className="preview-table"
                                            style={{
                                                width: "100%",
                                                borderCollapse: "collapse",
                                            }}
                                        >
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
                                                {(
                                                    runtimeSchemaDiscovery.columns ||
                                                    []
                                                ).map((column) => (
                                                    <tr
                                                        key={column.column_name}
                                                    >
                                                        <td>
                                                            {column.column_name}
                                                        </td>
                                                        <td>
                                                            {column.data_type}
                                                        </td>
                                                        <td>
                                                            {column.nullable
                                                                ? "Yes"
                                                                : "No"}
                                                        </td>
                                                        <td>
                                                            {
                                                                column.ordinal_position
                                                            }
                                                        </td>
                                                        <td>
                                                            {compactValue(
                                                                column.sample_value,
                                                            )}
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                ) : (
                                    <div className="pi-card-content">
                                        No runtime sample rows were captured for
                                        schema inference.
                                    </div>
                                )}
                            </div>

                            <div
                                className="pi-card pi-wide"
                                style={{ marginTop: 16 }}
                            >
                                <div className="pi-card-title">
                                    <FiGitBranch /> Runtime Execution Graph
                                </div>
                                {runtimeGraphNodes.length ? (
                                    <PipelineFlowCanvas
                                        nodes={runtimeGraphNodes}
                                        edges={runtimeGraphEdges}
                                    />
                                ) : (
                                    <div className="pi-card-content">
                                        No runtime execution graph available.
                                    </div>
                                )}
                            </div>

                            <div className="pi-grid" style={{ marginTop: 16 }}>
                                <div className="pi-card">
                                    <div className="pi-card-title">
                                        Resolved Dynamic Expressions
                                    </div>
                                    <div className="pi-list">
                                        {renderListItems(
                                            runtimeAnalysis.resolved_expressions ||
                                                [],
                                            (item) =>
                                                `${item.expression} => ${typeof item.resolved_value === "object" ? JSON.stringify(item.resolved_value) : String(item.resolved_value)}`,
                                        )}
                                    </div>
                                </div>
                                <div className="pi-card">
                                    <div className="pi-card-title">
                                        Actual API Endpoints
                                    </div>
                                    <div className="pi-list">
                                        {renderListItems(
                                            runtimeAnalysis.actual_api_endpoints ||
                                                [],
                                            (item) =>
                                                `${item.activity}: ${item.method || "GET"} ${item.url || "No URL captured"}${item.status_code ? ` • ${item.status_code}` : ""}`,
                                        )}
                                    </div>
                                </div>
                                <div className="pi-card">
                                    <div className="pi-card-title">
                                        Actual Metadata Rows
                                    </div>
                                    <div className="pi-list">
                                        {renderListItems(
                                            runtimeAnalysis.actual_metadata_rows ||
                                                [],
                                            (item) =>
                                                `${item.activity}: ${item.executed_sql || "No SQL captured"}`,
                                        )}
                                    </div>
                                </div>
                                <div className="pi-card">
                                    <div className="pi-card-title">
                                        Runtime DQ Observations
                                    </div>
                                    <div className="pi-list">
                                        {renderListItems(
                                            runtimeAnalysis.runtime_dq_observations ||
                                                [],
                                            (item) =>
                                                `${item.activity}: ${item.observation}${item.evidence ? ` (${item.evidence})` : ""}`,
                                        )}
                                    </div>
                                </div>
                                <div className="pi-card">
                                    <div className="pi-card-title">
                                        Runtime Notebook Parameters
                                    </div>
                                    <div className="pi-list">
                                        {renderListItems(
                                            runtimeAnalysis.runtime_notebook_parameters ||
                                                [],
                                            (item) =>
                                                `${item.activity}: ${JSON.stringify(item.parameters || {})}`,
                                        )}
                                    </div>
                                </div>
                                <div className="pi-card">
                                    <div className="pi-card-title">
                                        Runtime SQL Queries
                                    </div>
                                    <div className="pi-list">
                                        {renderListItems(
                                            runtimeAnalysis.runtime_sql_queries ||
                                                [],
                                            (item) =>
                                                `${item.activity}: ${item.sql}`,
                                        )}
                                    </div>
                                </div>
                            </div>

                            <div
                                className="pi-card pi-wide"
                                style={{ marginTop: 16 }}
                            >
                                <div className="pi-card-title">
                                    <FiRefreshCw /> Activity Output Explorer
                                </div>
                                <div className="pi-json-grid">
                                    {(runtimeAnalysis.activity_outputs
                                        ? Object.entries(
                                              runtimeAnalysis.activity_outputs,
                                          )
                                        : []
                                    ).map(([activityName, payload]) => (
                                        <SearchableJsonPanel
                                            key={activityName}
                                            title={activityName}
                                            value={payload}
                                        />
                                    ))}
                                </div>
                            </div>

                            <div
                                className="pi-card pi-wide"
                                style={{ marginTop: 16 }}
                            >
                                <div className="pi-card-title">
                                    <FiFile /> Runtime Payload Viewer
                                </div>
                                <div className="pi-json-grid">
                                    <SearchableJsonPanel
                                        title="Runtime Payload"
                                        value={
                                            runtimeAnalysis.runtime_payload_viewer
                                        }
                                    />
                                    <SearchableJsonPanel
                                        title="Resolved Expressions"
                                        value={
                                            runtimeAnalysis.resolved_expressions
                                        }
                                    />
                                </div>
                            </div>
                        </div>
                    )}

                    <div className="pi-actions">
                        <button
                            className="pi-btn-confirm"
                            onClick={() =>
                                onConfirm({
                                    ...data,
                                    deploymentStrategy,
                                    selectedWorkspace,
                                    selectedPipeline,
                                })
                            }
                            disabled={
                                selectedPlatform === "FABRIC" &&
                                !deploymentStrategy
                            }
                        >
                            <FiCheck />{" "}
                            {selectedPlatform === "FABRIC"
                                ? "Confirm Strategy & Configure"
                                : "Configure Data Sources"}
                        </button>
                    </div>
                </>
            )}

            {showCloudScanModal && (
                <CloudPortalScanModal
                    selectedClient={clientName}
                    initialTarget={target}
                    allowedTargets={allowedTargets
                        .filter((item) => item.scan)
                        .map((item) => item.id)}
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
                            setFabricTokenValidation(
                                result.__fabric_token_validation,
                            );
                        }
                        if (selectedPlatform === "FABRIC") {
                            const discovered =
                                result.fabric_workspaces ||
                                result.workspaces ||
                                result.raw_cloud_scan?.fabric_workspaces ||
                                result.raw_cloud_scan?.workspaces ||
                                result.payload?.fabric_workspaces ||
                                [];
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
