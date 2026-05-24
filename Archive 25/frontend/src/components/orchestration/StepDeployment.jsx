import React, { useEffect, useState } from "react";
import {
    FiAlertCircle,
    FiCheck,
    FiChevronRight,
    FiCopy,
    FiEdit2,
    FiFileText,
    FiPackage,
    FiPlus,
    FiServer,
    FiUploadCloud,
    FiZap,
} from "react-icons/fi";
import { motion } from "framer-motion";
import { apiUrl } from "../../hooks/useApi";

const CONNECTOR_TYPES = [
    "REST",
    "Lakehouse",
    "CSV",
    "DelimitedText",
    "SQL",
    "ADLS",
    "JSON",
    "Parquet",
];

function defaultParams(strategy) {
    if (strategy === "MODIFY_SINK") {
        return '{\n  "connection_id": "",\n  "relative_url": "/posts",\n  "request_method": "POST"\n}';
    }
    return '{\n  "base_url": "https://jsonplaceholder.typicode.com",\n  "relative_url": "/posts",\n  "request_method": "GET"\n}';
}

function parseParams(text, label) {
    try {
        return JSON.parse(text || "{}");
    } catch {
        throw new Error(`${label} must be valid JSON.`);
    }
}

export default function StepDeployment({
    selectedWorkspace,
    selectedPipeline,
    deploymentStrategy,
    deploymentPackage,
    setDeploymentPackage,
    fabricAccessToken,
    onNext,
}) {
    const [status, setStatus] = useState("idle");
    const [error, setError] = useState(null);
    const [logs, setLogs] = useState([]);
    const [isDragging, setIsDragging] = useState(false);
    const [cloneName, setCloneName] = useState(
        selectedPipeline?.name ? `${selectedPipeline.name}_clone` : "MyClone",
    );
    const [sourceType, setSourceType] = useState("REST");
    const [sinkType, setSinkType] = useState("REST");
    const [sourceParamsText, setSourceParamsText] = useState(
        defaultParams("MODIFY_SOURCE"),
    );
    const [sinkParamsText, setSinkParamsText] = useState(
        defaultParams("MODIFY_SINK"),
    );
    const [sourceConnectionName, setSourceConnectionName] = useState("");
    const [sinkConnectionName, setSinkConnectionName] = useState("");
    const [templatePipelineId, setTemplatePipelineId] = useState("");

    const workspaceId =
        selectedWorkspace?.workspace_id || selectedWorkspace?.id || "";
    const pipelineItemId =
        selectedPipeline?.pipeline_item_id || selectedPipeline?.id || "";
    const workspaceName =
        selectedWorkspace?.workspace_name ||
        selectedWorkspace?.name ||
        selectedWorkspace?.displayName ||
        "";
    const pipelineName =
        selectedPipeline?.pipeline_name ||
        selectedPipeline?.name ||
        selectedPipeline?.displayName ||
        "";

    useEffect(() => {
        setStatus("idle");
        setError(null);
        setLogs([]);
        setSourceParamsText(defaultParams("MODIFY_SOURCE"));
        setSinkParamsText(defaultParams("MODIFY_SINK"));
    }, [deploymentStrategy]);

    // Auto-inspect selected pipeline to prefill connector types and params
    useEffect(() => {
        const shouldInspect =
            (deploymentStrategy === "MODIFY_SOURCE" ||
                deploymentStrategy === "MODIFY_SINK") &&
            !!pipelineItemId &&
            !!workspaceId;
        if (!shouldInspect) return;
        let mounted = true;
        (async () => {
            try {
                // Try to obtain a token from backend if frontend doesn't have one
                let tokenToUse = fabricAccessToken || null;
                if (!tokenToUse) {
                    try {
                        const tResp = await fetch(
                            apiUrl("/auth/fabric/token"),
                            { cache: "no-store" },
                        );
                        if (tResp.ok) {
                            const tb = await tResp.json();
                            tokenToUse = tb?.accessToken || null;
                        }
                    } catch (err) {
                        // ignore
                    }
                }

                const resp = await fetch(apiUrl("/fabric/inspect"), {
                    method: "POST",
                    headers: authHeaders(true, tokenToUse),
                    body: JSON.stringify({
                        workspace_id: workspaceId,
                        pipeline_id: pipelineItemId,
                    }),
                });
                if (!resp.ok) return; // keep defaults
                const body = await resp.json();
                if (!mounted) return;
                const role =
                    deploymentStrategy === "MODIFY_SOURCE" ? "source" : "sink";
                const detectedList =
                    role === "source"
                        ? body.detected_source_types
                        : body.detected_sink_types;
                const inspectActivities = body.activities || [];
                const copyFromInspect =
                    inspectActivities.find((a) => a.type === "Copy") ||
                    inspectActivities[0] ||
                    null;
                const pipelineActivities =
                    body.pipeline?.properties?.activities || [];
                const copyFromPipeline =
                    pipelineActivities.find((a) => a.type === "Copy") ||
                    pipelineActivities[0] ||
                    null;
                const roleFromInspect = copyFromInspect?.roles?.[role] || {};
                const roleFromPipeline =
                    (copyFromPipeline?.typeProperties || {})[role] || {};
                const directConnectionId =
                    role === "source"
                        ? body.source_connection_id
                        : body.sink_connection_id;
                const directConnectorType =
                    role === "source" ? body.source_type : body.sink_type;
                const detected =
                    (Array.isArray(detectedList) && detectedList[0]) ||
                    directConnectorType ||
                    roleFromInspect.connector_type ||
                    roleFromInspect.raw_type ||
                    "";
                if (detected) {
                    if (deploymentStrategy === "MODIFY_SOURCE") {
                        setSourceType(detected);
                    } else {
                        setSinkType(detected);
                    }
                }

                // Prefill params from inspect roles or exported pipeline typeProperties
                const ds =
                    roleFromInspect.datasetSettings ||
                    roleFromPipeline.datasetSettings ||
                    {};
                const ext = ds.externalReferences || {};
                const tprops = ds.typeProperties || {};
                const guessed = {};
                if (directConnectionId || ext.connection)
                    guessed.connection_id =
                        directConnectionId || ext.connection;
                if (tprops.relativeUrl)
                    guessed.relative_url = tprops.relativeUrl;
                // For file-based datasets
                if (tprops.location) {
                    guessed.file_name =
                        tprops.location.fileName ||
                        tprops.location["fileName"] ||
                        "";
                    guessed.folder_path =
                        tprops.location.folderPath ||
                        tprops.location["folderPath"] ||
                        "";
                    guessed.file_system =
                        tprops.location.fileSystem ||
                        tprops.location["fileSystem"] ||
                        "";
                }
                // If we detected a known type like DelimitedText, set format hints
                if (
                    (detected || "").toLowerCase().includes("delimited") ||
                    (detected || "").toLowerCase().includes("csv")
                ) {
                    guessed.format = "CSV";
                    guessed.delimiter = ",";
                }
                // Only set the params text if we created some guessed keys
                if (Object.keys(guessed).length > 0) {
                    const text = JSON.stringify(guessed, null, 2);
                    if (deploymentStrategy === "MODIFY_SOURCE")
                        setSourceParamsText(text);
                    else setSinkParamsText(text);
                }
            } catch (e) {
                // ignore inspect failures, leave defaults
                console.warn("Pipeline inspect failed", e);
            }
        })();
        return () => {
            mounted = false;
        };
    }, [deploymentStrategy, pipelineItemId, workspaceId]);

    const addLog = (msg) =>
        setLogs((prev) => [
            ...prev,
            { time: new Date().toLocaleTimeString(), msg },
        ]);
    const authHeaders = (json = false, overrideToken = null) => ({
        ...(json ? { "Content-Type": "application/json" } : {}),
        ...(overrideToken || fabricAccessToken
            ? { Authorization: `Bearer ${overrideToken || fabricAccessToken}` }
            : {}),
    });

    const processFile = (file) => {
        if (!file.name.endsWith(".zip")) {
            setError(
                "Only .zip files are supported for Fabric deployment packages.",
            );
            return;
        }
        setDeploymentPackage?.({
            name: file.name,
            file,
            size: `${(file.size / 1024 / 1024).toFixed(2)} MB`,
            uploadedAt: new Date().toISOString(),
            manifest: {
                includes: [
                    "pipeline-content.json",
                    "metadata.json",
                    "notebooks/",
                ],
            },
        });
    };

    const executeDeployment = async () => {
        setStatus("deploying");
        setError(null);
        setLogs([]);
        addLog(`Starting ${deploymentStrategy} deployment...`);

        try {
            // Ensure we have a usable Fabric token. If `fabricAccessToken` isn't provided
            // attempt to fetch a cached/refreshed token from the backend.
            let tokenToUse = fabricAccessToken || null;
            if (!tokenToUse) {
                try {
                    const tResp = await fetch(apiUrl("/auth/fabric/token"), {
                        cache: "no-store",
                    });
                    if (tResp.ok) {
                        const tb = await tResp.json();
                        tokenToUse = tb?.accessToken || null;
                    }
                } catch (err) {
                    // ignore token fetch errors; we'll let server return 401 for unauthenticated
                }
            }
            let response;
            if (deploymentStrategy === "CREATE_NEW") {
                if (!deploymentPackage?.file)
                    throw new Error("Deployment package file is missing.");
                const formData = new FormData();
                if (fabricAccessToken)
                    formData.append("access_token", fabricAccessToken);
                formData.append("target_workspace_id", workspaceId);
                if (workspaceName)
                    formData.append("workspace_name", workspaceName);
                formData.append("zip_file", deploymentPackage.file);
                formData.append(
                    "pipeline_name",
                    deploymentPackage.name.replace(".zip", ""),
                );
                response = await fetch(apiUrl("/deploy/execute"), {
                    method: "POST",
                    body: formData,
                });
            } else if (deploymentStrategy === "CLONE") {
                const formData = new FormData();
                formData.append("source_workspace_id", workspaceId);
                formData.append("source_pipeline_id", pipelineItemId);
                formData.append("target_workspace_id", workspaceId);
                if (workspaceName)
                    formData.append("workspace_name", workspaceName);
                if (pipelineName)
                    formData.append("pipeline_name", pipelineName);
                formData.append("new_name", cloneName);
                response = await fetch(apiUrl("/fabric/clone"), {
                    method: "POST",
                    headers: authHeaders(false, tokenToUse),
                    body: formData,
                });
            } else if (deploymentStrategy === "REUSE") {
                response = await fetch(apiUrl("/fabric/reuse"), {
                    method: "POST",
                    headers: authHeaders(true, tokenToUse),
                    body: JSON.stringify({
                        workspace_id: workspaceId,
                        workspace_name: workspaceName,
                        pipeline_id: pipelineItemId,
                        pipeline_item_id: pipelineItemId,
                        pipeline_name: pipelineName,
                    }),
                });
            } else if (deploymentStrategy === "MODIFY_SOURCE") {
                response = await fetch(apiUrl("/fabric/modify-source"), {
                    method: "POST",
                    headers: authHeaders(true, tokenToUse),
                    body: JSON.stringify({
                        workspace_id: workspaceId,
                        workspace_name: workspaceName,
                        pipeline_id: pipelineItemId,
                        pipeline_item_id: pipelineItemId,
                        pipeline_name: pipelineName,
                        clone_name: cloneName,
                        mode: "source",
                        source_type: sourceType || null,
                        source_params: parseParams(
                            sourceParamsText,
                            "Source params",
                        ),
                        source_connection_name: sourceConnectionName || null,
                        template_pipeline_id:
                            templatePipelineId || pipelineItemId || null,
                    }),
                });
            } else if (deploymentStrategy === "MODIFY_SINK") {
                response = await fetch(apiUrl("/fabric/modify-sink"), {
                    method: "POST",
                    headers: authHeaders(true, tokenToUse),
                    body: JSON.stringify({
                        workspace_id: workspaceId,
                        workspace_name: workspaceName,
                        pipeline_id: pipelineItemId,
                        pipeline_item_id: pipelineItemId,
                        pipeline_name: pipelineName,
                        clone_name: cloneName,
                        mode: "sink",
                        sink_type: sinkType || null,
                        sink_params: parseParams(sinkParamsText, "Sink params"),
                        sink_connection_name: sinkConnectionName || null,
                        template_pipeline_id:
                            templatePipelineId || pipelineItemId || null,
                    }),
                });
            } else {
                throw new Error(
                    `Unsupported deployment strategy: ${deploymentStrategy}`,
                );
            }

            const result = await response.json();
            if (!response.ok)
                throw new Error(
                    typeof result.detail === "string"
                        ? result.detail
                        : JSON.stringify(result.detail || result),
                );
            const deployedId =
                result.cloned_pipeline_id ||
                result.id ||
                result.pipeline_id ||
                result.fabric_response?.id ||
                selectedPipeline?.id;
            const deployedName =
                result.cloned_display_name ||
                result.displayName ||
                result.pipeline_deployed ||
                cloneName;
            addLog(`Deployment successful: ${deployedName}`);
            addLog(`Pipeline ID: ${deployedId}`);
            setDeploymentPackage?.({
                ...deploymentPackage,
                deployedId,
                deployedName,
            });
            setStatus("success");
        } catch (e) {
            setError(e.message || "Deployment failed.");
            setStatus("error");
        }
    };

    if (status === "success") {
        return (
            <motion.div
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                className="step-body success-view"
                style={{ textAlign: "center", padding: "60px 40px" }}
            >
                <div
                    style={{
                        margin: "0 auto 24px",
                        width: 80,
                        height: 80,
                        background: "#f0fdf4",
                        borderRadius: "50%",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                    }}
                >
                    <FiCheck size={40} color="#16a34a" />
                </div>
                <h2 style={{ fontSize: 24, fontWeight: 900, marginBottom: 8 }}>
                    Deployment Successful
                </h2>
                <p style={{ color: "#64748b", marginBottom: 40 }}>
                    Fabric deployment completed.
                </p>
                <button
                    className="orch-btn primary"
                    style={{ width: 240 }}
                    onClick={onNext}
                >
                    Continue to Review{" "}
                    <FiChevronRight style={{ marginLeft: 8 }} />
                </button>
            </motion.div>
        );
    }

    return (
        <div className="step-body">
            <div
                className="deployment-summary pi-card pi-wide"
                style={{ marginBottom: 24, borderLeft: "4px solid #2563eb" }}
            >
                <div className="pi-card-title">
                    <FiServer /> Deployment Context
                </div>
                <div className="deployment-context-grid">
                    <div className="context-item">
                        <span className="context-label">Workspace</span>
                        <span className="context-value">
                            {selectedWorkspace?.name ||
                                selectedWorkspace?.displayName ||
                                selectedWorkspace?.id}
                        </span>
                    </div>
                    <div className="context-item">
                        <span className="context-label">Pipeline</span>
                        <span className="context-value">
                            {selectedPipeline?.name ||
                                selectedPipeline?.displayName ||
                                selectedPipeline?.id}
                        </span>
                    </div>
                    <div className="context-item">
                        <span className="context-label">Strategy</span>
                        <span className="context-value">
                            {deploymentStrategy?.replace(/_/g, " ")}
                        </span>
                    </div>
                </div>
            </div>

            {deploymentStrategy === "CREATE_NEW" && (
                <div
                    className="strategy-section pi-card pi-wide"
                    style={{ padding: 24 }}
                >
                    <h3
                        style={{
                            fontSize: 17,
                            fontWeight: 800,
                            display: "flex",
                            alignItems: "center",
                            gap: 10,
                        }}
                    >
                        <FiPlus color="#2563eb" /> Create New Pipeline
                    </h3>
                    {!deploymentPackage ? (
                        <div
                            className={`upload-zone ${isDragging ? "dragging" : ""}`}
                            onDragOver={(e) => {
                                e.preventDefault();
                                setIsDragging(true);
                            }}
                            onDragLeave={() => setIsDragging(false)}
                            onDrop={(e) => {
                                e.preventDefault();
                                setIsDragging(false);
                                if (e.dataTransfer.files[0])
                                    processFile(e.dataTransfer.files[0]);
                            }}
                        >
                            <input
                                type="file"
                                id="zip-upload"
                                hidden
                                accept=".zip"
                                onChange={(e) =>
                                    e.target.files[0] &&
                                    processFile(e.target.files[0])
                                }
                            />
                            <label
                                htmlFor="zip-upload"
                                style={{
                                    cursor: "pointer",
                                    padding: "60px 40px",
                                    display: "flex",
                                    flexDirection: "column",
                                    alignItems: "center",
                                }}
                            >
                                <FiUploadCloud size={48} color="#2563eb" />
                                <div style={{ marginTop: 16, fontWeight: 800 }}>
                                    Click or Drag ZIP to Upload
                                </div>
                            </label>
                        </div>
                    ) : (
                        <div className="package-preview pi-card pi-wide">
                            <div
                                style={{
                                    display: "flex",
                                    alignItems: "center",
                                    gap: 16,
                                }}
                            >
                                <FiPackage size={24} color="#2563eb" />
                                <div style={{ flex: 1 }}>
                                    <strong>{deploymentPackage.name}</strong>
                                    <div
                                        style={{
                                            fontSize: 12,
                                            color: "#64748b",
                                        }}
                                    >
                                        {deploymentPackage.size}
                                    </div>
                                </div>
                                <button
                                    className="orch-btn ghost tiny"
                                    onClick={() => setDeploymentPackage(null)}
                                >
                                    Remove
                                </button>
                            </div>
                            <div
                                style={{
                                    marginTop: 12,
                                    display: "flex",
                                    gap: 10,
                                    flexWrap: "wrap",
                                }}
                            >
                                {deploymentPackage.manifest.includes.map(
                                    (item) => (
                                        <span
                                            key={item}
                                            style={{
                                                fontSize: 11,
                                                color: "#64748b",
                                            }}
                                        >
                                            <FiFileText /> {item}
                                        </span>
                                    ),
                                )}
                            </div>
                        </div>
                    )}
                </div>
            )}

            {deploymentStrategy === "CLONE" && (
                <div
                    className="strategy-section pi-card pi-wide"
                    style={{ padding: 24 }}
                >
                    <h3
                        style={{
                            fontSize: 17,
                            fontWeight: 800,
                            display: "flex",
                            alignItems: "center",
                            gap: 10,
                        }}
                    >
                        <FiCopy color="#2563eb" /> Clone Pipeline
                    </h3>
                    <label className="context-label">Clone Name</label>
                    <input
                        className="orch-input"
                        value={cloneName}
                        onChange={(e) => setCloneName(e.target.value)}
                    />
                </div>
            )}

            {deploymentStrategy === "REUSE" && (
                <div
                    className="strategy-section pi-card pi-wide"
                    style={{
                        padding: 24,
                        background: "#f0f9ff",
                        border: "1px solid #bae6fd",
                    }}
                >
                    <h3 style={{ color: "#0369a1" }}>
                        <FiZap /> Reuse Existing
                    </h3>
                    <p style={{ fontSize: 13, color: "#0369a1" }}>
                        Reuse the selected pipeline as-is.
                    </p>
                </div>
            )}

            {(deploymentStrategy === "MODIFY_SOURCE" ||
                deploymentStrategy === "MODIFY_SINK") && (
                <div
                    className="strategy-section pi-card pi-wide"
                    style={{ padding: 24 }}
                >
                    <h3
                        style={{
                            fontSize: 17,
                            fontWeight: 800,
                            display: "flex",
                            alignItems: "center",
                            gap: 10,
                        }}
                    >
                        <FiEdit2 color="#2563eb" />{" "}
                        {deploymentStrategy === "MODIFY_SOURCE"
                            ? "Modify Source"
                            : "Modify Sink"}
                    </h3>
                    <p style={{ fontSize: 13, color: "#64748b" }}>
                        Mirrors the working `pipe_chg_src` clone command: clone
                        name, mode, connector type, params JSON, optional
                        connection name, optional template pipeline ID.
                    </p>
                    <div style={{ display: "grid", gap: 12, marginTop: 16 }}>
                        <label className="context-label">Clone Name</label>
                        <input
                            className="orch-input"
                            value={cloneName}
                            onChange={(e) => setCloneName(e.target.value)}
                        />

                        <label className="context-label">
                            {deploymentStrategy === "MODIFY_SOURCE"
                                ? "Source Type"
                                : "Sink Type"}
                        </label>
                        <select
                            className="orch-input"
                            value={
                                deploymentStrategy === "MODIFY_SOURCE"
                                    ? sourceType
                                    : sinkType
                            }
                            onChange={(e) =>
                                deploymentStrategy === "MODIFY_SOURCE"
                                    ? setSourceType(e.target.value)
                                    : setSinkType(e.target.value)
                            }
                        >
                            {CONNECTOR_TYPES.map((type) => (
                                <option key={type} value={type}>
                                    {type}
                                </option>
                            ))}
                        </select>

                        <label className="context-label">
                            {deploymentStrategy === "MODIFY_SOURCE"
                                ? "Source Params JSON"
                                : "Sink Params JSON"}
                        </label>
                        <textarea
                            className="orch-input"
                            style={{
                                minHeight: 160,
                                fontFamily: "monospace",
                                fontSize: 12,
                            }}
                            value={
                                deploymentStrategy === "MODIFY_SOURCE"
                                    ? sourceParamsText
                                    : sinkParamsText
                            }
                            onChange={(e) =>
                                deploymentStrategy === "MODIFY_SOURCE"
                                    ? setSourceParamsText(e.target.value)
                                    : setSinkParamsText(e.target.value)
                            }
                        />

                        <label className="context-label">
                            {deploymentStrategy === "MODIFY_SOURCE"
                                ? "Source Connection Name"
                                : "Sink Connection Name"}
                        </label>
                        <input
                            className="orch-input"
                            value={
                                deploymentStrategy === "MODIFY_SOURCE"
                                    ? sourceConnectionName
                                    : sinkConnectionName
                            }
                            onChange={(e) =>
                                deploymentStrategy === "MODIFY_SOURCE"
                                    ? setSourceConnectionName(e.target.value)
                                    : setSinkConnectionName(e.target.value)
                            }
                            placeholder="Optional"
                        />

                        <label className="context-label">
                            Template Pipeline ID
                        </label>
                        <input
                            className="orch-input"
                            value={templatePipelineId}
                            onChange={(e) =>
                                setTemplatePipelineId(e.target.value)
                            }
                            placeholder="Optional pipeline ID to extract REST connection UUID"
                        />
                    </div>
                </div>
            )}

            {status === "deploying" && (
                <div
                    style={{
                        marginTop: 24,
                        padding: 24,
                        background: "#f8fafc",
                        borderRadius: 16,
                        border: "1px solid #e2e8f0",
                    }}
                >
                    <strong>Deployment in progress...</strong>
                    <div
                        style={{
                            marginTop: 12,
                            background: "#fff",
                            border: "1px solid #f1f5f9",
                            borderRadius: 8,
                            padding: 12,
                            maxHeight: 120,
                            overflowY: "auto",
                        }}
                    >
                        {logs.map((log, index) => (
                            <div
                                key={index}
                                style={{
                                    fontSize: 11,
                                    color: "#64748b",
                                    fontFamily: "monospace",
                                }}
                            >
                                [{log.time}] {log.msg}
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
                <button
                    className="orch-btn primary deploy-btn"
                    disabled={
                        status === "deploying" ||
                        (deploymentStrategy === "CREATE_NEW" &&
                            !deploymentPackage) ||
                        (deploymentStrategy !== "CREATE_NEW" &&
                            (!workspaceId || !pipelineItemId))
                    }
                    onClick={executeDeployment}
                    style={{ background: "#2563eb", padding: "12px 32px" }}
                >
                    {status === "deploying" ? (
                        "Deploying..."
                    ) : (
                        <>
                            {deploymentStrategy === "CREATE_NEW" && (
                                <>
                                    <FiPlus style={{ marginRight: 8 }} /> Create
                                    & Deploy Pipeline
                                </>
                            )}
                            {deploymentStrategy === "CLONE" && (
                                <>
                                    <FiCopy style={{ marginRight: 8 }} /> Clone
                                    Pipeline
                                </>
                            )}
                            {deploymentStrategy === "REUSE" && (
                                <>
                                    <FiZap style={{ marginRight: 8 }} /> Reuse
                                    Pipeline
                                </>
                            )}
                            {deploymentStrategy === "MODIFY_SOURCE" && (
                                <>
                                    <FiEdit2 style={{ marginRight: 8 }} />{" "}
                                    Modify Source
                                </>
                            )}
                            {deploymentStrategy === "MODIFY_SINK" && (
                                <>
                                    <FiEdit2 style={{ marginRight: 8 }} />{" "}
                                    Modify Sink
                                </>
                            )}
                        </>
                    )}
                </button>
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
                    word-break: break-word;
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
            `}</style>
        </div>
    );
}
