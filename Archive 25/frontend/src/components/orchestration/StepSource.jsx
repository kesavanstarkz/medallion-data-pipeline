import { useState } from "react";
import {
    FiFolder,
    FiLink,
    FiDatabase,
    FiHardDrive,
    FiZap,
    FiCheck,
    FiChevronRight,
} from "react-icons/fi";
import { motion } from "framer-motion";
import "../../pages/orchestration.css";

const SOURCE_OPTIONS = [
    {
        id: "LOCAL",
        label: "Local Files",
        icon: <FiFolder />,
        desc: "Upload files from your computer",
        color: "#3b82f6",
        gradient: "linear-gradient(135deg, #3b82f6 0%, #1e40af 100%)",
    },
    {
        id: "REST_API",
        label: "REST API",
        icon: <FiLink />,
        desc: "Connect to REST API endpoints",
        color: "#10b981",
        gradient: "linear-gradient(135deg, #10b981 0%, #059669 100%)",
    },
    {
        id: "S3",
        label: "AWS S3",
        icon: <FiDatabase />,
        desc: "Amazon S3 buckets",
        color: "#f59e0b",
        gradient: "linear-gradient(135deg, #f59e0b 0%, #d97706 100%)",
    },
    {
        id: "ADLS",
        label: "Azure ADLS",
        icon: <FiHardDrive />,
        desc: "Azure Data Lake Storage",
        color: "#0078d4",
        gradient: "linear-gradient(135deg, #0078d4 0%, #003a8c 100%)",
    },
    {
        id: "FABRIC",
        label: "Microsoft Fabric",
        icon: <FiZap />,
        desc: "Fabric workspace & pipelines",
        color: "#6366f1",
        gradient: "linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)",
    },
];

const itemVariants = {
    initial: { opacity: 0, y: 20, scale: 0.95 },
    animate: (i) => ({
        opacity: 1,
        y: 0,
        scale: 1,
        transition: {
            delay: i * 0.08,
            type: "spring",
            damping: 20,
            stiffness: 200,
        },
    }),
};

export default function StepSource({
    sourceType,
    setSourceType,
    sourceForm = {},
    setSourceForm = () => {},
    selectedWorkspace,
    selectedPipeline,
    setShowUploadModal,
    onNext,
    onBack,
}) {
    const [selected, setSelected] = useState(sourceType || "");

    const handleSelect = (id) => {
        setSelected(id);
        setSourceType(id);
        setSourceForm((prev) => ({ ...prev, source_type: id }));
    };

    const handleNext = () => {
        if (selected) {
            onNext?.();
        }
    };

    const updateField = (field, value) => {
        setSourceForm((prev) => ({ ...prev, [field]: value }));
    };

    const renderEssentialFields = () => {
        if (!selected) return null;
        if (selected === "LOCAL") {
            return (
                <div className="pi-card pi-wide" style={{ marginTop: 24 }}>
                    <div className="pi-card-title">Local Files</div>
                    <button className="orch-btn primary" onClick={() => setShowUploadModal?.(true)}>
                        Upload Files
                    </button>
                </div>
            );
        }
        if (selected === "REST_API") {
            return (
                <div className="source-essential-grid">
                    <input className="orch-input" placeholder="Client name" value={sourceForm.client_name || ""} onChange={(e) => updateField("client_name", e.target.value)} />
                    <input className="orch-input" placeholder="Source name" value={sourceForm.source_name || ""} onChange={(e) => updateField("source_name", e.target.value)} />
                    <input className="orch-input" placeholder="Base URL" value={sourceForm.base_url || ""} onChange={(e) => updateField("base_url", e.target.value)} />
                    <input className="orch-input" placeholder="Authentication" value={sourceForm.auth_type || ""} onChange={(e) => updateField("auth_type", e.target.value)} />
                    <textarea className="orch-input" placeholder="Endpoint list, one per line" value={sourceForm.endpoints || ""} onChange={(e) => updateField("endpoints", e.target.value)} />
                </div>
            );
        }
        if (selected === "S3") {
            return (
                <div className="source-essential-grid">
                    <input className="orch-input" placeholder="Bucket" value={sourceForm.bucket_name || ""} onChange={(e) => updateField("bucket_name", e.target.value)} />
                    <input className="orch-input" placeholder="Access key" value={sourceForm.aws_access_key_id || ""} onChange={(e) => updateField("aws_access_key_id", e.target.value)} />
                    <input className="orch-input" placeholder="Secret key" type="password" value={sourceForm.aws_secret_access_key || ""} onChange={(e) => updateField("aws_secret_access_key", e.target.value)} />
                    <input className="orch-input" placeholder="Prefix / folder" value={sourceForm.prefix || ""} onChange={(e) => updateField("prefix", e.target.value)} />
                </div>
            );
        }
        if (selected === "ADLS") {
            return (
                <div className="source-essential-grid">
                    <input className="orch-input" placeholder="Storage account" value={sourceForm.azure_account_name || ""} onChange={(e) => updateField("azure_account_name", e.target.value)} />
                    <input className="orch-input" placeholder="Container" value={sourceForm.azure_container_name || ""} onChange={(e) => updateField("azure_container_name", e.target.value)} />
                    <input className="orch-input" placeholder="Folder" value={sourceForm.azure_folder || ""} onChange={(e) => updateField("azure_folder", e.target.value)} />
                </div>
            );
        }
        if (selected === "FABRIC") {
            return (
                <div className="source-essential-grid">
                    <input className="orch-input" readOnly value={selectedWorkspace?.displayName || selectedWorkspace?.name || ""} placeholder="Workspace selected in Intelligence" />
                    <input className="orch-input" readOnly value={selectedPipeline?.displayName || selectedPipeline?.name || ""} placeholder="Pipeline selected in Intelligence" />
                </div>
            );
        }
        return null;
    };

    return (
        <motion.div
            key="step-source"
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
            exit={{ opacity: 0, x: -20 }}
            className="orch-step-panel"
        >
            <div className="step-header-responsive">
                <div className="step-header-text">
                    <h2 className="step-title">Select Data Source</h2>
                    <p className="step-sub">
                        Choose where your data originates from. You'll configure
                        the connection details in the next step.
                    </p>
                </div>
            </div>

            <div className="step-body">
                <div className="source-grid">
                    {SOURCE_OPTIONS.map((source, i) => (
                        <motion.div
                            key={source.id}
                            custom={i}
                            variants={itemVariants}
                            initial="initial"
                            animate="animate"
                            whileHover={{
                                y: -6,
                                scale: 1.02,
                                transition: { duration: 0.2 },
                            }}
                            whileTap={{ scale: 0.98 }}
                            onClick={() => handleSelect(source.id)}
                            className={`source-card ${selected === source.id ? "selected" : ""}`}
                            style={{
                                "--source-color": source.color,
                                "--source-gradient": source.gradient,
                            }}
                        >
                            <div className="source-icon">{source.icon}</div>
                            <h3 className="source-label">{source.label}</h3>
                                <p className="source-desc">{source.desc}</p>

                            {selected === source.id && (
                                <motion.div
                                    initial={{ scale: 0, opacity: 0 }}
                                    animate={{ scale: 1, opacity: 1 }}
                                    className="selection-checkmark"
                                >
                                    <FiCheck size={18} />
                                </motion.div>
                            )}
                        </motion.div>
                    ))}
                </div>
                <div style={{ marginTop: 24 }}>{renderEssentialFields()}</div>
            </div>

            <div className="step-footer">
                <button className="btn btn-outline" onClick={() => onBack?.()}>
                    Back
                </button>
                <button
                    className="btn btn-primary"
                    onClick={handleNext}
                    disabled={!selected}
                >
                    Next
                    <FiChevronRight size={18} />
                </button>
            </div>
        </motion.div>
    );
}
