import {
    FiZap,
    FiCopy,
    FiEdit2,
    FiRefreshCw,
    FiPlus,
    FiCheck,
} from "react-icons/fi";
import { motion } from "framer-motion";
import "../../pages/orchestration.css";

const STRATEGIES = [
    {
        id: "REUSE",
        label: "Reuse Existing",
        icon: <FiZap />,
        desc: "Use orchestration as-is, updating only metadata and parameters.",
        color: "#f59e0b",
        gradient: "linear-gradient(135deg, #f59e0b 0%, #d97706 100%)",
    },
    {
        id: "CLONE",
        label: "Clone Pipeline",
        icon: <FiCopy />,
        desc: "Duplicate the pipeline within the workspace for this execution.",
        color: "#6366f1",
        gradient: "linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)",
    },
    {
        id: "MODIFY_SOURCE",
        label: "Modify Source",
        icon: <FiEdit2 />,
        desc: "Replace pipeline source connector and configuration.",
        color: "#10b981",
        gradient: "linear-gradient(135deg, #10b981 0%, #059669 100%)",
    },
    {
        id: "MODIFY_SINK",
        label: "Modify Sink",
        icon: <FiRefreshCw />,
        desc: "Replace pipeline sink connector and configuration.",
        color: "#8b5cf6",
        gradient: "linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%)",
    },
    {
        id: "CREATE_NEW",
        label: "Create New",
        icon: <FiPlus />,
        desc: "Deploy a completely new pipeline from scratch.",
        color: "#0ea5e9",
        gradient: "linear-gradient(135deg, #0ea5e9 0%, #0284c7 100%)",
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

export default function PipelineStrategySelector({
    selectedStrategy,
    setSelectedStrategy,
}) {
    return (
        <div style={{ marginBottom: "32px" }}>
            <div
                style={{
                    marginBottom: "16px",
                    paddingBottom: "8px",
                    borderBottom: "2px solid var(--border)",
                }}
            >
                <h3
                    style={{
                        fontSize: "16px",
                        fontWeight: "600",
                        color: "var(--text)",
                        margin: "0",
                        display: "flex",
                        alignItems: "center",
                        gap: "8px",
                    }}
                >
                    <FiZap size={18} color="var(--blue)" />
                    Pipeline Execution Strategy
                </h3>
                <p
                    style={{
                        fontSize: "13px",
                        color: "var(--text-secondary)",
                        margin: "4px 0 0 0",
                    }}
                >
                    Select how you want to execute or manipulate this pipeline.
                    Your choice affects available configuration options and
                    deployment behavior.
                </p>
            </div>

            <div
                style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
                    gap: "12px",
                }}
            >
                {STRATEGIES.map((strategy, i) => (
                    <motion.div
                        key={strategy.id}
                        custom={i}
                        variants={itemVariants}
                        initial="initial"
                        animate="animate"
                        whileHover={{ y: -2, transition: { duration: 0.2 } }}
                        whileTap={{ scale: 0.98 }}
                    >
                        <button
                            onClick={() => setSelectedStrategy(strategy.id)}
                            style={{
                                width: "100%",
                                padding: "16px",
                                border:
                                    selectedStrategy === strategy.id
                                        ? `2px solid ${strategy.color}`
                                        : "1px solid var(--border)",
                                borderRadius: "8px",
                                background:
                                    selectedStrategy === strategy.id
                                        ? `${strategy.gradient}20`
                                        : "var(--surface)",
                                cursor: "pointer",
                                transition: "all 0.2s",
                                display: "flex",
                                flexDirection: "column",
                                alignItems: "flex-start",
                                gap: "8px",
                                position: "relative",
                            }}
                        >
                            {/* Selection indicator */}
                            {selectedStrategy === strategy.id && (
                                <div
                                    style={{
                                        position: "absolute",
                                        top: "8px",
                                        right: "8px",
                                        width: "24px",
                                        height: "24px",
                                        borderRadius: "50%",
                                        background: strategy.color,
                                        display: "flex",
                                        alignItems: "center",
                                        justifyContent: "center",
                                        color: "#fff",
                                    }}
                                >
                                    <FiCheck size={16} />
                                </div>
                            )}

                            {/* Icon */}
                            <div
                                style={{
                                    fontSize: "24px",
                                    color: strategy.color,
                                }}
                            >
                                {strategy.icon}
                            </div>

                            {/* Label */}
                            <div
                                style={{
                                    fontSize: "14px",
                                    fontWeight: "600",
                                    color: "var(--text)",
                                    textAlign: "left",
                                }}
                            >
                                {strategy.label}
                            </div>

                            {/* Description */}
                            <div
                                style={{
                                    fontSize: "12px",
                                    color: "var(--text-secondary)",
                                    textAlign: "left",
                                    lineHeight: "1.4",
                                }}
                            >
                                {strategy.desc}
                            </div>
                        </button>
                    </motion.div>
                ))}
            </div>
        </div>
    );
}
