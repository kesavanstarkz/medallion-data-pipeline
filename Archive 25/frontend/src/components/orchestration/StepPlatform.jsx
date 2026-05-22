import { FiZap, FiCloud, FiBox, FiCpu, FiCheck, FiChevronRight } from 'react-icons/fi';
import { motion, AnimatePresence } from 'framer-motion';
import logo from '../../assets/images/image.png';

const PLATFORMS = [
  {
    id: 'FABRIC',
    label: 'Microsoft Fabric',
    icon: <FiZap />,
    color: '#6366f1',
    gradient: 'linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%)',
    desc: 'Unified analytics platform',
    features: ['Lakehouse', 'Data Pipelines', 'Notebooks', 'Warehouse'],
    enabled: true,
  },
  {
    id: 'AZURE',
    label: 'Azure',
    icon: <FiCloud />,
    color: '#0078d4',
    gradient: 'linear-gradient(135deg, #0078d4 0%, #00a4ef 100%)',
    desc: 'Azure Data Factory & ADLS',
    features: ['Data Factory', 'ADLS Gen2', 'Synapse', 'Databricks'],
    enabled: false,
  },
  {
    id: 'AWS',
    label: 'AWS',
    icon: <FiBox />,
    color: '#f59e0b',
    gradient: 'linear-gradient(135deg, #f59e0b 0%, #f97316 100%)',
    desc: 'S3, Glue & Lambda',
    features: ['S3', 'Glue', 'Lambda', 'Redshift'],
    enabled: false,
  },
  {
    id: 'DATABRICKS',
    label: 'Databricks',
    icon: <FiCpu />,
    color: '#ef4444',
    gradient: 'linear-gradient(135deg, #ef4444 0%, #f97316 100%)',
    desc: 'Lakehouse platform',
    features: ['Delta Lake', 'Spark', 'MLflow', 'Unity Catalog'],
    enabled: false,
  },
];

const itemVariants = {
  initial: { opacity: 0, y: 20, scale: 0.95 },
  animate: (i) => ({
    opacity: 1,
    y: 0,
    scale: 1,
    transition: { delay: i * 0.08, type: 'spring', damping: 20, stiffness: 200 },
  }),
};

export default function StepPlatform({ selectedPlatform, setSelectedPlatform, onNext }) {
  return (
    <motion.div
      key="step0"
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
      exit={{ opacity: 0, x: -20 }}
      className="orch-step-panel"
    >
      <div className="step-header-responsive">
        <div className="step-header-text">
          <h2 className="step-title">Select Execution Platform</h2>
          <p className="step-sub">
            Choose the hyperscaler or platform where your pipelines run. This determines discovery agents, deployment targets, and available integrations.
          </p>
        </div>
        <img src={logo} alt="Agilisium" className="step-header-logo" />
      </div>

      <div className="step-body">
        <div className="platform-grid">
          {PLATFORMS.map((p, i) => (
            <motion.div
              key={p.id}
              custom={i}
              variants={itemVariants}
              initial="initial"
              animate="animate"
              whileHover={{ y: -6, scale: 1.02, transition: { duration: 0.2 } }}
              whileTap={{ scale: 0.98 }}
              onClick={() => {
                if (p.enabled) setSelectedPlatform(p.id);
              }}
              className={`platform-selection-card ${selectedPlatform === p.id ? 'selected' : ''} ${!p.enabled ? 'disabled coming-soon' : ''}`}
              style={{
                '--p-color': p.color,
                '--p-gradient': p.gradient,
                opacity: p.enabled ? 1 : 0.62,
                cursor: p.enabled ? 'pointer' : 'not-allowed',
              }}
            >
              {/* Selection indicator */}
              {selectedPlatform === p.id && (
                <motion.div
                  initial={{ scale: 0 }}
                  animate={{ scale: 1 }}
                  className="selection-indicator"
                  style={{ background: p.gradient, boxShadow: `0 4px 12px ${p.color}40` }}
                >
                  <FiCheck size={14} />
                </motion.div>
              )}

              {/* Icon */}
              <div
                className="platform-icon-box"
                style={{ background: p.gradient, boxShadow: `0 6px 20px ${p.color}30` }}
              >
                {p.icon}
              </div>

              {/* Label */}
              <div
                className="platform-label"
                style={{ color: selectedPlatform === p.id ? p.color : 'var(--text1)' }}
              >
                {p.label}
              </div>
              {!p.enabled && (
                <div className="coming-soon-pill">Coming Soon</div>
              )}

              {/* Description */}
              <div className="platform-desc">
                {p.desc}
              </div>

              {/* Feature pills */}
              <div className="platform-features">
                {p.features.map((f) => (
                  <span
                    key={f}
                    className="feature-pill"
                    style={{
                      background: selectedPlatform === p.id ? `${p.color}15` : 'var(--surface2)',
                      color: selectedPlatform === p.id ? p.color : 'var(--text3)',
                    }}
                  >
                    {f}
                  </span>
                ))}
              </div>
            </motion.div>
          ))}
        </div>
      </div>

      {/* Footer */}
      <AnimatePresence>
        {selectedPlatform && (
          <motion.div
            initial={{ opacity: 0, scale: 0.98, y: 10 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.98, y: 10 }}
            className="step-footer-actions-container"
          >
            <div className="selected-info">
              Selected: <strong style={{ color: PLATFORMS.find((p) => p.id === selectedPlatform)?.color }}>
                {PLATFORMS.find((p) => p.id === selectedPlatform)?.label}
              </strong>
            </div>
            <button
              className="orch-btn primary premium-btn"
              onClick={onNext}
              disabled={selectedPlatform !== 'FABRIC'}
            >
              Continue <FiChevronRight />
            </button>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
