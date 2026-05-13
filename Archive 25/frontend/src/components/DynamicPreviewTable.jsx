import React, { useState, useEffect } from 'react';
import { FiDatabase, FiPlay, FiSearch, FiAlertCircle, FiTerminal, FiLayout, FiDownload, FiChevronDown, FiTable, FiColumns } from 'react-icons/fi';
import { apiUrl } from '../hooks/useApi';
import './DynamicPreview.css';

const DynamicPreviewTable = ({ 
  initialData, 
  onQuery, 
  onClose,
  clientId,
  workspaceId,
  lakehouseId,
  onDataChange
}) => {
  const [session, setSession] = useState(initialData?.session_id);
  const [columns, setColumns] = useState(initialData?.columns || []);
  const [rows, setRows] = useState(initialData?.rows || []);
  const [sqlQuery, setSqlQuery] = useState(
    initialData?.session_id 
      ? `SELECT * FROM runtime_preview_${initialData.session_id.replace(/-/g, '_')} LIMIT 100` 
      : ""
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [viewMode, setViewMode] = useState('table'); // 'table' or 'json'
  const [metadata, setMetadata] = useState({ tables: [], columns: [], active_table: "" });
  const [showMetadata, setShowMetadata] = useState(false);

  // Sync with initial data updates
  useEffect(() => {
    if (initialData) {
      setSession(initialData.session_id);
      setColumns(initialData.columns || []);
      setRows(initialData.rows || []);
      if (!sqlQuery && initialData.session_id) {
        setSqlQuery(`SELECT * FROM runtime_preview_${initialData.session_id.replace(/-/g, '_')} LIMIT 100`);
      }
      // Fetch metadata when session changes
      if (initialData.session_id) {
        fetchMetadata(initialData.session_id);
      }
    }
  }, [initialData]);

  const fetchMetadata = async (sessionId) => {
    try {
      const token = sessionStorage.getItem('fabric_access_token');
      const response = await fetch(apiUrl(`/discovery/fabric-runtime-metadata-discovery?session_id=${sessionId}`), {
        headers: { 
          'Authorization': `Bearer ${token}`
        }
      });
      if (response.ok) {
        const data = await response.json();
        setMetadata(data);
      }
    } catch (err) {
      console.error("Metadata discovery failed", err);
    }
  };

  const handleExecuteQuery = async () => {
    if (!sqlQuery.trim() || !session) return;
    
    setLoading(true);
    setError(null);
    console.log("Executing dynamic SQL query:", sqlQuery);

    try {
      const token = sessionStorage.getItem('fabric_access_token');
      const response = await fetch(apiUrl('/discovery/fabric-runtime-source-query'), {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          session_id: session,
          workspace_id: workspaceId,
          artifact_id: lakehouseId,
          query: sqlQuery
        }),
      });

      const result = await response.json();
      console.log("Query response:", result);

      if (!response.ok) {
        throw new Error(result.detail || 'SQL Execution failed');
      }

      const newColumnsRaw = result.columns || (result.rows?.length ? Object.keys(result.rows[0]) : []);
      const newRows = result.rows || [];

      // Normalize columns to objects if they are strings
      const newColumns = newColumnsRaw.map(col => {
        if (typeof col === 'string') return { name: col, type: 'string' };
        return {
          name: col.name || col.column_name || col.displayName || String(col),
          type: col.type || col.data_type || 'string'
        };
      });

      console.log("Dynamic columns extracted:", newColumns);
      
      setColumns(newColumns);
      setRows(newRows);

      if (onDataChange) {
        onDataChange({
          ...initialData,
          rows: newRows,
          columns: newColumns,
          row_count: newRows.length,
          preview_mode: 'queried'
        });
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const downloadCSV = () => {
    if (!rows.length) return;
    const header = columns.join(',');
    const csvRows = rows.map(row => 
      columns.map(col => `"${String(row[col] || '').replace(/"/g, '""')}"`).join(',')
    );
    const blob = new Blob([[header, ...csvRows].join('\n')], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `preview_${session.substring(0,8)}.csv`;
    a.click();
  };

  return (
    <div className="dynamic-preview-wrapper animate-fade-in">
      {/* SQL Header Section */}
      <div className="preview-sql-panel">
        <div className="panel-header">
          <div className="title" onClick={() => setShowMetadata(!showMetadata)} style={{ cursor: 'pointer' }}>
            <FiTerminal /> SQL QUERY EDITOR {showMetadata ? <FiChevronDown /> : <FiSearch />}
          </div>
          <div className="stats">
            {rows.length} rows • {columns.length} columns
          </div>
        </div>

        {showMetadata && (
          <div className="metadata-explorer" style={{ padding: '10px 15px', background: '#f8fafc', borderBottom: '1px solid #e2e8f0', display: 'flex', gap: 20 }}>
            <div className="meta-group">
              <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', marginBottom: 5, display: 'flex', alignItems: 'center', gap: 5 }}>
                <FiTable size={12}/> TABLES
              </div>
              <div style={{ display: 'flex', gap: 5, flexWrap: 'wrap' }}>
                {metadata.tables.map(t => (
                  <span key={t} className={`pi-tag ${t === metadata.active_table ? 'active' : 'inactive'}`} 
                        onClick={() => setSqlQuery(`SELECT * FROM ${t} LIMIT 100`)}
                        style={{ cursor: 'pointer', fontSize: 10 }}>
                    {t}
                  </span>
                ))}
              </div>
            </div>
            <div className="meta-group">
              <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', marginBottom: 5, display: 'flex', alignItems: 'center', gap: 5 }}>
                <FiColumns size={12}/> COLUMNS ({metadata.active_table})
              </div>
              <div style={{ display: 'flex', gap: 5, flexWrap: 'wrap', maxHeight: 60, overflowY: 'auto' }}>
                {metadata.columns.map(c => (
                  <span key={c.column_name} className="pi-tag inactive" style={{ fontSize: 10 }}>
                    {c.column_name} <small style={{ opacity: 0.6 }}>{c.data_type}</small>
                  </span>
                ))}
              </div>
            </div>
          </div>
        )}
        
        <div className="sql-editor-container">
          <textarea
            className="sql-textarea"
            value={sqlQuery}
            onChange={(e) => setSqlQuery(e.target.value)}
            placeholder="SELECT * FROM ..."
            spellCheck="false"
          />
          <div className="sql-actions">
            <button 
              className={`execute-btn ${loading ? 'loading' : ''}`}
              onClick={handleExecuteQuery}
              disabled={loading || !session}
            >
              {loading ? <div className="btn-spinner" /> : <FiPlay />}
              <span>Execute Query</span>
            </button>
            <button className="icon-btn" onClick={() => setViewMode(viewMode === 'table' ? 'json' : 'table')} title="Toggle View Mode">
              <FiLayout />
            </button>
            <button className="icon-btn" onClick={downloadCSV} title="Download CSV" disabled={!rows.length}>
              <FiDownload />
            </button>
          </div>
        </div>
        
        {error && (
          <div className="sql-error-alert animate-slide-up">
            <FiAlertCircle />
            <span>{error}</span>
          </div>
        )}
      </div>

      {/* Main Content Area */}
      <div className="preview-content-panel">
        {loading && !rows.length ? (
          <div className="preview-placeholder">
            <div className="loader-ring" />
            <p>Fetching dynamic results...</p>
          </div>
        ) : rows.length > 0 ? (
          <div className="table-responsive-container">
            {viewMode === 'table' ? (
              <table className="dynamic-data-table">
                <thead>
                  <tr>
                    <th className="row-number-col">#</th>
                    {columns.map((col, idx) => {
                      const colName = typeof col === 'string' ? col : (col.name || col.column_name || col.displayName);
                      return <th key={idx} title={colName}>{colName}</th>;
                    })}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, rowIndex) => (
                    <tr key={rowIndex}>
                      <td className="row-number-col">{rowIndex + 1}</td>
                      {columns.map((col, colIdx) => {
                        const colName = typeof col === 'string' ? col : (col.name || col.column_name || col.displayName);
                        return (
                          <td key={`${rowIndex}-${colIdx}`}>
                            {row[colName] !== null && row[colName] !== undefined
                              ? String(row[colName])
                              : <span className="null-val">null</span>}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <div className="json-view">
                <pre>{JSON.stringify(rows.slice(0, 50), null, 2)}</pre>
              </div>
            )}
          </div>
        ) : (
          <div className="preview-placeholder empty">
            <FiSearch size={40} />
            <p>No results found for the current query.</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default DynamicPreviewTable;
