import React, { useState, useEffect } from 'react';
import { FiDatabase, FiPlay, FiSearch, FiAlertCircle, FiTerminal, FiLayout, FiMaximize2, FiDownload } from 'react-icons/fi';
import './DynamicPreview.css';

const DynamicPreviewTable = ({ 
  initialData, 
  onQuery, 
  onClose,
  clientId,
  workspaceId,
  lakehouseId 
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

  // Sync with initial data updates
  useEffect(() => {
    if (initialData) {
      setSession(initialData.session_id);
      setColumns(initialData.columns || []);
      setRows(initialData.rows || []);
      if (!sqlQuery && initialData.session_id) {
        setSqlQuery(`SELECT * FROM runtime_preview_${initialData.session_id.replace(/-/g, '_')} LIMIT 100`);
      }
    }
  }, [initialData]);

  const handleExecuteQuery = async () => {
    if (!sqlQuery.trim() || !session) return;
    
    setLoading(true);
    setError(null);
    console.log("Executing dynamic SQL query:", sqlQuery);

    try {
      const response = await fetch('/discovery/fabric-runtime-source-query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
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

      // Dynamic Extraction Logic
      const newColumns = result.columns || (result.rows?.length ? Object.keys(result.rows[0]) : []);
      const newRows = result.rows || [];

      console.log("Dynamic columns extracted:", newColumns);
      
      setColumns(newColumns);
      setRows(newRows);
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
          <div className="title">
            <FiTerminal /> SQL QUERY EDITOR
          </div>
          <div className="stats">
            {rows.length} rows • {columns.length} columns
          </div>
        </div>
        
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
                    {columns.map((column) => (
                      <th key={column} title={column}>{column}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, rowIndex) => (
                    <tr key={rowIndex}>
                      <td className="row-number-col">{rowIndex + 1}</td>
                      {columns.map((column) => (
                        <td key={`${rowIndex}-${column}`}>
                          {row[column] !== null && row[column] !== undefined
                            ? String(row[column])
                            : <span className="null-val">null</span>}
                        </td>
                      ))}
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
