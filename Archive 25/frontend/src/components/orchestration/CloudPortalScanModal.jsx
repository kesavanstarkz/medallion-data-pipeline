import { useEffect, useMemo, useState } from 'react';
import { FiCloud, FiLock, FiSearch, FiX, FiZap } from 'react-icons/fi';
import { motion } from 'framer-motion';
import { API_BASE_URL } from '../../hooks/useApi';

const PORTALS = [
  { id: 'aws', label: 'AWS', authMode: 'credentials', icon: <FiCloud /> },
  { id: 'azure', label: 'Azure', authMode: 'sso', icon: <FiCloud /> },
  { id: 'fabric', label: 'Microsoft Fabric', authMode: 'sso', icon: <FiZap /> },
];

const EMPTY_CREDS = {
  aws: { access_key: '', secret_key: '', region: '', role_arn: '' },
  azure: { tenant_id: '', client_id: '', client_secret: '', subscription_id: '', resource_group: '', sso_token: '' },
  fabric: { sso_token: '' },
};

function CredentialInput({ label, value, onChange, type = 'text', placeholder = '' }) {
  return (
    <label className="cloud-scan-field">
      <span>{label}</span>
      <input
        className="orch-input"
        type={type}
        value={value ?? ''}
        placeholder={placeholder}
        autoComplete="off"
        onChange={(e) => onChange(e.target.value)}
      />
    </label>
  );
}

function backendUrl(path) {
  return `${API_BASE_URL}${path.startsWith('/') ? path : `/${path}`}`;
}

export default function CloudPortalScanModal({ selectedClient, initialTarget = 'aws', allowedTargets = ['aws', 'azure', 'fabric'], sourceType = '', useCloudLlm = true, onTargetChange, onClose, onScanComplete }) {
  const [target, setTarget] = useState(initialTarget);
  const [credentials, setCredentials] = useState(EMPTY_CREDS);
  const [loading, setLoading] = useState(false);
  const [signingIn, setSigningIn] = useState(false);
  const [error, setError] = useState('');
  const [ssoAccount, setSsoAccount] = useState(null);
  const [authRequestId, setAuthRequestId] = useState('');
  const [authStatus, setAuthStatus] = useState({
    app_registration_configured: false,
    azure_local_session_supported: true,
    fabric_local_session_supported: true,
    fabric_requires_token_or_app_registration: false,
  });
  const [fabricTokenValidation, setFabricTokenValidation] = useState(null);

  const apiOrigin = useMemo(() => {
    try {
      return new URL(backendUrl('/')).origin;
    } catch {
      return window.location.origin;
    }
  }, []);
  const visiblePortals = useMemo(
    () => PORTALS.filter((portal) => (allowedTargets || []).includes(portal.id)),
    [allowedTargets]
  );

  useEffect(() => {
    if (visiblePortals.length === 0) return;
    if (!visiblePortals.some((portal) => portal.id === target)) {
      const nextTarget = visiblePortals[0].id;
      setTarget(nextTarget);
      onTargetChange?.(nextTarget);
    }
  }, [visiblePortals, target, onTargetChange]);

  const updateCredential = (key, value) => {
    setCredentials((prev) => ({
      ...prev,
      [target]: { ...(prev[target] || {}), [key]: value },
    }));
  };

  const buildCredentials = () => {
    const raw = credentials[target] || {};
    return Object.fromEntries(Object.entries(raw).filter(([, value]) => String(value || '').trim() !== ''));
  };

  useEffect(() => {
    const handleMessage = (event) => {
      if (event.origin !== apiOrigin) return;
      const payload = event.data || {};
      if (payload.source !== 'dea-msal') return;

      setSigningIn(false);
      if (!payload.success || !payload.accessToken) {
        setError(payload.error || 'Microsoft sign-in failed.');
        return;
      }

      setError('');
      setSsoAccount(payload.account || null);
      setAuthRequestId('');
      if (payload.target === 'fabric') setFabricTokenValidation(payload.tokenValidation || null);
      setCredentials((prev) => ({
        ...prev,
        [payload.target]: {
          ...(prev[payload.target] || {}),
          sso_token: payload.accessToken,
        },
      }));
    };

    window.addEventListener('message', handleMessage);
    return () => window.removeEventListener('message', handleMessage);
  }, [apiOrigin]);

  useEffect(() => {
    if (!authRequestId || !signingIn) return undefined;

    const timer = window.setInterval(async () => {
      try {
        const response = await fetch(backendUrl(`/auth/microsoft/result?auth_request_id=${encodeURIComponent(authRequestId)}`), { cache: 'no-store' });
        const payload = await response.json();
        if (!payload || payload.status === 'pending' || payload.status === 'unknown') return;

        window.clearInterval(timer);
        setSigningIn(false);
        setAuthRequestId('');

        if (!payload.success || !payload.accessToken) {
          setError(payload.error || 'Microsoft sign-in failed.');
          return;
        }

        setError('');
        setSsoAccount(payload.account || null);
        if (payload.target === 'fabric') setFabricTokenValidation(payload.tokenValidation || null);
        setCredentials((prev) => ({
          ...prev,
          [payload.target]: {
            ...(prev[payload.target] || {}),
            sso_token: payload.accessToken,
          },
        }));
      } catch {
        // ignore poll failures and keep waiting
      }
    }, 1000);

    return () => window.clearInterval(timer);
  }, [authRequestId, signingIn]);

  useEffect(() => {
    let cancelled = false;
    setError('');
    fetch(backendUrl(`/auth/microsoft/status?_=${Date.now()}`), { cache: 'no-store' })
      .then((res) => {
        if (!res.ok) throw new Error(`Auth status failed with ${res.status}`);
        return res.json();
      })
      .then((data) => {
        if (!cancelled) {
          setAuthStatus((prev) => ({ ...prev, ...data }));
        }
      })
      .catch((statusError) => {
        if (!cancelled) setError(statusError?.message || 'Could not refresh Microsoft SSO status.');
      });
    return () => {
      cancelled = true;
    };
  }, [target]);

  const startMicrosoftSso = () => {
    if (!authStatus.app_registration_configured) {
      if (target === 'azure') {
        setError(
          authStatus.azure_local_session_supported
            ? 'Azure app-registration SSO is not configured. This environment can still use your local Azure session via `az login`.'
            : 'Azure SSO is not configured. Set AZURE_CLIENT_ID and AZURE_CLIENT_SECRET for your company Microsoft app registration.'
        );
      } else {
        setError(
          authStatus.fabric_local_session_supported
            ? 'Fabric app-registration SSO is not configured. This environment can still try a local Azure session.'
            : 'Fabric SSO is not configured. Set AZURE_CLIENT_ID and AZURE_CLIENT_SECRET for your company Microsoft app registration.'
        );
      }
      return;
    }

    setError('');
    setSigningIn(true);
    fetch(backendUrl(`/auth/microsoft/start?target=${encodeURIComponent(target)}&origin=${encodeURIComponent(window.location.origin)}`), { cache: 'no-store' })
      .then((res) => res.json())
      .then((data) => {
        if (!data?.login_url || !data?.auth_request_id) {
          throw new Error('Microsoft sign-in could not be started.');
        }
        setAuthRequestId(data.auth_request_id);
        const popup = window.open(data.login_url, 'dea-microsoft-sso', 'width=560,height=720');
        if (!popup) {
          setSigningIn(false);
          setAuthRequestId('');
          setError('Popup was blocked. Allow popups and try Microsoft sign-in again.');
        }
      })
      .catch((ssoError) => {
        setSigningIn(false);
        setAuthRequestId('');
        setError(ssoError?.message || 'Microsoft sign-in could not be started.');
      });
  };

  const runScan = async () => {
    if (!selectedClient) {
      setError('Select a client before scanning a cloud framework.');
      return;
    }

    const requestCredentials = buildCredentials();
    if (
      target === 'fabric' &&
      !requestCredentials.sso_token &&
      !authStatus.app_registration_configured &&
      !authStatus.fabric_local_session_supported
    ) {
      setError('Fabric scan requires Microsoft SSO. Configure AZURE_CLIENT_ID and AZURE_CLIENT_SECRET or provide a valid Fabric bearer token.');
      return;
    }

    setLoading(true);
    setError('');
    try {
      const headers = { 'Content-Type': 'application/json' };
      let authMode = 'none';
      if (requestCredentials.sso_token) {
        headers.Authorization = `Bearer ${requestCredentials.sso_token}`;
        delete requestCredentials.sso_token;
        authMode = 'sso';
      }
      if (Object.keys(requestCredentials).length > 0 && authMode !== 'sso') {
        authMode = 'credentials';
      }

      // Platforms vs source-type connectors are architecturally separate.
      // `target` here is the PLATFORM (fabric / azure / aws).
      // `sourceType` prop is the DATA-SOURCE CONNECTOR (REST_API / S3 / ADLS / LOCAL).
      // The backend /discovery/analyze now accepts both as distinct fields.
      const PLATFORM_TARGETS = ['fabric', 'azure', 'aws', 'databricks'];
      const isTargetPlatform = PLATFORM_TARGETS.includes(target);

      const requestBody = {
        client_name: selectedClient,
        scan_mode: 'live',
        auth_mode: authMode,
        credentials: requestCredentials,
        use_cloud_llm: useCloudLlm,
        llm_provider: 'gpt',
      };

      // Always set target for the scanner routing
      requestBody.target = target;

      if (isTargetPlatform) {
        // Tell the backend which platform this scan is for
        requestBody.platform = target.toUpperCase();
      } else {
        // Legacy: target is itself a source-type connector (e.g. direct s3/adls scan)
        requestBody.source_type = sourceType || target;
      }

      // If a separate data-source connector is also provided, include it
      if (sourceType && !PLATFORM_TARGETS.includes((sourceType || '').toLowerCase())) {
        requestBody.source_type = sourceType;
      }

      const response = await fetch(backendUrl('/discovery/analyze'), {
        method: 'POST',
        headers,
        body: JSON.stringify(requestBody),
      });
      if (!response.ok) {
        let message = `Scan failed with status ${response.status}`;
        try {
          const failure = await response.json();
          message = failure.detail || failure.message || message;
        } catch {}
        throw new Error(message);
      }
      const data = await response.json();
      if (target === 'fabric' && headers.Authorization) {
        data.__fabric_access_token = headers.Authorization.replace(/^Bearer\s+/i, '');
        data.__fabric_token_validation = fabricTokenValidation;
      }
      onScanComplete(data);
      setCredentials(EMPTY_CREDS);
      setSsoAccount(null);
      onClose();
    } catch (scanError) {
      setError(scanError?.message || 'Framework scan failed.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="mode-modal-overlay" style={{ zIndex: 1400 }}>
      <motion.div
        initial={{ opacity: 0, scale: 0.96 }}
        animate={{ opacity: 1, scale: 1 }}
        exit={{ opacity: 0, scale: 0.96 }}
        className="mode-modal-card cloud-scan-modal"
      >
        <div className="cloud-scan-header">
          <div>
            <h3 style={{ margin: 0 }}>Cloud Portal Selection</h3>
            <div className="step-sub" style={{ marginTop: 4 }}>Run framework discovery with transient scan credentials.</div>
          </div>
          <button className="orch-btn ghost tiny" onClick={onClose} aria-label="Close"><FiX /></button>
        </div>

        <div className="cloud-scan-portals">
          {visiblePortals.map((portal) => (
            <button
              key={portal.id}
              type="button"
              className={`cloud-scan-portal ${target === portal.id ? 'selected' : ''}`}
              onClick={() => {
                setTarget(portal.id);
                onTargetChange?.(portal.id);
              }}
            >
              <span>{portal.icon}</span>
              {portal.label}
            </button>
          ))}
        </div>

        <div className="cloud-scan-body">
          {target === 'aws' && (
            <div className="cloud-scan-form">
              <CredentialInput label="Access Key" value={credentials.aws.access_key} onChange={(v) => updateCredential('access_key', v)} />
              <CredentialInput label="Secret Key" type="password" value={credentials.aws.secret_key} onChange={(v) => updateCredential('secret_key', v)} />
              <CredentialInput label="Region" value={credentials.aws.region} onChange={(v) => updateCredential('region', v)} placeholder="us-east-1" />
              <CredentialInput label="Role ARN" value={credentials.aws.role_arn} onChange={(v) => updateCredential('role_arn', v)} />
            </div>
          )}

          {target === 'azure' && (
            <>
              <div className="cloud-scan-sso">
                <button className="orch-btn primary" type="button" onClick={startMicrosoftSso} disabled={signingIn}>
                  <FiLock style={{ marginRight: 8 }} />
                  {signingIn ? 'Signing in...' : authStatus.app_registration_configured ? 'Sign in with Microsoft / Continue with SSO' : authStatus.azure_local_session_supported ? 'Use Local Azure Session' : 'Microsoft SSO Not Configured'}
                </button>
                <div className="step-sub">
                  {authStatus.app_registration_configured
                    ? 'Azure discovery uses your delegated Microsoft token when available. Service principal fields below remain as an optional fallback.'
                    : authStatus.azure_local_session_supported
                      ? 'Azure discovery can run from your existing local Azure session. Run `az login` on this machine, then use Run Framework Scan without popup sign-in.'
                      : 'Azure discovery is configured for Microsoft SSO only. Add the backend Azure app registration settings before scanning.'}
                </div>
                {ssoAccount?.username && (
                  <div className="step-sub">Connected as {ssoAccount.name || ssoAccount.username}.</div>
                )}
              </div>
              <div className="cloud-scan-form">
                <CredentialInput label="Tenant ID" value={credentials.azure.tenant_id} onChange={(v) => updateCredential('tenant_id', v)} />
                <CredentialInput label="Client ID" value={credentials.azure.client_id} onChange={(v) => updateCredential('client_id', v)} />
                <CredentialInput label="Client Secret" type="password" value={credentials.azure.client_secret} onChange={(v) => updateCredential('client_secret', v)} />
                <CredentialInput label="Subscription ID" value={credentials.azure.subscription_id} onChange={(v) => updateCredential('subscription_id', v)} />
                <CredentialInput label="Resource Group" value={credentials.azure.resource_group} onChange={(v) => updateCredential('resource_group', v)} />
              </div>
            </>
          )}

          {target === 'fabric' && (
            <div className="cloud-scan-sso">
              <button className="orch-btn primary" type="button" onClick={startMicrosoftSso} disabled={signingIn}>
                <FiLock style={{ marginRight: 8 }} /> {signingIn ? 'Signing in...' : authStatus.app_registration_configured ? 'Sign in with Microsoft / Continue with SSO' : authStatus.fabric_local_session_supported ? 'Use Local Fabric Session' : 'Microsoft SSO Required'}
              </button>
              <div className="step-sub">
                {authStatus.app_registration_configured
                  ? 'Fabric discovery uses the same Microsoft sign-in flow. For local demos, you can still paste an existing Fabric bearer token.'
                  : authStatus.fabric_local_session_supported
                    ? 'Fabric discovery can use a local Azure identity session in this environment.'
                    : 'Fabric discovery is configured to require Microsoft SSO or an explicit bearer token. Local machine session fallback is disabled.'}
              </div>
              {ssoAccount?.username && (
                <div className="step-sub">Connected as {ssoAccount.name || ssoAccount.username}.</div>
              )}
              {fabricTokenValidation && (
                <div className="pi-alert warning" style={{ marginTop: 12 }}>
                  <div><strong>Audience:</strong> {fabricTokenValidation.aud || 'Unavailable'}</div>
                  <div><strong>Current scopes:</strong> {(fabricTokenValidation.scp || []).join(', ') || 'None'}</div>
                  <div><strong>Required scopes:</strong> {(fabricTokenValidation.required_scopes || []).join(', ') || 'None'}</div>
                  {!!(fabricTokenValidation.missing_scopes || []).length && (
                    <div><strong>Missing scopes:</strong> {(fabricTokenValidation.missing_scopes || []).join(', ')}</div>
                  )}
                  {!!(fabricTokenValidation.missing_scopes || []).length && (
                    <div style={{ marginTop: 8, whiteSpace: 'pre-wrap' }}>
                      {`Please grant delegated Microsoft Fabric API permissions:\n- Workspace.ReadWrite.All\n- Item.ReadWrite.All\n- Item.Execute.All\n- Connection.ReadWrite.All\n\nThen provide admin consent.`}
                    </div>
                  )}
                </div>
              )}
              <CredentialInput label="Demo SSO Token" type="password" value={credentials.fabric.sso_token} onChange={(v) => updateCredential('sso_token', v)} />
            </div>
          )}
        </div>

        {error && <div className="panel-error-alert">{error}</div>}

        <div className="step-footer" style={{ marginTop: 18, paddingTop: 18 }}>
          <button className="orch-btn ghost" onClick={onClose} disabled={loading}>Cancel</button>
          <button className="orch-btn primary" onClick={runScan} disabled={loading || !selectedClient}>
            <FiSearch style={{ marginRight: 8 }} />
            {loading ? 'Scanning Framework...' : 'Run Framework Scan'}
          </button>
        </div>
      </motion.div>
    </div>
  );
}
