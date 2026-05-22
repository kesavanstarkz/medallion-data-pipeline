const GUID_RE =
  /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

export function isFabricGuid(value) {
  return typeof value === "string" && GUID_RE.test(value.trim());
}

/**
 * Normalize workspace selection: keep display names separate from Fabric GUIDs.
 */
export function normalizeFabricWorkspace(workspace) {
  if (!workspace) return null;

  const config = workspace.configuration || {};
  const workspaceId =
    workspace.workspace_id ||
    (isFabricGuid(workspace.id) ? workspace.id : null) ||
    config.WorkspaceId ||
    config.workspaceId ||
    null;
  const workspaceName =
    workspace.workspace_name ||
    workspace.name ||
    workspace.displayName ||
    config.displayName ||
    "";

  return {
    ...workspace,
    id: workspaceId || workspace.id,
    workspace_id: workspaceId || workspace.workspace_id,
    workspace_name: workspaceName,
    name: workspaceName || workspace.name,
    displayName: workspace.displayName || workspaceName,
  };
}

/**
 * Normalize pipeline selection: pipeline_item_id is the Fabric item GUID.
 */
export function normalizeFabricPipeline(pipeline, workspaceId = null) {
  if (!pipeline) return null;

  const config = pipeline.configuration || {};
  const pipelineItemId =
    pipeline.pipeline_item_id ||
    (isFabricGuid(pipeline.id) ? pipeline.id : null) ||
    config.ItemId ||
    config.itemId ||
    null;
  const pipelineName =
    pipeline.pipeline_name ||
    pipeline.name ||
    pipeline.displayName ||
    config.displayName ||
    config.Name ||
    "";

  return {
    ...pipeline,
    id: pipelineItemId || pipeline.id,
    pipeline_item_id: pipelineItemId || pipeline.pipeline_item_id,
    pipeline_id: pipelineItemId || pipeline.pipeline_id,
    pipeline_name: pipelineName,
    name: pipelineName || pipeline.name,
    displayName: pipeline.displayName || pipelineName,
    workspace_id:
      pipeline.workspace_id ||
      workspaceId ||
      config.WorkspaceId ||
      config.workspaceId ||
      null,
  };
}
