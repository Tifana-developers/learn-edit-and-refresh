// index.js
const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto');
const { BigQuery } = require('@google-cloud/bigquery');
const { Datastore } = require('@google-cloud/datastore');
const { GoogleAuth } = require('google-auth-library');

const app = express();
app.use(bodyParser.json({ limit: '2mb' }));

// 環境変数（デプロイ時に設定）
const VALID_API_KEY = process.env.VALID_API_KEY || '';
const DEFAULT_BQ_PROJECT = process.env.DEFAULT_BQ_PROJECT || '';
const DEFAULT_LOCATION = process.env.DEFAULT_LOCATION || 'asia-northeast1';
const DEFAULT_COLLECTION = process.env.DEFAULT_COLLECTION || '';

function isValidApiKey(req) {
  const headerKey = req.get('x-api-key') || (req.get('authorization') ? req.get('authorization').replace(/^Bearer\s+/i, '') : undefined);
  if (!headerKey || !VALID_API_KEY) return false;
  try {
    const a = Buffer.from(headerKey);
    const b = Buffer.from(VALID_API_KEY);
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  } catch (e) {
    return false;
  }
}

async function datastoreUpsertWithRetry(datastore, entities, maxRetries = 3) {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      await datastore.upsert(entities);
      return { ok: true };
    } catch (err) {
      if (attempt === maxRetries) return { ok: false, error: err };
      const backoff = Math.pow(2, attempt) * 200 + Math.floor(Math.random() * 100);
      await new Promise(r => setTimeout(r, backoff));
    }
  }
}

async function datastoreDeleteWithRetry(datastore, keys, maxRetries = 3) {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      await datastore.delete(keys);
      return { ok: true };
    } catch (err) {
      if (attempt === maxRetries) return { ok: false, error: err };
      const backoff = Math.pow(2, attempt) * 200 + Math.floor(Math.random() * 100);
      await new Promise(r => setTimeout(r, backoff));
    }
  }
}

async function detectDatasetLocation(bigquery, projectId, datasetId) {
  try {
    const ds = bigquery.dataset(datasetId);
    const [meta] = await ds.getMetadata();
    if (meta && meta.location) return String(meta.location).toLowerCase();
  } catch (e) {
    console.warn('dataset metadata read failed:', e.message || e);
  }
  return null;
}

async function startConnectorRun(project, location, collection, body = {}) {
  if (!project || !collection) throw new Error('connector project & collection required');
  const loc = location || DEFAULT_LOCATION || 'global';
  const url = `https://discoveryengine.googleapis.com/v1alpha/projects/${encodeURIComponent(project)}/locations/${encodeURIComponent(loc)}/collections/${encodeURIComponent(collection)}/dataConnector:startConnectorRun`;
  const auth = new GoogleAuth({ scopes: ['https://www.googleapis.com/auth/cloud-platform'] });
  const client = await auth.getClient();
  const resp = await client.request({ url, method: 'POST', data: body });
  return resp.data;
}

// 共通の検証ミドルウェア
function requireApiKey(req, res, next) {
  if (!isValidApiKey(req)) return res.status(401).json({ error: 'Unauthorized: missing/invalid API key' });
  next();
}

// エンドポイント: /faq-refresh
// Body: same as before: { bqProjectId, datasetId, tableId, rows: [...], datastoreProjectId, datastoreKind, ... , connectorProject, connectorLocation, connectorCollection, connectorEntities, bqLocation (optional override) }
app.post('/faq-refresh', requireApiKey, async (req, res) => {
  try {
    const body = req.body || {};
    const {
      bqProjectId,
      datasetId,
      tableId,
      rows,
      datastoreProjectId,
      datastoreKind,
      keyField = 'uniqueid',
      keyIsName = true,
      namespace = undefined,
      addMetaTimestamp = true,
      connectorProject,
      connectorLocation,
      connectorCollection,
      connectorEntities,
      forceRefreshContent = true
    } = body;

    if (!datasetId || !tableId || !Array.isArray(rows) || rows.length === 0) {
      return res.status(400).json({ error: 'datasetId, tableId, rows required' });
    }
    if (!datastoreProjectId || !datastoreKind) {
      return res.status(400).json({ error: 'datastoreProjectId and datastoreKind required' });
    }

    const bigquery = new BigQuery(bqProjectId ? { projectId: bqProjectId } : (DEFAULT_BQ_PROJECT ? { projectId: DEFAULT_BQ_PROJECT } : {}));
    const projectForBQ = bqProjectId || DEFAULT_BQ_PROJECT || bigquery.projectId;
    const fqTable = `\`${projectForBQ}.${datasetId}.${tableId}\``;

    // Build MERGE SQL (same pattern as earlier)
    const rowsJsonArray = rows.map(r => JSON.stringify(r));
    const mergeSql = `
MERGE ${fqTable} T
USING (
  SELECT
    JSON_EXTRACT_SCALAR(js, '$.uniqueid') AS uniqueid,
    JSON_EXTRACT_SCALAR(js, '$.question') AS question,
    JSON_EXTRACT_SCALAR(js, '$.answer') AS answer,
    JSON_EXTRACT_SCALAR(js, '$.detail') AS detail,
    CASE
      WHEN JSON_EXTRACT_SCALAR(js, '$.area') IS NULL OR JSON_EXTRACT_SCALAR(js, '$.area') = '' THEN NULL
      ELSE SAFE_CAST(JSON_EXTRACT_SCALAR(js, '$.area') AS INT64)
    END AS area
  FROM UNNEST(@rows_json) AS js
) AS S
ON T.uniqueid = S.uniqueid
WHEN MATCHED THEN
  UPDATE SET
    question = S.question,
    answer = S.answer,
    detail = S.detail,
    area = S.area,
    __syncedAt = CURRENT_DATETIME()
WHEN NOT MATCHED THEN
  INSERT (uniqueid, question, answer, detail, area, __syncedAt)
  VALUES (S.uniqueid, S.question, S.answer, S.detail, S.area, CURRENT_DATETIME())
`;

    // Determine location: priority body.bqLocation -> dataset metadata -> DEFAULT_LOCATION
    let jobLocation = body.bqLocation || null;
    if (!jobLocation) {
      const detected = await detectDatasetLocation(bigquery, projectForBQ, datasetId);
      if (detected) jobLocation = detected;
    }
    if (!jobLocation) jobLocation = body.bqLocation || DEFAULT_LOCATION;

    const queryOptions = {
      query: mergeSql,
      params: { rows_json: rowsJsonArray },
      location: jobLocation,
      useLegacySql: false
    };

    const [job] = await bigquery.createQueryJob(queryOptions);
    console.log('BQ job started', job.id);
    await job.getQueryResults();
    console.log('BQ job finished', job.id);

    // Datastore upsert
    const ds = new Datastore({ projectId: datastoreProjectId });
    const entities = rows.map(row => {
      const data = { ...row };
      if (addMetaTimestamp) data.__syncedAt = new Date().toISOString();
      let keyVal = row[keyField];
      if (keyVal === undefined || keyVal === null) keyVal = `${Date.now()}_${Math.floor(Math.random() * 1000)}`;
      const typedKey = keyIsName ? String(keyVal) : Number(keyVal);
      const key = namespace ? ds.key({ namespace, path: [datastoreKind, typedKey] }) : ds.key([datastoreKind, typedKey]);
      return { key, data };
    });

    const upsertResult = await datastoreUpsertWithRetry(ds, entities, 3);
    if (!upsertResult.ok) {
      console.error('Datastore upsert failed', upsertResult.error);
      return res.status(500).json({ error: 'Datastore upsert failed', detail: String(upsertResult.error) });
    }

    // start connector run if provided
    const connProject = connectorProject || bqProjectId || DEFAULT_BQ_PROJECT || projectForBQ;
    const connLocation = connectorLocation || DEFAULT_LOCATION;
    const connCollection = connectorCollection || DEFAULT_COLLECTION;
    let connectorResp = null;
    if (connProject && connCollection) {
      const startBody = {};
      if (Array.isArray(connectorEntities) && connectorEntities.length) startBody.entities = connectorEntities;
      startBody.forceRefreshContent = !!forceRefreshContent;
      connectorResp = await startConnectorRun(connProject, connLocation, connCollection, startBody);
    } else {
      console.warn('connectorProject or connectorCollection not provided; skipping connector run');
    }

    return res.json({
      ok: true,
      bqJobId: job.id,
      datastoreUpsertCount: entities.length,
      connectorRun: connectorResp
    });

  } catch (err) {
    console.error('error /faq-refresh', err);
    return res.status(500).json({ error: 'internal', detail: err?.response?.data || err.message || String(err) });
  }
});

// エンドポイント: /faq-delete
// Body: { bqProjectId, datasetId, tableId, ids: ["id1", "id2"], datastoreProjectId, datastoreKind, ... connector... }
app.post('/faq-delete', requireApiKey, async (req, res) => {
  try {
    const body = req.body || {};
    const { bqProjectId, datasetId, tableId, ids, datastoreProjectId, datastoreKind, connectorProject, connectorLocation, connectorCollection, connectorEntities, forceRefreshContent = true } = body;

    if (!datasetId || !tableId || !Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: 'datasetId, tableId, ids required' });
    }
    if (!datastoreProjectId || !datastoreKind) {
      return res.status(400).json({ error: 'datastoreProjectId and datastoreKind required' });
    }

    const bigquery = new BigQuery(bqProjectId ? { projectId: bqProjectId } : (DEFAULT_BQ_PROJECT ? { projectId: DEFAULT_BQ_PROJECT } : {}));
    const projectForBQ = bqProjectId || DEFAULT_BQ_PROJECT || bigquery.projectId;
    const fqTable = `\`${projectForBQ}.${datasetId}.${tableId}\``;

    // Determine location
    let jobLocation = body.bqLocation || null;
    if (!jobLocation) {
      const detected = await detectDatasetLocation(bigquery, projectForBQ, datasetId);
      if (detected) jobLocation = detected;
    }
    if (!jobLocation) jobLocation = body.bqLocation || DEFAULT_LOCATION;

    // DELETE query
    const deleteSql = `DELETE FROM ${fqTable} WHERE uniqueid IN UNNEST(@ids)`;
    const [job] = await bigquery.createQueryJob({
      query: deleteSql,
      params: { ids },
      location: jobLocation,
      useLegacySql: false
    });
    await job.getQueryResults();
    console.log('BQ delete finished', job.id);

    // Datastore delete
    const ds = new Datastore({ projectId: datastoreProjectId });
    const keys = ids.map(id => ds.key([datastoreKind, String(id)]));
    const delResult = await datastoreDeleteWithRetry(ds, keys, 3);
    if (!delResult.ok) {
      console.error('Datastore delete failed', delResult.error);
      return res.status(500).json({ error: 'Datastore delete failed', detail: String(delResult.error) });
    }

    // start connector run optionally
    const connProject = connectorProject || bqProjectId || DEFAULT_BQ_PROJECT || projectForBQ;
    const connLocation = connectorLocation || DEFAULT_LOCATION;
    const connCollection = connectorCollection || DEFAULT_COLLECTION;
    let connectorResp = null;
    if (connProject && connCollection) {
      const startBody = {};
      if (Array.isArray(connectorEntities) && connectorEntities.length) startBody.entities = connectorEntities;
      startBody.forceRefreshContent = !!forceRefreshContent;
      connectorResp = await startConnectorRun(connProject, connLocation, connCollection, startBody);
    }

    return res.json({
      ok: true,
      bqJobId: job.id,
      datastoreDeletedCount: keys.length,
      connectorRun: connectorResp
    });

  } catch (err) {
    console.error('error /faq-delete', err);
    return res.status(500).json({ error: 'internal', detail: err?.response?.data || err.message || String(err) });
  }
});

// health
app.get('/health', (req, res) => res.send('ok'));

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Server listening on ${port}`));
