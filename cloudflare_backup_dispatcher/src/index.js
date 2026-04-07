const ACTIVE_RUN_STATUSES = new Set([
  "queued",
  "in_progress",
  "pending",
  "requested",
  "waiting",
  "action_required",
]);

export default {
  async scheduled(_controller, env, ctx) {
    ctx.waitUntil(runBackupDispatch(env, "scheduled"));
  },

  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return jsonResponse({
        ok: true,
        service: "macro-feishu-backup-dispatcher",
        timezone: getConfig(env).timezone,
      });
    }

    if (url.pathname === "/run" && request.method === "POST") {
      const result = await runBackupDispatch(env, "manual");
      return jsonResponse(result, result.ok ? 200 : 500);
    }

    return jsonResponse({
      ok: true,
      message: "Use POST /run to test the dispatcher or GET /health for status.",
    });
  },
};

async function runBackupDispatch(env, triggerSource) {
  const config = getConfig(env);
  const now = new Date();
  const todayKey = toDateKey(now, config.timezone);
  const runs = await listWorkflowRuns(config);
  const todaysRuns = runs.filter((run) => toDateKey(new Date(run.created_at), config.timezone) === todayKey);

  const healthyRun = todaysRuns.find((run) => {
    if (ACTIVE_RUN_STATUSES.has(run.status)) {
      return true;
    }
    return run.status === "completed" && run.conclusion === "success";
  });

  if (healthyRun) {
    return {
      ok: true,
      action: "skip",
      reason: "A healthy workflow run for today already exists.",
      triggerSource,
      timezone: config.timezone,
      today: todayKey,
      workflow: config.workflowFile,
      matchedRun: summarizeRun(healthyRun),
      todaysRuns: todaysRuns.map(summarizeRun),
    };
  }

  const dispatchResult = await dispatchWorkflow(config);
  return {
    ok: dispatchResult.ok,
    action: "dispatch",
    triggerSource,
    timezone: config.timezone,
    today: todayKey,
    workflow: config.workflowFile,
    todaysRuns: todaysRuns.map(summarizeRun),
    dispatchedRef: config.ref,
  };
}

function getConfig(env) {
  const config = {
    githubToken: env.GITHUB_TOKEN,
    owner: env.GITHUB_OWNER,
    repo: env.GITHUB_REPO,
    workflowFile: env.GITHUB_WORKFLOW_FILE || "daily_macro_feishu.yml",
    ref: env.GITHUB_REF || "main",
    timezone: env.LOCAL_TIMEZONE || "Asia/Shanghai",
    apiVersion: env.GITHUB_API_VERSION || "2022-11-28",
  };

  for (const [key, value] of Object.entries(config)) {
    if (!value) {
      throw new Error(`Missing required configuration: ${key}`);
    }
  }

  return config;
}

async function listWorkflowRuns(config) {
  const workflow = encodeURIComponent(config.workflowFile);
  const query = new URLSearchParams({
    branch: config.ref,
    per_page: "20",
  });

  const response = await githubRequest(
    config,
    `/repos/${config.owner}/${config.repo}/actions/workflows/${workflow}/runs?${query.toString()}`,
    { method: "GET" },
  );

  return response.workflow_runs || [];
}

async function dispatchWorkflow(config) {
  const workflow = encodeURIComponent(config.workflowFile);
  await githubRequest(
    config,
    `/repos/${config.owner}/${config.repo}/actions/workflows/${workflow}/dispatches`,
    {
      method: "POST",
      body: JSON.stringify({ ref: config.ref }),
    },
  );

  return { ok: true };
}

async function githubRequest(config, path, init) {
  const response = await fetch(`https://api.github.com${path}`, {
    ...init,
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${config.githubToken}`,
      "Content-Type": "application/json",
      "User-Agent": "macro-feishu-backup-dispatcher",
      "X-GitHub-Api-Version": config.apiVersion,
      ...(init.headers || {}),
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`GitHub API request failed: ${response.status} ${body}`);
  }

  if (response.status === 204) {
    return {};
  }

  return response.json();
}

function toDateKey(date, timezone) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);

  const lookup = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${lookup.year}-${lookup.month}-${lookup.day}`;
}

function summarizeRun(run) {
  return {
    id: run.id,
    event: run.event,
    status: run.status,
    conclusion: run.conclusion,
    created_at: run.created_at,
    html_url: run.html_url,
  };
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}
