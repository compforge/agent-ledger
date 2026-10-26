import type { SessionRef } from "@compforge/agent-common";
import { inspect, nodeKey, type View } from "./inspect.js";
import { children, concrete, type Node, type NodeRef } from "./node.js";

function escape(value: string): string {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}
function detail(label: string, value: unknown, open = false): string {
  if (value === undefined) return "";
  return `<details${open ? " open" : ""}><summary>${escape(label)}</summary><pre>${escape(JSON.stringify(value, null, 2))}</pre></details>`;
}
function anchor(id: string, session?: SessionRef): string {
  return "node-" + encodeURIComponent(nodeKey(id, session));
}

/**
 * @spec Render one inspected snapshot; scoped links never conflate equal native IDs from different Sessions.
 * @spec Native bodies are escaped, external evidence is never fetched and missing fragments stay visible.
 * @see docs/observation.md
 */
export function renderHTML(document: View, title?: string): string {
  const inspection = inspect(document);
  const isJob = document.schema_version === "agent-ledger/job@1";
  const rootSession = isJob ? undefined : document.session;
  title ??= isJob ? document.name ?? "Agent job" : "Agent session";
  const present = new Set<string>();
  function collect(node: Node): void {
    present.add(nodeKey(node.id, node.session ?? rootSession));
    children(node).forEach(collect);
  }
  document.nodes.forEach(collect);
  function link(ref: NodeRef): string {
    const scope = ref.session ?? rootSession;
    const label = `${scope ? `${scope.source}/${scope.id} · ` : ""}${ref.id}`;
    if (!present.has(nodeKey(ref.id, scope))) {
      return `<span class="gap">${escape(label)} — fragment not included</span>`;
    }
    return `<a href="#${escape(anchor(ref.id, scope))}">${escape(label)}</a>`;
  }
  function render(value: Node): string {
    const node = concrete(value);
    const scope = node.session ?? rootSession;
    const label = node.name ?? (node.kind === "operation" ? node.operation : node.kind === "tool_call" ? node.request?.tool_name : undefined) ?? node.kind;
    let body = "";
    if (node.kind === "model_call" || node.kind === "tool_call") {
      body += detail("Request", node.request) + detail("Response", node.response);
      if (node.response === undefined) body += '<p class="muted">No response body recorded.</p>';
      if (node.kind === "tool_call") {
        if (node.model_call_id !== undefined) {
          body += `<p>Model call: ${link({id: node.model_call_id, ...(scope ? {session: scope} : {})})}</p>`;
        }
        body += detail("Tool call ID", node.tool_call_id) + detail("Related sessions", node.sessions);
      }
    }
    if (node.kind === "input") body += detail(`Input · ${node.mode ?? "unspecified"}`, node.content, true);
    if (node.kind === "operation") body += detail("Operation data", node.data);
    body += children(node).map(render).join("");
    body += detail("Evidence", node.evidence) + detail("Attributes", node.attributes);
    const timing = node.started_at || node.finished_at
      ? `<p class="muted">${escape(node.started_at ?? "unknown start")} → ${escape(node.finished_at ?? "unknown finish")}</p>` : "";
    const ownership = isJob
      ? `<p class="ownership">${scope ? `Execution Session: ${escape(scope.source)} / ${escape(scope.id)}` : "Job-local work"}</p>` : "";
    return `<article id="${escape(anchor(node.id, scope))}"><header><strong>${escape(label)}</strong> <span>${escape(node.status ?? "unknown")}</span></header><p class="muted">${escape(node.id)}${node.lane_id ? ` · lane ${escape(node.lane_id)}` : ""}</p>${ownership}${timing}${body}</article>`;
  }
  const issues = [...new Set([...(document.issues ?? []), ...inspection.issues])]
    .map(issue => `<li>${escape(issue)}</li>`).join("");
  let overview: string;
  if (isJob) {
    const relations = (document.relations ?? []).map(relation =>
      `<li>${link(relation.source)} ${relation.kind === "depends_on" ? "depends on" : "hands off to"} ${link(relation.target)}${detail("Relation evidence", relation.evidence)}</li>`).join("");
    overview = `<section><h2>Goal and outcome</h2><p>Status: ${escape(document.status ?? "unknown")}</p>`
      + detail("Job", document.job) + detail("Goal", document.goal, true) + detail("Result", document.result, true)
      + detail("Timing", {started_at: document.started_at, finished_at: document.finished_at}) + "</section>"
      + (relations ? `<section><h2>Dependencies and handoffs</h2><ul>${relations}</ul></section>` : "")
      + '<p class="muted">Calls may serve several jobs. Recorded usage is not an exclusive job cost.</p><h2>Work</h2>';
  } else {
    overview = detail("Session", document.session, true) + detail("Lanes", document.lanes) + "<h2>Inputs and execution</h2>";
  }
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escape(title)}</title><style>body{font:15px/1.5 system-ui,sans-serif;margin:2rem auto;max-width:1100px;padding:0 1rem;color:#24332d;background:#f7f8f5}article{border:1px solid #cdd7d0;border-radius:8px;padding:1rem;margin:1rem 0;background:#fff;scroll-margin-top:1rem}article:target{outline:2px solid #477657}article article{margin-left:1rem}header{display:flex;justify-content:space-between;gap:1rem}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f2f4f1;padding:1rem}summary{cursor:pointer;padding:.4rem 0}.muted{color:#62736a;font-size:.85rem;overflow-wrap:anywhere}.ownership{font-size:.9rem;overflow-wrap:anywhere}.gap{color:#805c18}.issues{background:#fff0cf;padding:1rem 2rem}a{color:#275b40;overflow-wrap:anywhere}</style></head><body><h1>${escape(title)}</h1>${issues ? `<ul class="issues">${issues}</ul>` : ""}${overview}<p class="muted">Grouping and source order do not imply serial execution or causality.</p>${detail("Attributes", document.attributes)}${document.nodes.map(render).join("")}</body></html>`;
}
