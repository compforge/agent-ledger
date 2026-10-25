import { validate, type SessionView, type Item } from "./model.js";

function escape(value: string): string {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}
function detail(label: string, value: unknown): string {
  if (value === undefined) return "";
  return `<details><summary>${escape(label)}</summary><pre>${escape(JSON.stringify(value, null, 2))}</pre></details>`;
}

/** Standalone, script-free rendering of recorded facts. Content and source URLs are never executable. */
export function renderHTML(document: SessionView, title = "Agent session"): string {
  validate(document);
  function render(node: Item): string {
    const label = node.name ?? (node.kind === "operation" ? node.operation : node.kind === "tool_call" ? node.request?.tool_name : undefined) ?? node.kind;
    let body = "";
    if (node.kind === "model_call" || node.kind === "tool_call") {
      body += detail("Request", node.request) + detail("Response", node.response);
      if (node.response === undefined) body += '<p class="muted">No response body recorded.</p>';
      if (node.kind === "tool_call") {
        body += detail("Call association", {tool_call_id: node.tool_call_id, model_call_id: node.model_call_id});
        body += detail("Related sessions", node.sessions);
      }
    }
    if (node.kind === "input") body += detail(`Input · ${node.mode ?? "unspecified"}`, node.content);
    if (node.kind === "operation") body += detail("Operation data", node.data);
    if (node.kind === "turn" || node.kind === "operation") body += (node.items ?? []).map(render).join("");
    body += detail("Evidence", node.evidence) + detail("Attributes", node.attributes);
    const timing = node.started_at || node.finished_at ? `<p class="muted">${escape(node.started_at ?? "unknown start")} → ${escape(node.finished_at ?? "unknown finish")}</p>` : "";
    return `<article><header><strong>${escape(label)}</strong> <span>${escape(node.status ?? "unknown")}</span></header><p class="muted">${escape(node.id)}${node.lane_id ? ` · lane ${escape(node.lane_id)}` : ""}</p>${timing}${body}</article>`;
  }
  const issues = (document.issues ?? []).map(issue => `<li>${escape(issue)}</li>`).join("");
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escape(title)}</title><style>body{font:15px/1.5 system-ui,sans-serif;margin:2rem auto;max-width:1100px;padding:0 1rem;color:#24332d;background:#f7f8f5}article{border:1px solid #cdd7d0;border-radius:8px;padding:1rem;margin:1rem 0;background:#fff}article article{margin-left:1rem}header{display:flex;justify-content:space-between;gap:1rem}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f2f4f1;padding:1rem}summary{cursor:pointer;padding:.4rem 0}.muted{color:#62736a;font-size:.85rem;overflow-wrap:anywhere}.issues{background:#fff0cf;padding:1rem 2rem}</style></head><body><h1>${escape(title)}</h1>${issues ? `<ul class="issues">${issues}</ul>` : ""}${detail("Session", document.session)}${detail("Lanes", document.lanes)}${detail("Attributes", document.attributes)}${document.items.map(render).join("")}</body></html>`;
}
