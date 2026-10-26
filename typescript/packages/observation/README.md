# @compforge/agent-observation

SessionView (`agent-ledger/session@1`) follows one Agent Session. JobView (`agent-ledger/job@1`)
follows one task across Sessions. Both use Input, ModelCall, ToolCall, Operation and Turn; neither
requires Ledger storage. Source adapters supply business correlation and task outcomes.

```ts
import { renderHTML, validate, referenceIssues, type JobView } from "@compforge/agent-observation";
const job: JobView = {
  schema_version: "agent-ledger/job@1",
  job: { source: "application", id: "task-1" },
  goal: "Inspect the proposed change",
  items: [],
};
validate(job);
const gaps = referenceIssues(job);
const html = renderHTML(job);
```

```sh
npx --package @compforge/agent-observation agent-observation session.json -o session.html
npx --package @compforge/agent-observation agent-observation job.json -o job.html
```

Calls keep their Session-scoped identity and exact recorded request/response. Partial external
references stay visible; recorded usage is not exclusive Job cost. The renderer escapes source
content, runs no scripts and fetches no external evidence. See `docs/observation.md` in the repository
for producer, relation and evaluation boundaries.
