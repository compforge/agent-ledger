# @compforge/agent-observation

Session observability, independent of Ledger storage. `SessionView` is the current main capability:
a structured observation of one continuing Agent Session, shared by renderers and analysis consumers.

```ts
import { renderHTML, type SessionView } from "@compforge/agent-observation";
const html = renderHTML(view, "Session progress");
```

```sh
npx --package @compforge/agent-observation session-view session-view.json -o session-view.html
```

Inputs preserve native evidence and missing outcomes. Rendering never resumes a Session or fetches
external content. See the repository's `docs/observation.md` for the contract.
