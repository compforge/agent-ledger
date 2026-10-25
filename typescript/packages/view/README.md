# @compforge/agent-view

Shared agent execution model, validation and standalone HTML rendering. No Ledger ingestion is required.

```sh
npx --package @compforge/agent-view agent-view agent-view.json -o agent-view.html
```

```ts
import { renderHTML, type Document } from "@compforge/agent-view";
const html = renderHTML(document);
```

The input uses `agent-ledger/view@1`. Model/tool content remains structured; absent observations are not fabricated.
See the [execution view guide](https://github.com/compforge/agent-ledger/blob/main/docs/execution-view.md).
