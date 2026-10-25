# @compforge/agent-pi

Pi integration for Agent Ledger's two capabilities:

- `@compforge/agent-pi/ledger`: `bindPiHarness`, `LedgerPiSessionStorage`; durable execution and native recovery storage.
- `@compforge/agent-pi/observation`: `sessionView`, `readSessionView`; native Session observations without a Ledger Store.

```ts
import { readSessionView } from "@compforge/agent-pi/observation";
const view = await readSessionView(storage);
```

The observation entrypoint does not import Ledger Core. Native Session entries remain authoritative;
missing model requests stay absent and tool results match only proposals on their ancestor path.
