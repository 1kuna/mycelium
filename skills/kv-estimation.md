# KV Estimation

Fit is `weights + KV`, not weights alone.

Rules:

- Use backend-aware estimates.
- GGUF estimates come from metadata and parser output.
- vLLM and SGLang claims mirror their launch-time reservation behavior.
- A failed estimate is a loud placement failure, never a guess.
- Node-local model files are inspected by the owning node, not by assuming server disk access.

