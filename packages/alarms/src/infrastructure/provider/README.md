# Provider adapters

HTTP adapters implement the application ports without leaking Node transport types upward. The
message sender emits `DEFINITELY_NOT_SENT` only when synchronous request construction throws before
a `ClientRequest` exists and before `request.end` can run. A synchronous `request.end` throw and all
asynchronous timeout/reset/abort/response errors remain `AMBIGUOUS`; socket events are not treated as
proof that the provider did not receive a request.

Provider error bodies are reduced to allowlisted codes. Raw response messages, recipients, and
notification content do not cross the adapter for logging.
