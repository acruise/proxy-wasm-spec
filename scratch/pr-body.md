Fixes #67 (and #95).

## Motivation

`proxy_http_call` requires the complete request body in Wasm memory, and
delivers the complete response body at once. That rules out request
mirroring and spilling to external storage, and any callout whose payload
may be larger than the Wasm VM's memory.

Concrete example: synchronous AI governance in Envoy. Before a request
reaches the inference provider, the plugin:

1. tees the raw request body to two streaming calls at once: a spill
   store (for audit) and a governance endpoint, which scans the content
   as it arrives and may deny early;
2. waits until both calls have closed, whatever their outcome;
3. forwards the request, or rejects it with 403 (or 503 if either call
   failed).

Request bodies can be larger than the Wasm VM's memory, so the plugin
must never hold all of a body. It must also slow down when either call
falls behind.

## Proposal

New hostcalls:
- `proxy_http_stream(parent_context_id, upstream, headers, end_of_stream, timeout, &stream_id)`
- `proxy_http_stream_send(stream_id, body, end_of_stream)`

New callbacks:
- `proxy_on_http_stream_backpressure(stream_id, above_limit)`: send-side
  flow control. While a call is above its limit, the plugin stops
  sending and leaves the unsent data in the paused downstream request's
  buffer.
- `proxy_on_http_stream_close(stream_id)`: called **exactly once** per
  call, whether it succeeded, was reset, failed, timed out, or its parent
  context was torn down. Details are available through
  `proxy_get_status`. That guarantee is what lets a plugin wait reliably
  for several calls to finish.

Reused, per #66:
- The response is delivered through `proxy_on_response_headers`,
  `proxy_on_response_body` and `proxy_on_response_trailers`, with
  `stream_context_id = stream_id`. The new hostcall is what tells the host
  that the plugin understands this, so plugins that don't use it are
  unaffected, as discussed in #66.
- Request trailers are set with `proxy_set_header_map_pairs(stream_id,
  HTTP_REQUEST_TRAILERS, ...)` before the final send.
- `proxy_close_stream(stream_id, ...)` resets the call, and
  `proxy_continue_stream(stream_id, HTTP_RESPONSE)` releases a paused,
  buffered response body.

A call's parent can be either a request context or the plugin context.
A call parented to the plugin context outlives the request. For example,
the spill call can then finish recording, or be explicitly marked
partial, after the client aborts.

## Worked example: cap, tee, sync, forward

This flow works end to end with only this PR. Requests over a
configured cap are rejected with 413 ("spiked"). The cap is enforced on
the host buffer, so it can be much larger than Wasm memory, and the
plugin's memory stays at about one chunk.

```text
on_request_headers(ctx):
  if content-length > CAP: send_local_response(413); return PAUSE
  spill = proxy_http_stream(PLUGIN_CTX, "spill", [PUT /spill/{key}], false, 0)
  gov   = proxy_http_stream(ctx, "governance", [POST /verdict, x-spill-key], false, 30s)
  return PAUSE

on_request_body(ctx, body_size, eos):          -- body stays in the host buffer
  if body_size > CAP: reset legs; send_local_response(413); return PAUSE
  if !spill.above && !gov.above: tee(ctx)      -- send from offset tee_off to each live call
  return PAUSE

on_http_stream_backpressure(id, above):
  leg(id).above = above
  if !blocked(): tee(ctx)                      -- request is paused, buffer still accessible

on_response_body(gov, n, eos):  buffer verdict JSON (PAUSE until eos)

on_http_stream_close(id):
  leg(id).done = true
  if spill.done && gov.done:                   -- wait for both calls
    if either failed:   send_local_response(503)
    elif verdict.deny:  send_local_response(403)
    else:               proxy_continue_stream(ctx, HTTP_REQUEST)   -- original body
```

The full walkthrough is in the linked notes. It includes the version
with no size cap, early-deny policy, and client-abort handling.

## Related issues

This PR covers the callout side. Handling request bodies **with no
size cap** end to end also needs these existing issues:

- **#63:** in Envoy, `PAUSE` from a body callback maps to
  `StopIterationAndBuffer`, which returns 413 on buffer overflow instead
  of applying backpressure to the client. proxy-wasm-cpp-host already
  passes `StopIterationAndWatermark` (`2`) through, but the spec
  and SDKs don't expose it. The text of
  `proxy_on_http_stream_backpressure` notes this limitation.
- **#64 / #65:** if the body is drained to the spill store instead of
  buffered, an approved request has to be re-sent by streaming the body
  back into the paused request. That needs "flush partial body" and
  control of `end_of_stream`.
- Streaming local responses are another missing piece. They would be
  needed to send the request to the provider as a callout and relay its
  streamed (SSE) response back to the client.

## Possible future work: substituting a governed request

Beyond allow/deny, governance may return a governed version of the
request (PII redacted, system instructions replaced) to forward instead.
With a size cap, this already works on top of this PR:

- the governance call's response headers carry the verdict and any
  header mutations;
- its response body is the governed request body;
- the plugin drops the original from the paused request's buffer,
  appends the governed bytes, fixes `content-length` (the headers are
  still paused), and continues the request.

Without a size cap, it needs the same #64/#65 work as above. A full
governance protocol is out of scope here, but Envoy's `ext_proc`
`FULL_DUPLEX_STREAMED` mode is the closest prior art.

## Host implementation notes

The design maps directly onto Envoy's `Http::AsyncClient::Stream`:
`sendHeaders`, `sendData` and `sendTrailers`; `reset()` for
`proxy_close_stream`; and `setWatermarkCallbacks` with
`SidestreamWatermarkCallbacks::onSidestream{Above,BelowLow}Watermark`
for `proxy_on_http_stream_backpressure`.

Today, buffered `proxy_http_call` ids in proxy-wasm-cpp-host are tokens
from a counter separate from context ids, and the response is stashed on
the parent context. Because streaming calls reuse the `proxy_on_response_*`
callbacks keyed by `stream_context_id`, the spec requires `stream_id` to be
allocated from the context-id namespace and never to collide with a live
context. Hosts therefore need a lightweight callout context object, with its
own buffer and header-map slots, registered in the context table. This is new
host code, not new ABI.

On the receive side, Envoy's async stream doesn't expose read-disable, so
a paused response body would be buffered up to the stream's buffer limit
and then reset. The spec allows this ("should apply backpressure… may
reset… hard limit").

## Open questions

1. Host features: this should be gated on `HAS_HTTP_CALLOUTS_STREAMING`
   from #91. I left out the "gated on" lines so this PR doesn't depend on
   #91, and will add them after #91 lands.
2. Lifecycle (#110): the stream context is created implicitly, without
   `proxy_on_context_create`, which matches the direction in #110.
   `proxy_on_http_stream_close` could become `proxy_on_context_finalize`
   once #110 lands, as long as `proxy_get_status` stays callable from it
   and it's still called exactly once.
3. gRPC: message-level streaming already exists (`proxy_grpc_stream` /
   `proxy_grpc_send`), so this PR is HTTP-only. The existing gRPC streams
   are enough to *move* the data in the example above, but in Envoy
   they're missing what makes the flow reliable:
   - **No send-side backpressure.** `proxy_grpc_send` always accepts,
     although Envoy's async gRPC stream tracks
     `isAboveWriteBufferHighWatermark`.
   - **No exactly-once close.** `proxy_grpc_cancel` erases the stream
     without a callback, and when the parent context is torn down,
     `~Context` calls `resetStream()` after `onDone`/`onDelete`, so
     `proxy_on_grpc_close` never fires.
   - **No timeout.** The spec text for `proxy_grpc_stream` mentions one,
     but the hostcall takes no `timeout` parameter, and Envoy doesn't set
     one.

   If #68 moves gRPC into SDKs, gRPC streaming would sit on top of these
   hostcalls and get all three for free. Otherwise, `proxy_grpc_stream`
   needs the same three things, in this PR or a follow-up.
4. Should `timeout = 0` mean "no timeout", or "host default"?
5. Naming: `proxy_http_stream` follows `proxy_grpc_stream`, but it is
   easy to confuse with the downstream "HTTP streams" section.
6. Zero-copy sending: today each chunk crosses the Wasm boundary once per
   destination. A hostcall such as
   `proxy_http_stream_send_buffer(stream_id, source_context_id, buffer_id, start, size, end_of_stream)`
   would let hosts tee and mirror without copying through Wasm memory.
   In Envoy that just moves buffer slices. Worth adding here, or as a
   follow-up?
