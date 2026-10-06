Fixes #67 (and #95).

## Motivation

`proxy_http_call` requires the complete request body in Wasm memory, and
delivers the complete response body at once. That rules out request
mirroring and spilling to external storage, and any callout whose payload
may be larger than the Wasm VM's memory.

Concrete example: an AI governance gateway (Proxy-Wasm in Envoy) that
decides whether to block a request before it reaches the inference
provider, while capturing the raw request body, which is unbounded in
size, to a spill store. The plugin needs to copy each downstream body
chunk into an HTTP callout as it arrives, without holding the whole body
in Wasm memory. When the spill store is slower than the client, the
plugin needs to slow the downstream request down.

## Proposal

New hostcalls:
- `proxy_http_stream(parent_context_id, upstream, headers, end_of_stream, timeout, &stream_id)`
- `proxy_http_stream_send(stream_id, body, end_of_stream)`

New callbacks:
- `proxy_on_http_stream_backpressure(stream_id, above_limit)`: send-side
  flow control. The plugin is expected to `PAUSE` the downstream
  `proxy_on_request_body` while above the limit, and to resume it with
  `proxy_continue_stream` when the buffer drains.
- `proxy_on_http_stream_close(stream_id)`: always called exactly once;
  details are available through `proxy_get_status`.

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

Example flow (spill-to-store):

```
on_request_headers(ctx):   id = proxy_http_stream(ctx, "spill", hdrs, false, 0)
on_request_body(ctx, n):   chunk = get_buffer_bytes(ctx, HTTP_REQUEST_BODY)
                           proxy_http_stream_send(id, chunk, eos)
                           return paused_by_backpressure ? PAUSE : CONTINUE
on_http_stream_backpressure(id, above):
                           if !above: proxy_continue_stream(ctx, HTTP_REQUEST)
on_response_headers(id, status, eos): record spill result
on_http_stream_close(id):  done
```

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
   once #110 lands, as long as `proxy_get_status` stays callable from it.
3. gRPC: message-level streaming already exists (`proxy_grpc_stream` /
   `proxy_grpc_send`), so this PR is HTTP-only. If #68 moves gRPC into
   SDKs, gRPC streaming would sit on top of these hostcalls. Otherwise,
   `proxy_grpc_send` probably needs the same backpressure callback.
4. Should `timeout = 0` mean "no timeout", or "host default"?
5. Naming: `proxy_http_stream` follows `proxy_grpc_stream`, but it is
   easy to confuse with the downstream "HTTP streams" section.
