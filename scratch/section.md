## Streaming HTTP calls

Streaming HTTP calls allow plugins to send HTTP requests and receive
HTTP responses of arbitrary size in chunks, without buffering the
complete request or response in either the plugin or the host.

A streaming HTTP call is opened using [`proxy_http_stream`], which
sends HTTP request headers and returns a new, unique stream identifier.
The host implicitly creates a stream context for it, and
[`proxy_on_context_create`] is not called.

HTTP request body is sent in chunks using [`proxy_http_stream_send`].
HTTP request trailers can be added using [`proxy_set_header_map_pairs`]
or [`proxy_add_header_map_value`] with `map_id` set to
`HTTP_REQUEST_TRAILERS`, before the final call to
[`proxy_http_stream_send`] with `end_of_stream` set to `true`.

HTTP response is delivered using the existing HTTP stream callbacks
([`proxy_on_response_headers`], [`proxy_on_response_body`]
and [`proxy_on_response_trailers`]) with `stream_context_id` set to
the stream identifier. The response can be received while the request
is still being sent.

Returning `PAUSE` from any of those callbacks stops the delivery of
further response events until processing is resumed using
[`proxy_continue_stream`] with `stream_type` set to `HTTP_RESPONSE`.
While paused, received response body is buffered by the host, and its
total size is reported in the subsequent [`proxy_on_response_body`]
calls. Hosts should apply backpressure to the upstream while paused,
and they may reset the streaming HTTP call when the buffered response
exceeds a host-defined limit. Returning `CONTINUE` from
[`proxy_on_response_body`] indicates that the plugin consumed the
buffered response body, which is then discarded by the host.

The streaming HTTP call can be reset at any time using
[`proxy_close_stream`] with `stream_type` set to either `HTTP_REQUEST`
or `HTTP_RESPONSE`.

[`proxy_on_http_stream_close`] is always called exactly once, after
both request and response are complete, or after the streaming HTTP
call is reset, fails, or exceeds its `timeout`. The stream identifier
is invalid after that callback returns.

Streaming HTTP calls are associated with `parent_context_id` (either
`plugin_context_id` or `stream_context_id`) and bound to its lifetime.
Outstanding streaming HTTP calls are reset when their parent context
is finalized, and [`proxy_on_http_stream_close`] is called for each of
them before that happens.


### Functions exposed by the host

#### `proxy_http_stream`

* params:
  - `i32 (uint32_t) parent_context_id`
  - `i32 (const char *) upstream_name_data`
  - `i32 (size_t) upstream_name_size`
  - `i32 (const uint8_t *) serialized_headers_data`
  - `i32 (size_t) serialized_headers_size`
  - `i32 (bool) end_of_stream`
  - `i32 (uint32_t) timeout`
  - `i32 (uint32_t *) return_stream_id`
* returns:
  - `i32 (`[`proxy_status_t`]`) status`

Opens a streaming HTTP call to upstream (`upstream_name_data`,
`upstream_name_size`) and sends HTTP request with [serialized] headers
(`serialized_headers_data`, `serialized_headers_size`).

When `end_of_stream` is `true`, the HTTP request consists only of
headers, and [`proxy_http_stream_send`] cannot be used.

The `timeout` (in milliseconds) applies to the entire streaming HTTP
call, from opening until both request and response are complete.
Value `0` means no timeout, in which case the host may still enforce
its own limits.

HTTP request body can be sent using [`proxy_http_stream_send`] with
the returned unique stream identifier (`return_stream_id`).

Returned `status` value is:
- `OK` on success.
- `UNKNOWN_RESOURCE_ID` for unknown `parent_context_id`.
- `BAD_ARGUMENT` for unknown `upstream`, or when `headers` are missing
  required `:authority`, `:method` and/or `:path` values.
- `INTERNAL_FAILURE` when the host failed to open the streaming HTTP call.
- `INVALID_MEMORY_ACCESS` when `upstream_name_data`,
  `upstream_name_size`, `serialized_headers_data`,
  `serialized_headers_size` and/or `return_stream_id` point to invalid
  memory address.


#### `proxy_http_stream_send`

* params:
  - `i32 (uint32_t) stream_id`
  - `i32 (const uint8_t *) body_data`
  - `i32 (size_t) body_size`
  - `i32 (bool) end_of_stream`
* returns:
  - `i32 (`[`proxy_status_t`]`) status`

Sends a chunk of HTTP request body (`body_data`, `body_size`) on the
streaming HTTP call `stream_id` previously opened using
[`proxy_http_stream`].

The data is copied by the host, so the plugin can release its memory
as soon as this function returns.

When `end_of_stream` is `true`, this is the final chunk of HTTP request
body, and HTTP request trailers, if they were added, are sent after it.
`body_size` may be `0` in order to end the HTTP request without sending
more data.

The host accepts the data even when its send buffer for `stream_id`
is above its limit, but it notifies the plugin about it using
[`proxy_on_http_stream_backpressure`], and plugins should stop sending
data until notified that the backpressure was released. Hosts may reset
the streaming HTTP call if plugin continues sending data, and its send
buffer exceeds a host-defined hard limit.

Returned `status` value is:
- `OK` on success.
- `UNKNOWN_RESOURCE_ID` for unknown `stream_id`.
- `BAD_ARGUMENT` when the HTTP request for `stream_id` was already
  ended.
- `INVALID_MEMORY_ACCESS` when `body_data` and/or `body_size` point
  to invalid memory address.


### Callbacks exposed by the Wasm module

#### `proxy_on_http_stream_backpressure`

* params:
  - `i32 (uint32_t) stream_id`
  - `i32 (bool) above_limit`
* returns:
  - none

Called when the amount of HTTP request body buffered by the host for
the streaming HTTP call `stream_id`, and not yet sent upstream, crosses
the host-defined limits.

When `above_limit` is `true`, the plugin should stop calling
[`proxy_http_stream_send`] for `stream_id`. When the data originates
from a downstream HTTP request, this can be achieved by returning
`PAUSE` from [`proxy_on_request_body`].

When `above_limit` is `false`, the buffered data was drained, and the
plugin can resume sending data (e.g. using [`proxy_continue_stream`]
with `stream_type` set to `HTTP_REQUEST` for the paused downstream
HTTP request).

Calls with `above_limit` set to `true` and `false` always alternate,
starting with `true`.


#### `proxy_on_http_stream_close`

* params:
  - `i32 (uint32_t) stream_id`
* returns:
  - none

Called when the streaming HTTP call `stream_id` opened using
[`proxy_http_stream`] is closed, either after both HTTP request
and response were completed, or after it was reset, failed, or exceeded
its `timeout`.

The HTTP status code and status message can be retrieved using
[`proxy_get_status`]. Status code `0` means that the HTTP response
headers were not received.

Streaming HTTP calls that completed successfully always deliver
the HTTP response with `end_of_stream` set to `true` (or HTTP response
trailers) before this callback is called.

No other callbacks for `stream_id` are called after this callback.


