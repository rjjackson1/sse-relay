# sse-relay

An upstream model streams a completion exactly once. Meanwhile the user has the
same page open on a laptop and a phone, the tab was reloaded halfway through, and
a mobile network dropped the connection for four seconds.

`sse-relay` sits between the two: the producer POSTs each token chunk once,
and any number of browsers subscribe to the same stream id over Server-Sent
Events. Every subscriber receives the history it missed and then follows the live
tail. A client that reconnects sends `Last-Event-ID` and resumes without a gap.

- one producer, many consumers, no duplicated upstream calls
- bounded in-memory ring buffer per stream, so replay never grows without limit
- `Last-Event-ID` resumption, plus an explicit `gap` event when the history is
  genuinely gone
- heartbeat comment frames so proxies do not kill an idle connection
- slow consumers are dropped instead of stalling the producer, and are told to
  reconnect
- graceful shutdown: every stream is finished before the listener closes

No dependencies outside the standard library.

## Install

```bash
go install github.com/rjjackson1/sse-relay@latest
```

## Usage

```bash
sse-relay -addr :8080 -buffer 1024 -heartbeat 15s
```

Subscribe from one terminal (this blocks and prints frames as they arrive):

```bash
curl -N http://localhost:8080/streams/chat-42/events
```

Push chunks from another, as your model loop produces them:

```bash
curl -sXPOST --data-binary 'Hello'  http://localhost:8080/streams/chat-42
curl -sXPOST --data-binary ' world' http://localhost:8080/streams/chat-42
curl -sXPOST -H 'content-type: application/json' \
     -d '{"data":"!","done":true}' http://localhost:8080/streams/chat-42
```

The subscriber sees:

```
retry: 2000

id: 1
data: Hello

id: 2
data:  world

id: 3
data: !

event: done
data: {}
```

Reconnecting after the second chunk replays only what is missing:

```bash
curl -N -H 'Last-Event-ID: 2' http://localhost:8080/streams/chat-42/events
```

```
retry: 2000

id: 3
data: !

event: done
data: {}
```

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/streams/{id}` | append a chunk; body is the raw text, or JSON with `data` and `done` |
| `POST` | `/streams/{id}/done` | end a stream without publishing anything |
| `DELETE` | `/streams/{id}` | end a stream and drop its buffer |
| `GET` | `/streams/{id}/events` | SSE subscription, honours `Last-Event-ID` |
| `GET` | `/streams/{id}` | JSON stats for one stream |
| `GET` | `/streams` | JSON list of live streams |
| `GET` | `/healthz` | liveness probe |

A stream is created on its first `POST`. Ids are 1 to 128 characters of
`[A-Za-z0-9._-]`.

Stats look like this:

```json
{
  "id": "chat-42",
  "events": 3,
  "buffered": 3,
  "subscribers": 1,
  "done": true,
  "created_at": "2026-02-11T10:02:41.117Z",
  "updated_at": "2026-02-11T10:02:43.884Z"
}
```

## Event types on the wire

| Frame | Meaning |
|---|---|
| `id: N` + `data: ...` | one chunk; `N` is what the client should send back as `Last-Event-ID` |
| `event: done` | the producer finished, the server closes the response |
| `event: gap` | the requested resume point had already been evicted from the buffer; `data` carries the id you asked for |
| `event: lagged` | this subscriber could not keep up and was dropped; reconnect with the last id you saw |
| `: ping` | heartbeat comment, ignored by every SSE client |

Multi-line chunks are split across several `data:` lines, as the protocol
requires, and the client reassembles them with newlines in between.

## Configuration

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `:8080` | listen address |
| `-buffer` | `1024` | events kept per stream for replay |
| `-heartbeat` | `15s` | delay between `: ping` frames |
| `-retry` | `2s` | reconnect delay advertised in the `retry:` field |
| `-shutdown-timeout` | `10s` | grace period for in-flight requests |
| `-version` | `false` | print the version and exit |

| Environment | Meaning |
|---|---|
| `RELAY_TOKEN` | when set, `POST` and `DELETE` require `Authorization: Bearer <token>`; reads stay public |

Sizing the buffer: it is a per stream cap, so worst case memory is roughly
`streams x buffer x average chunk size`. For token deltas of ~4 bytes, 1024
events per stream is about 4KiB of history per conversation.

## Behaviour under pressure

Each subscriber gets its own bounded channel. If a consumer stops reading (a
suspended tab, a stalled TCP connection), the producer never blocks: the slow
subscriber is detached, receives `event: lagged`, and is expected to reconnect
with `Last-Event-ID`. As long as it comes back before the ring buffer wraps, the
replay is lossless.

On `SIGINT` or `SIGTERM` the relay finishes every stream first, so open
subscriptions receive `event: done` and terminate normally, and only then waits
for the HTTP server to drain.

## Running as a service

`deploy/systemd/sse-relay.service` runs the binary under systemd: a
dedicated user, a token read from an environment file instead of the unit
itself, and `TimeoutStopSec` set high enough to cover the graceful shutdown
described above.

```bash
go build -o sse-relay .
sudo install -m 755 sse-relay /usr/local/bin/sse-relay

sudo useradd --system --no-create-home --shell /usr/sbin/nologin sse-relay
sudo install -d -m 750 -o sse-relay -g sse-relay /etc/sse-relay
printf 'RELAY_TOKEN=%s\n' "$(openssl rand -hex 32)" | sudo tee /etc/sse-relay/env >/dev/null
sudo chmod 640 /etc/sse-relay/env
sudo chown root:sse-relay /etc/sse-relay/env

sudo install -m 644 deploy/systemd/sse-relay.service /etc/systemd/system/sse-relay.service
sudo systemctl daemon-reload
sudo systemctl enable --now sse-relay
```

`systemctl stop sse-relay` sends `SIGTERM`, which the process already
handles: it finishes every open stream before the listener stops accepting
connections. Adjust the `ExecStart` flags in the unit file to change the
listen address or buffer size; edit `/etc/sse-relay/env` and `systemctl
restart sse-relay` to rotate the token.

## Running with Docker

```bash
docker build -t sse-relay --build-arg VERSION=$(git describe --tags --always) .
docker run -d --name sse-relay \
  -p 8080:8080 \
  -e RELAY_TOKEN=$(openssl rand -hex 32) \
  sse-relay -addr :8080 -buffer 1024 -heartbeat 15s
```

The image is a single static binary on top of `distroless/static`, run as the
image's built-in nonroot user, so there is no shell inside the container to
break out to.

`docker stop` sends `SIGTERM` and waits 10 seconds by default before killing
the container; that matches the default `-shutdown-timeout`, but if you raise
the flag, raise the grace period to match:

```bash
docker stop --time 30 sse-relay
```

## Test

```bash
go test ./...
go test -race ./...
```

## License

MIT
