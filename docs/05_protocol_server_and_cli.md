# Chapter 5: Protocol, Server, and CLI

## Goal of this chapter

This chapter covers the operational surface of Adolap:

- how messages travel over TCP
- what the binary protocol contains
- how the server handles requests
- how the CLI behaves as a development shell
- how meta commands expose internal state

## Why a binary protocol exists at all

Even though the main user-facing language is textual, the system communicates over a binary request/response protocol. This separation is useful because it lets the project evolve transport and UI behavior independently from the AQL language.

In other words:

- AQL is the language users type.
- The protocol is the language processes speak.

## Framing over TCP

The transport uses a very simple length-prefixed frame format:

```text
[u32 length][frame bytes...]
```

This is implemented in `crates/protocol/src/framing.rs`.

The design is intentionally minimal:

- the framing layer does not know message semantics
- it only reads and writes a size-prefixed payload
- actual interpretation happens in the codec layer

Representative helper:

```rust
pub async fn read_frame<R>(stream: &mut R) -> Result<Vec<u8>, AdolapError>
where
    R: AsyncReadExt + Unpin,
```

This separation is a good pattern because it keeps networking concerns from leaking into application message logic.

## Protocol message model

The protocol currently includes several message families.

### Client messages

- `QueryText(String)`
- `CreateDatabase`
- `CreateTable`
- `InsertRows`
- `IngestInto`
- `MetaCommand(String)`
- `Ping`

### Server messages

- `QueryResult(QueryResult)`
- `Ok(String)`
- `MetaResult(MetaResult)`
- `Error(String)`
- `Pong`

The most important user-visible result type is:

```rust
pub struct QueryResult {
    pub result_set: ResultSet,
    pub logical_plan: String,
    pub physical_plan: String,
}
```

That is a nice choice for a learning database because the result itself carries plan text. The client does not need a separate debug channel to see what happened.

## The server lifecycle

The TCP server accepts incoming clients and spawns a task per connection.

The top-level flow in `crates/server/src/tcp.rs` is:

1. bind a `TcpListener`
2. accept sockets in a loop
3. spawn `handle_connection`
4. read a frame
5. decode a client message
6. route it through the server handler
7. encode and write the response

That makes the server easy to understand and easy to instrument.

## Request dispatch

The central dispatch code is in `crates/server/src/handler.rs`.

That module translates wire messages into execution and storage actions. Examples:

- `QueryText` becomes parse, optimize, bind, plan, execute, and stringify.
- `CreateTable` becomes schema conversion plus catalog-backed table creation.
- `InsertRows` becomes protocol row conversion plus `TableWriter::insert_rows`.
- `MetaCommand` becomes server-side metadata inspection.

This is the key bridge between protocol and engine behavior.

## Why server-side meta commands are useful

The CLI could have tried to inspect the filesystem locally, but that would be the wrong architectural choice. The server is the source of truth for:

- the active data root
- resolved table metadata
- current process memory
- explain output and segment metadata

So the CLI forwards commands such as:

- `\databases`
- `\tables analytics`
- `\schema analytics.events`
- `\segments analytics.events`
- `\storage analytics.events`
- `\stats analytics.events`
- `\explain ...`

to the server via `MetaCommand` messages.

That means introspection remains consistent with the server process actually executing your queries.

## CLI interaction model

The CLI is built around a REPL and a small TCP client.

Important behavior:

- it connects to the server at startup
- it supports multiline input
- a query is considered complete when it ends with `;`
- lines beginning with `\` are treated as meta commands when no multiline query is active
- it stores history in `.adolap_history`
- it handles Ctrl+C differently depending on whether a query buffer is active

This makes the shell feel like a small professional database client rather than a raw demo program.

## Output and diagnostics

The CLI supports multiple output formats:

- `unicode`
- `ascii`
- `csv`
- `tsv`

It also exposes local toggles:

- `\plan on|off`
- `\timing on|off`
- `\profile on|off`
- `\format ...`
- `\memory`

This is particularly helpful when developing the engine because you can use the same client both for normal querying and for plan/debug feedback.

## Example shell session

```text
adolap> \plan on
plan on

adolap> \timing on
timing on

adolap> FROM analytics.events
.....> SELECT country, revenue
.....> FILTER revenue > 50
.....> ORDER BY revenue DESC;
```

This is a small detail, but the continuation prompt is an important ergonomic touch. It makes multiline queries much easier to read and edit.

## Code snippets that capture the architecture

### Client-side request timing

The CLI measures end-to-end request duration around each message exchange:

```rust
let request = encode_client_message(&message);
let started = Instant::now();
write_frame(&mut self.stream, &request).await?;
let response = read_frame(&mut self.stream).await?;
```

That makes timing and profile output cheap to expose in the shell.

### Server-side query execution

The server packages both data and plans into a single response:

```rust
Ok(ServerMessage::QueryResult(QueryResult::new(
    convert_batches_to_resultset(batches)?,
    logical_plan,
    physical_plan,
)))
```

That is a very practical design for development because the CLI can render useful context immediately.

## Why this layer matters

The protocol, server, and CLI layer is what turns the rest of the workspace into a usable system. Without it, Adolap would still have a parser and a storage engine, but it would not feel like a database you can actually operate.

This layer also creates a strong platform for future work such as:

- authentication
- richer typed protocol values
- remote administration
- streaming results
- long-running query cancellation

## Chapter takeaway

Adolap’s operational layer is intentionally simple but well-shaped. A framed binary protocol keeps transport explicit, the server owns execution and metadata truth, and the CLI acts as both a user interface and an engineering tool.