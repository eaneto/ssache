# SSache

Super Simple cache.

A very simple implementation of a cache system inspired by
[redis][0]. The goal of this project is to build a usable simple cache
system that i can use on my own website.

## Commands supported

To send the commands to ssache you need to stablish a tcp connection, the protocol used is based on [RESP][1].

- GET
- SET
- PING
- QUIT

## TODOs

- Support storing integers
- Keep connection with client open
- Distributed

## Building

```shell
cargo build
```

## Usage example

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
SET key some-value
+OK
Connection closed by foreign host.

$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
GET key
$10
+some-value
Connection closed by foreign host.
```

[0]: https://redis.io/
[1]: https://redis.io/docs/reference/protocol-spec/
