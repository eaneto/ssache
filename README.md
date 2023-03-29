# SSache

Super Simple cache.

A very simple implementation of a cache system inspired by
[redis][0]. The goal of this project is to build a usable simple cache
system that i can use on my own website.

## Commands supported

To send the commands to ssache you need to stablish a tcp connection, the protocol used is based on [RESP][1].

- GET
- SET
- SAVE
- PING
- QUIT

## TODOs

- Keep connection with client open
- Integration tests
- Flush data to disk once every hour
- Define ttl to data
  - Implement EXPIRE and EX on GET
- Support storing integers
  - Implement INCR and DECR
- Distributed storage
  - Simple last write wins algorithm

## Building

```shell
cargo build
```

## Examples

### SET

SET [key] [value]

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
SET key some-value
+OK
Connection closed by foreign host.
```

### GET

GET [key]

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
GET key
$10
+some-value
Connection closed by foreign host.
```

### QUIT

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
QUIT
+OK
Connection closed by foreign host.
```

### SAVE

Saves a dump file with all the data in memory.

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
SAVE
+OK
Connection closed by foreign host.
```

### LOAD

Loads the content on the dump file into memory.

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
LOAD
+OK
Connection closed by foreign host.
```
### PING

PING [Optional: message]

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
PING
+PONG
Connection closed by foreign host.
```

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
PING message
$7
+message
Connection closed by foreign host.
```

### QUIT

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
QUIT
+OK
Connection closed by foreign host.
```

[0]: https://redis.io/
[1]: https://redis.io/docs/reference/protocol-spec/
