# SSache

Super Simple cache.

A very simple implementation of a cache system inspired by
[redis][0]. The goal of this project is to build a usable simple cache
system that i can use on my own website.

## Commands supported

To send the commands to ssache you need to stablish a tcp connection,
the protocol used is based on [RESP][1].

- GET
- SET
- EXPIRE
- INCR
- DECR
- SAVE
- LOAD
- PING
- QUIT

## Building

```shell
cargo build
```

## Tests

There are unit tests written in rust and integration tests written in
python. These tests can be ran executing the `tests/test_runner.py`
script.

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
```

### EXPIRE

EXPIRE [key] [ttl]

Sets expiration time for a key, the ttl is set in milliseconds.

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
EXPIRE key 1000
+OK
```

### INCR

Increments the value stored in a [key], initializes with zero if the
key doesn't exist. Returns the incremented value.

INCR [key]

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
INCR key
:0
```

### DECR

Decrements the value stored in a [key], initializes with zero if the
key doesn't exist. Returns the incremented value.

DECR [key]

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
DECR key
:0
```

### QUIT

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
QUIT
+OK
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
```

### LOAD

Loads the content of the dump file into memory.

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
LOAD
+OK
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
```

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
PING message
$7
+message
```

### QUIT

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
QUIT
+OK
```

[0]: https://redis.io/
[1]: https://redis.io/docs/reference/protocol-spec/
