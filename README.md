# SSache

A simple implementation of an in-memory cache server inspired by
[redis][0].

## Internal structures

SSache stores all data on a sharded `HashMap`, the default number of
shards is 8 but can be changed via CLI using the `-s` option. Any
stored data can have an expiration time defined by the `EXPIRE`
command, a background thread runs every second checking for
expirations so the actual deletion might no be very precise. SSache
also supports replication for other servers, if any replicas are
configured ssache will append all write operations(`SET`, `INCR` and
`DECR`) to an internal log(kept for each replica) sharded by the same
number of shards used for the storage. The replication process runs
concurrently with write operations because while it's sending the
commands to the replicas only an offset is updated, after all possible
operations are sent to the replicas(with a maximum of 100 operations)
the replication process blocks writes to the log, "drains" all the
replicated operations and resets the offset.

If you need to dump all the stored keys to a file you can either use
the `SAVE` command or configure ssache to dump everything in a given
interval, this is disabled by default. With a dump file you can then
`LOAD` all the keys in-memory if needed.

## Building

```shell
cargo build --release
```

## Tests

There are unit tests written in rust and integration tests written in
python. These tests can be ran executing the `tests/test_runner.py`
script(they need a built release version of ssache to run).

## Commands

To send the commands to ssache you need to stablish a tcp connection
with the server, the protocol used is based on [RESP][1].

### SET

SET [key] [value]

Writes a value to a key. Returns OK if the write was done
successfully.

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

Reads the value of a key. Returns a bulk string with the value or -1
if the key doesn't exist.

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

INCR [key]

Increments the value stored in a key, initializes with zero if the key
doesn't exist. Returns the incremented value.

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
INCR key
:0
```

### DECR

Decrements the value stored in a key, initializes with zero if the key
doesn't exist. Returns the incremented value.

DECR [key]

```shell
$ telnet 127.0.0.1 7777
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
DECR key
:0
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

Simple ping message to check the connection with the server.

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

Closes the connection with the server.

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
