from time import sleep

from ssache_client import (
    CRLF,
    SsacheClient,
    find_and_kill_ssache_process,
    initialize_ssache,
    kill_ssache,
)

# Kill main process to reinitialize with replicas configuration
find_and_kill_ssache_process()

replica_1_pid = initialize_ssache(7778)
replica_2_pid = initialize_ssache(7779)
primary_pid = initialize_ssache(
    port=7777,
    args="-r --replicas 127.0.0.1:7778 --replicas 127.0.0.1:7779 --replication-interval 1",
)

primary_client = SsacheClient()
primary_client.connect()

try:
    # Testing replication of key overwrite
    for i in range(10):
        response = primary_client.set("key", str(i))
        expected_response = f"+OK{CRLF}"
        assert response.decode("utf-8") == expected_response

    # Wait a little more than one minute so that the replication job may run
    sleep(65)

    for port in [7778, 7779]:
        client = SsacheClient()
        client.connect(port)
        response = client.get("key")
        expected_response = f"$1{CRLF}+9{CRLF}"
        assert response.decode("utf-8") == expected_response

    # Testing replication with multiple different keys
    for i in range(25):
        response = primary_client.set(f"key-{i}", f"value-{i:02d}")
        expected_response = f"+OK{CRLF}"
        assert response.decode("utf-8") == expected_response

    # Wait a little more than one minute so that the replication job may run
    sleep(65)

    for port in [7778, 7779]:
        client = SsacheClient()
        client.connect(port)
        for i in range(25):
            response = client.get(f"key-{i}")
            expected_response = f"$8{CRLF}+value-{i:02d}{CRLF}"
            assert response.decode("utf-8") == expected_response

    # Testing replication increments and decrements
    response = primary_client.set("int-key", "0")
    expected_response = f"+OK{CRLF}"
    assert response.decode("utf-8") == expected_response

    for i in range(3):
        response = primary_client.incr("int-key")
        expected_response = f":{i+1}{CRLF}"
        assert response.decode("utf-8") == expected_response

    for i in range(3, 0, -1):
        response = primary_client.decr("int-key")
        expected_response = f":{i-1}{CRLF}"
        assert response.decode("utf-8") == expected_response

    # Wait a little more than one minute so that the replication job may run
    sleep(65)

    for port in [7778, 7779]:
        client = SsacheClient()
        client.connect(port)
        response = client.get("int-key")
        expected_response = f"$1{CRLF}+0{CRLF}"
        assert response.decode("utf-8") == expected_response
finally:
    kill_ssache(replica_1_pid)
    kill_ssache(replica_2_pid)
    kill_ssache(primary_pid)
