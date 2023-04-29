from time import sleep

from ssache_client import (
    CRLF,
    SsacheClient,
    find_and_kill_ssache_process,
    initialize_ssache,
    kill_ssache,
)


def set_and_check(client, key):
    response = client.set(key, "value")
    expected_response = f"+OK{CRLF}"
    assert response.decode("utf-8") == expected_response


def get_and_check_value_not_found(client, key):
    response = client.get(key)
    expected_response = f"$-1{CRLF}"
    assert response.decode("utf-8") == expected_response


def get_and_check_found_value(client, key):
    response = client.get(key)
    expected_response = f"$5{CRLF}+value{CRLF}"
    assert response.decode("utf-8") == expected_response


find_and_kill_ssache_process()

pid = initialize_ssache(args="-e --save-job-interval 1")
try:
    client = SsacheClient()
    client.connect()

    set_and_check(client, "key-1")
    set_and_check(client, "key-2")
    set_and_check(client, "key-3")
    set_and_check(client, "key-4")

    # Waits until the scheduled save job executes
    sleep(65)
finally:
    kill_ssache(pid)


pid = initialize_ssache(args="-e --save-job-interval 1")

try:
    client.connect()
    get_and_check_value_not_found(client, "key-1")
    get_and_check_value_not_found(client, "key-2")
    get_and_check_value_not_found(client, "key-3")
    get_and_check_value_not_found(client, "key-4")

    response = client.load()
    expected_response = f"+OK{CRLF}"
    assert response.decode("utf-8") == expected_response

    get_and_check_found_value(client, "key-1")
    get_and_check_found_value(client, "key-2")
    get_and_check_found_value(client, "key-3")
    get_and_check_found_value(client, "key-4")
finally:
    kill_ssache(pid)
