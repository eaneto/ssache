from time import sleep

from ssache_client import CRLF, SsacheClient

client = SsacheClient()

# Set and Get key with single word on value
client.connect()
response = client.set("key", "value")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.get("key")
expected_response = f"$5{CRLF}+value{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.expire("key", 2000)
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

sleep(3)

response = client.get("key")
expected_response = f"$-1{CRLF}"
assert response.decode("utf-8") == expected_response
