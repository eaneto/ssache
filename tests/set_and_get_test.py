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

# Set and Get key with spaces on value
response = client.set("key", "value with spaces")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.get("key")
expected_response = f"$17{CRLF}+value with spaces{CRLF}"
assert response.decode("utf-8") == expected_response

# Set overwriting value for a key
response = client.set("key", "new value")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.get("key")
expected_response = f"$9{CRLF}+new value{CRLF}"
assert response.decode("utf-8") == expected_response
