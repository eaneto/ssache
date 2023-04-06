from ssache_client import CRLF, SsacheClient

client = SsacheClient()

# Ping without message
client.connect()
response = client.ping()
expected_response = f"+PONG{CRLF}"
assert response.decode("utf-8") == expected_response

# Ping with simple message
client.connect()
response = client.ping("message")
expected_response = f"$7{CRLF}+message{CRLF}"
assert response.decode("utf-8") == expected_response

# Ping with message with spaces
client.connect()
response = client.ping("message with spaces")
expected_response = f"$19{CRLF}+message with spaces{CRLF}"
assert response.decode("utf-8") == expected_response
