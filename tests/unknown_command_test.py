from ssache_client import CRLF, SsacheClient

client = SsacheClient()

# Check error return with unknown command
client.connect()
response = client.unknown()
expected_response = f"-ERROR unknown command{CRLF}"
assert response.decode("utf-8") == expected_response
