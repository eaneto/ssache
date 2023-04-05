from ssache_client import CRLF, SsacheClient

client = SsacheClient()

client.connect()
response = client.ping()

expected_response = f"+PONG{CRLF}"

assert response.decode("utf-8") == expected_response

client.connect()
response = client.ping("message")

expected_response = f"$7{CRLF}+message{CRLF}"

assert response.decode("utf-8") == expected_response
