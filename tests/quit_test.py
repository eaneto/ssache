from ssache_client import CRLF, SsacheClient

client = SsacheClient()

client.connect()
response = client.quit()

expected_response = f"+OK{CRLF}"

assert response.decode("utf-8") == expected_response
