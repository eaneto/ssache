from ssache_client import CRLF, SsacheClient

client = SsacheClient()

# Set key and increment
client.connect()
response = client.set("key", "1")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.incr("key")
expected_response = f":2{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.get("key")
expected_response = f"$1{CRLF}+2{CRLF}"
assert response.decode("utf-8") == expected_response

# Set key and increment twice
response = client.set("key", "1")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.incr("key")
expected_response = f":2{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.incr("key")
expected_response = f":3{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.get("key")
expected_response = f"$1{CRLF}+3{CRLF}"
assert response.decode("utf-8") == expected_response

# Increment key without set
response = client.incr("key-without-value")
expected_response = f":0{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.get("key-without-value")
expected_response = f"$1{CRLF}+0{CRLF}"
assert response.decode("utf-8") == expected_response

# Increment key with string stored
response = client.set("key", "value")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.incr("key")
expected_response = f"-ERROR the value is not a valid number{CRLF}"
assert response.decode("utf-8") == expected_response

# Set key and decrement
client.connect()
response = client.set("key", "1")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.decr("key")
expected_response = f":0{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.get("key")
expected_response = f"$1{CRLF}+0{CRLF}"
assert response.decode("utf-8") == expected_response

# Set key and decrement twice
response = client.set("key", "2")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.decr("key")
expected_response = f":1{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.decr("key")
expected_response = f":0{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.get("key")
expected_response = f"$1{CRLF}+0{CRLF}"
assert response.decode("utf-8") == expected_response

# Decrement key without set
response = client.decr("key-without-value-decr")
expected_response = f":0{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.get("key-without-value-decr")
expected_response = f"$1{CRLF}+0{CRLF}"
assert response.decode("utf-8") == expected_response

# Decrement key to negative value
response = client.decr("negative-value")
expected_response = f":0{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.decr("negative-value")
expected_response = f":-1{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.get("negative-value")
expected_response = f"$2{CRLF}+-1{CRLF}"
assert response.decode("utf-8") == expected_response

# Decrement key with string stored
response = client.set("key", "value")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

response = client.decr("key")
expected_response = f"-ERROR the value is not a valid number{CRLF}"
assert response.decode("utf-8") == expected_response
