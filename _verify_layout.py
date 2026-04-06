import struct, socket, sys

sys.path.insert(0, "src")
from tdxapi.protocol.requests import build_quote_request
from tdxapi.parser.quote_parser import _get_price, _cal_price, _get_volume

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(5)
sock.connect(("218.75.126.9", 7709))
sock.sendall(bytes.fromhex("0c 02 18 93 00 01 03 00 03 00 0d 00 01"))
try:
    sock.recv(8192)
except:
    pass
sock.sendall(bytes.fromhex("0c 02 18 94 00 01 03 00 03 00 0d 00 02"))
try:
    sock.recv(8192)
except:
    pass

req = build_quote_request([(0, "000001"), (1, "600519")])
sock.sendall(req)
data = b""
while True:
    try:
        chunk = sock.recv(8192)
        if not chunk:
            break
        data += chunk
    except socket.timeout:
        break
sock.close()

body = data[16:]
print("Body len:", len(body))

# Test layout: body[0:2] = num_stock?
num = struct.unpack_from("<H", body, 0)[0]
print("body[0:2] as <H:", num)

# Layout: header at 0-3, num_stock at 2-3, stocks start at 4
# First stock: market at 4, code at 5-10
# Second stock: market at 85, code at 86-91

print("\nFirst stock (pos=4):")
market1 = body[4]
code1 = body[5:11].decode("ascii")
print("  market=%d code=%s" % (market1, code1))

print("\nSecond stock (pos=85):")
market2 = body[85]
code2 = body[86:92].decode("ascii")
print("  market=%d code=%s" % (market2, code2))

# Now parse first stock with my current parse_quotes
print("\nCurrent parse_quotes approach:")
pos = 0
(num_stock,) = struct.unpack_from("<H", body, pos)
pos += 2
print("num_stock=%d pos=%d" % (num_stock, pos))

# Loop iteration 1: pos += 4
pos += 4
print("After pos+=4: pos=%d" % pos)
market = body[pos]
code_raw = body[pos + 1 : pos + 7]
print("  market=%d code=%s" % (market, code_raw.decode("ascii")))
# Price starts at pos+9 = 4+4+9 = 17
price_pos = pos + 9
print("  price_pos=%d" % price_pos)
price, _ = _get_price(body, price_pos)
print("  price=%d -> %.2f" % (price, price / 100.0))
