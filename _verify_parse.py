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

# Try to parse manually with correct layout
pos = 0
pos += 2
(num_stock,) = struct.unpack_from("<H", body, pos)
pos += 2
print("num_stock=%d pos=%d" % (num_stock, pos))

# First stock
print("\nFirst stock:")
market = body[pos]
code = body[pos + 1 : pos + 7].decode("ascii")
print("  pos=%d market=%d code=%s" % (pos, market, code))
pos += 9
print("  After header, pos=%d" % pos)
print("  Price data bytes:", body[pos : pos + 10].hex())

price, pos = _get_price(body, pos)
last_close_diff, pos = _get_price(body, pos)
open_diff, pos = _get_price(body, pos)
high_diff, pos = _get_price(body, pos)
low_diff, pos = _get_price(body, pos)
print(
    "  price=%d last_close_diff=%d open_diff=%d high_diff=%d low_diff=%d"
    % (price, last_close_diff, open_diff, high_diff, low_diff)
)
print(
    "  Price=%.2f last_close=%.2f open=%.2f high=%.2f low=%.2f"
    % (
        _cal_price(price, 0),
        _cal_price(price, last_close_diff),
        _cal_price(price, open_diff),
        _cal_price(price, high_diff),
        _cal_price(price, low_diff),
    )
)

reversed_bytes0, pos = _get_price(body, pos)
reversed_bytes1, pos = _get_price(body, pos)
vol, pos = _get_price(body, pos)
cur_vol, pos = _get_price(body, pos)
(amount_raw,) = struct.unpack_from("<I", body, pos)
amount = _get_volume(amount_raw)
pos += 4
s_vol, pos = _get_price(body, pos)
b_vol, pos = _get_price(body, pos)
_, pos = _get_price(body, pos)
_, pos = _get_price(body, pos)
bid1, pos = _get_price(body, pos)
ask1, pos = _get_price(body, pos)
bid_vol1, pos = _get_price(body, pos)
ask_vol1, pos = _get_price(body, pos)
bid2, pos = _get_price(body, pos)
ask2, pos = _get_price(body, pos)
bid_vol2, pos = _get_price(body, pos)
ask_vol2, pos = _get_price(body, pos)
bid3, pos = _get_price(body, pos)
ask3, pos = _get_price(body, pos)
bid_vol3, pos = _get_price(body, pos)
ask_vol3, pos = _get_price(body, pos)
bid4, pos = _get_price(body, pos)
ask4, pos = _get_price(body, pos)
bid_vol4, pos = _get_price(body, pos)
ask_vol4, pos = _get_price(body, pos)
bid5, pos = _get_price(body, pos)
ask5, pos = _get_price(body, pos)
bid_vol5, pos = _get_price(body, pos)
ask_vol5, pos = _get_price(body, pos)
(reversed_bytes4,) = struct.unpack_from("<H", body, pos)
pos += 2
_, pos = _get_price(body, pos)
_, pos = _get_price(body, pos)
_, pos = _get_price(body, pos)
_, pos = _get_price(body, pos)
(reversed_bytes9, active2) = struct.unpack_from("<hH", body, pos)
pos += 4
print("  After stock 1, pos=%d" % pos)
print("  Expected: 85 (next stock starts at pos 85)")

# Now check what byte is at pos=85
print("\nSecond stock at pos=85:")
print("  Byte at %d: 0x%02x" % (pos, body[pos]))
print("  Code at %d: %s" % (pos + 1, body[pos + 1 : pos + 7]))
