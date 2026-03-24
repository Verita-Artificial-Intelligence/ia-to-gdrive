import requests

url = "https://archive.org/download/moby-dick-herman-melville/Moby%20Dick_Herman%20Melville.pdf"

print("1. Testing GET without Range")
r1 = requests.get(url, stream=True)
print("Status:", r1.status_code)
print("URL:", r1.url)
size = int(r1.headers.get("content-length", 0))
print("Size:", size)
r1.close()

if size > 0:
    print("\n2. Testing GET with Range: bytes=1000- on CDN URL")
    r2 = requests.get(r1.url, headers={"Range": "bytes=1000-"}, stream=True)
    print("Status:", r2.status_code)
    print("Content-Length:", r2.headers.get("content-length"))
    r2.close()
    
    print("\n3. Testing original Vanity URL with Range: bytes=1000-")
    r3 = requests.get(url, headers={"Range": "bytes=1000-"}, stream=True)
    print("Status:", r3.status_code)
    print("Content-Length:", r3.headers.get("content-length"))
    r3.close()
    
    print("\n4. Testing original Vanity URL with Range: bytes=0-")
    r4 = requests.get(url, headers={"Range": "bytes=0-"}, stream=True)
    print("Status:", r4.status_code)
    r4.close()
