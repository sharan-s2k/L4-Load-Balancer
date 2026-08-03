#!/usr/bin/env python3
import argparse, asyncio, statistics, time
async def client(host,port,requests,payload,latencies):
    reader,writer=await asyncio.open_connection(host,port)
    for _ in range(requests):
        start=time.perf_counter();writer.write(payload);await writer.drain();received=b''
        while len(received)<len(payload):
            chunk=await reader.read(65536)
            if not chunk: raise RuntimeError('connection closed')
            received+=chunk
        latencies.append((time.perf_counter()-start)*1000)
    writer.close();await writer.wait_closed()
async def main():
    p=argparse.ArgumentParser();p.add_argument('--host',default='127.0.0.1');p.add_argument('--port',type=int,default=8080);p.add_argument('--clients',type=int,default=100);p.add_argument('--requests',type=int,default=100);p.add_argument('--payload-bytes',type=int,default=128);a=p.parse_args();lat=[];payload=b'x'*a.payload_bytes;start=time.perf_counter();await asyncio.gather(*(client(a.host,a.port,a.requests,payload,lat) for _ in range(a.clients)));elapsed=time.perf_counter()-start;lat.sort();pct=lambda p:lat[min(len(lat)-1,int(p*len(lat)))];total=a.clients*a.requests;print(f'requests={total}\nelapsed_seconds={elapsed:.3f}\nrequests_per_second={total/elapsed:.2f}\np50_ms={pct(.50):.3f}\np95_ms={pct(.95):.3f}\np99_ms={pct(.99):.3f}\nmean_ms={statistics.mean(lat):.3f}')
asyncio.run(main())
