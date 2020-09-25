# Framed - a replacement for tokio Framed<TcpStream>/UdpFramed
--------------------------------------------------------------

### Feature
- targets on tokio 0.2
- supports shutdown for UdpFramed with little overhead
- supports shutdown for Framed<TcpStream>(or TcpFramed) with little overhead
- faster UdpFramed
- UdpFramed that will make use of all bytes in packet (tokio's UdpFramed will drop the rest of the buffer after the first message was decoded)

### Performance Statistics

For tcp, we use 2,700,000 bytes of text as input.  
Output is 100,000 msgs in length 26.  
Decoder is LinesCodec from tokio.  
```markdown
tcp:       160ns/iter, total: 16051084ns
tokio tcp: 174ns/iter, total: 17432640ns
```

For udp, we use same input and output settings.  
Just need to limit the speed by some threshold.  
Send speed is at 120MB/sec (packet size: 65506, sleep micros: 520)  
```markdown
udp:       10653ns/iter, total: 1065372741ns
tokio udp: 85072ns/iter, total: 8507242417ns
```

There are several findings:  
1. tokio's UdpFramed is awfully slow. At least 7 times slower than our implementation.
2. adding Cache in front of decoder doesn't make it faster.
3. receive in one thread and decode in another doesn't make it faster.
