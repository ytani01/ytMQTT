# Mqtt


## Install

## Usage

Beebotte Publisher
```python3
from Mqtt import BeebottePublisher as BBT

bbt = BBT('token_XXXX')
bbt.start()

while True:
    data = input()
    if data == '':
        break
    bbt.send_data(data, [ 'ch1/res1', 'ch1/res2' ])

bbt.end()
```

Beebotte Subscriber (async)
```python3
from Mqtt import BeebotteSubscriber as BBT

def cb_recv_data(data, topic, ts):
    print('%s [%s] %s' % (BBT.ts2datestr(ts), topic, data))

bbt = BBT(cb_recv_data, [ 'ch1/res1', 'ch2/res2' ], 'token_XXXX')
bbt.start()

for i in range(10):
    time.sleep(1)

bbt.end()
```

Beebotte Subscriber (sync)
```python3
from Mqtt import BeebotteSubscriber as BBT

bbt = BBT(BBT.CB_QPUT, [ 'ch1/res1', 'ch1/res2' ], 'token_XXXX`)
bbt.start()

for i in range(10):
    ret = bbt.recv_data()
    if ret is None:
        break

    (data, topic, ts) = ret
    print('%s [%s] %s' % (BBT.ts2datestr(ts), topic, data))

bbt.end()
```

## References

* [BeeBotte](https://beebotte.com/)

* [paho-mqtt: Python Client - documentation](https://www.eclipse.org/paho/clients/python/docs/)
