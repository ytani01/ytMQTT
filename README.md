# Mqtt


## Install

## Usage

Beebotte Publisher
```python3
from Mqtt import BeebottePublisher

bbt = BeebottePublisher('token_XXXX')
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
from Mqtt import BeebotteSubscriber

def cb_recv_data(data, topic, ts):
    print('%s [%s] %s' % (BeebotteSubscriber.ts2datestr(ts), topic, data))

bbt = BeebotteSubscriber(cb_recv_data, [ 'ch1/res1', 'ch2/res2' ], 'token_XX')
bbt.start()

for i in range(10):
    time.sleep(1)

bbt.end()
```

Beebotte Subscriber (sync)
```python3
from Mqtt import BeebotteSubscriber

bbt = BeebotteSubscriber(BeebotteSubscriber.CB_QPUT,
                       [ 'ch1/res1', 'ch1/res2' ], 'token_XXXX`)
bbt.start()

for i in range(10):
    ret = bbt.recv_data()
    if ret is None:
        break
        
    (data, topic, ts) = ret
    print('%s [%s] %s' % (bbt.ts2datestr(ts), topic, data))
    
bbt.end()
```

## References

* [BeeBotte](https://beebotte.com/)
