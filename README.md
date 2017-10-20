# aiozbx
Asynchronous API to zabbix.

## zabbix sender example
```python
from aiozbx import Sender
import time

sender = Sender("localhost", 10051)
data = [{"host": "testhost",
         "key": "key[param1, param2]",
         "value": "OK",
         "clock": int(time.time()),
         "ns": 0}]

res = await sender.send(data)
print(res)
```
