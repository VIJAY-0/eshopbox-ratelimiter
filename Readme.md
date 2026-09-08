# Rate limiter

A shared limiter that caps our channel API calls at N per second. Every thread calls `AllowRequest()` before making a call; it returns true if there is still room in this second, false otherwise.

A mutex lock protects the whole critical section, which is where we check and reset the second, then read, compare, and increment the count of calls in that second. Threads can race to that area, but only one can be inside it at a time, so at most N requests ever get through per second.

One extra thing worth knowing about the "N per second" rule: 

> it runs on fixed clock seconds, meaning the count resets when the second ticks over. 
> So in the worst case all N calls land in the last millisecond of one second and N more in the first millisecond of the next, and the API can see 2N calls in two milliseconds while no single second ever went over its limit.


## Running the tests

```bash
go test -race ./...        # correctness tests with the race detector
go test -v -run Load -count=1   # load simulation (uses -v so logs show, -count=1 to skip Go's cache)
```
