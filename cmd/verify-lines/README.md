## Verify your Line Protocol

verify-lines.go is provided to allow calling the LP decoder from Python to check line protocol for errors.

To do so, first build a shared object line for your system from the root of this project.

```bash
go build -buildmode=c-shared -o verify-lines.so ./cmd/verify-lines/verify-lines.go
```

Then within your Python script or program, run these:

```python
import ctypes
so = ctypes.cdll.LoadLibrary('./verify-lines.so')
verifyLines = so.verifyLines
```

Then you can check single line protocol lines or batches of them. Encoding to utf8 is required.

```python
>>> verifyLines('foo,tag1=val1,tag2=val2 x=1,y="hello" 1625823259000000'.encode('utf-8'))
0
>>> verifyLines('foo,,,, 1625823259000000'.encode('utf-8'))
at line 1:5: expected tag key or field but found ',' instead
1
>>> batch="""##comment
... "foo",tag1=val1,"tag2"="tag
... " x2=1,y="hel
... lo" 1625823259000000
... _bar enabled=true
... foo,bar=a\\ x=1 x=1"""
>>> verifyLines(batch.encode('utf-8'))
at line 2:28: expected tag key or field but found '\n' instead
at line 6:13: empty tag key
1
```

The error messages explain where the first error in each line occurred. There may be more than one error in a line.
The line count is 1 indexed.