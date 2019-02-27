# scalaps
Scala-inspired data structures for Python

Example:
```python
from scalaps import ScSeq

(ScSeq(range(10))
 .map(lambda x: x+3)
 .filter(lambda x: x%2==0)
 .group_by(lambda x: x%3)
 .items()
 .for_each(print))
```

Output:
```
(1, ScList([4, 10]))
(0, ScList([6, 12]))
(2, ScList([8]))
```

Very much a work in progress. Expect major changes as the library evolves.

I'd very match appreciate other people's input, so feel free to submit a PR.

Contact: Matt Hagy <matthew.hagy@gmail.com>