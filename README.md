# scalaps
See blog post, 
[Introducing scalaps: Scala-inspired data structures for Python](https://medium.com/@matthagy/introducing-scalaps-scala-inspired-data-structures-for-python-53f3afc8696)
to learn about using this library.
> A functional, object-oriented approach for working with sequences and collections. Also similar to Java Streams. Hope you find they simplify your code by providing a plethora of common algorithms for working with sequences and collections.

## Example
```python
from scalaps import ScSeq

(ScSeq(range(10))
 .map(lambda x: x+3)
 .filter(lambda x: x%2==0)
 .group_by(lambda x: x%3)
 .items()
 .for_each(print))
```

#### Output
```
(1, ScList([4, 10]))
(0, ScList([6, 12]))
(2, ScList([8]))
```

## Examples
See examples/ directory for additional examples of using scalaps. 

Also see example usages in
[career_village_entities](https://github.com/matthagy/career_village_entities).

## Closing Remarks
Very much a work in progress. Expect major changes as the library evolves.
I'd appreciate other people's input, so feel free to submit a PR.

Contact: Matt Hagy <matthew.hagy@gmail.com>