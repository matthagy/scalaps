"""
scalaps - Scala-inspired data structures for Python

Also similar to Java streams

Currently a work in progress and improvements are much appreciated
Feel free to send a PR on GitHub
"""

# Copyright 2019 Matt Hagy <matthew.hagy@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the “Software”), to deal in
# the Software without restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
# Software, and to permit persons to whom the Software is furnished to do so, subject
# to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies
# or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.


import operator
import copy
from functools import reduce
from itertools import chain
from collections import defaultdict, Counter, deque
from typing import Callable, Any, Union, Iterable

# prefer importing the model as
# > import scalas as sc
__all__ = ['Seq', 'List', 'FrozenList', 'Dict']

CallableTypes = Union[Callable, str, int]
NoneType = type(None)


def identity(x):
    return x


def get_callable(obj: CallableTypes) -> Callable[[Any], Any]:
    if callable(obj):
        return obj
    elif isinstance(obj, str):
        return operator.attrgetter(obj)
    elif isinstance(obj, int):
        return operator.itemgetter(obj)
    else:
        raise TypeError(f"Can't call {obj!r}")


class IterableMixin:
    """Adds some useful Scala-inspired methods for working with iterables"""

    def __iter__(self):
        raise RuntimeError(f"__iter__ not overridden in {self.__class__.__name__}")

    def to_list(self) -> 'List':
        return List(self)

    def to_frozen_list(self) -> 'FrozenList':
        return FrozenList(self)

    def to_dict(self) -> 'Dict':
        return Dict(self)

    def map(self, func_like: CallableTypes) -> 'Seq':
        return Seq(map(get_callable(func_like), self))

    def flat_map(self, func_like: CallableTypes) -> 'Seq':
        func = get_callable(func_like)
        return Seq(y for x in self for y in func(x))

    def for_each(self, func_like: CallableTypes):
        func = get_callable(func_like)
        for x in self:
            func(x)

    def take(self, n) -> 'Seq':
        return Seq(x for _, x in zip(range(n), self))

    def drop(self, n) -> 'Seq':
        def gen():
            i = iter(self)
            try:
                for _ in range(n):
                    next(i)
            except StopIteration:
                pass
            yield from i

        return Seq(gen())

    def last(self, n) -> 'Seq':
        def gen():
            d = deque()
            for x in self:
                d.append(x)
                if len(d) > n:
                    d.popleft()
            yield from d

        return Seq(gen())

    def filter(self, func_like: CallableTypes) -> 'Seq':
        return Seq(filter(get_callable(func_like), self))

    def chain(self, other) -> 'Seq':
        return Seq(chain(self, other))

    def apply(self, func):
        return get_callable(func)(self)

    def apply_seq(self, func) -> 'Seq':
        return Seq(self.apply(func))

    def fold(self, init_value: Any, func_like: CallableTypes):
        return reduce(get_callable(func_like), self, init_value)

    def reduce(self, func_like: CallableTypes):
        return reduce(func_like, self)

    def sum(self):
        return sum(self)

    def count(self) -> int:
        return sum(1 for _ in self)

    def value_counts(self) -> 'Dict':
        return Dict(Counter(self))

    def sort_by(self, rank_func_like: CallableTypes) -> 'List':
        return List(sorted(self, key=get_callable(rank_func_like)))

    def sort(self) -> 'List':
        return self.sort_by(identity)

    def distinct(self) -> 'List':
        return List(set(self))

    def group_by(self, key_func_like: CallableTypes) -> 'Dict':
        key_func_like = get_callable(key_func_like)
        d = defaultdict(List)
        for el in self:
            d[key_func_like(el)].append(el)
        return Dict(d)

    def key_by(self, key_func_like: CallableTypes) -> 'Dict':
        key_func_like = get_callable(key_func_like)
        d = Dict()
        for el in self:
            k = key_func_like(el)
            if k in d:
                raise ValueError(f"duplicate key {k!r}")
            d[k] = el
        return d

    def aggregate_by(self,
                     key: CallableTypes,
                     create_aggregate: CallableTypes,
                     add_to_aggregate: CallableTypes) -> 'Dict':
        key = get_callable(key)
        create_aggregate = get_callable(create_aggregate)
        add_to_aggregate = get_callable(add_to_aggregate)

        aggs_by_key = {}
        for x in self:
            k = key(x)
            try:
                agg = aggs_by_key[k]
            except KeyError:
                agg = aggs_by_key[k] = create_aggregate(x)
            aggs_by_key[x] = add_to_aggregate(agg, x)

        return Dict(aggs_by_key)

    def fold_by(self,
                key: CallableTypes,
                agg0,
                add_to_aggregate: CallableTypes) -> 'Dict':
        return self.aggregate_by(key, lambda _: agg0, add_to_aggregate)

    def reduce_by(self, key: CallableTypes, reducer: CallableTypes) -> 'Dict':
        key = get_callable(key)
        reducer = get_callable(reducer)

        existings_by_key = {}
        for x in self:
            k = key(x)
            try:
                existing = existings_by_key[k]
            except KeyError:
                existings_by_key[k] = x
            else:
                existings_by_key[k] = reducer(existing, x)

        return Dict(existings_by_key)


class Seq(IterableMixin):
    """
    Wrapper around arbitrary sequences.`ScSeq` instances are single pass and can't be realized
    over multiple times. Use `ScList` for realized sequences that can be iterated over multiple.
    Alternatively, write a function that builds the sequence so that you can create multiple
    sequences and realize each one. See examples/seq_func.py for an example of this.
    times.
    """

    def __init__(self, inner_seq: Iterable):
        self._inner_seq = inner_seq
        self._ran = False

    def __iter__(self):
        if self._ran:
            raise RuntimeError("Re-running sequence")
        self._ran = True
        try:
            return iter(self._inner_seq)
        finally:
            # remove the reference so that we can free up sources in GCing
            # while realizing the rest of the sequence of operations
            del self._inner_seq

    def reverse(self):
        if self._ran:
            raise RuntimeError("Reversing a sequence that has already been ran")
        try:
            r = reversed(self._inner_seq)
        except TypeError:
            raise RuntimeError(f"Can't reverse {self._inner_seq!r}. "
                               "Instead you can realize the sequence as .to_list() "
                               "and then reverse that.")
        return Seq(r)
        # Note, we're specifically not marking the self ScSeq as _ran.
        # This is because all reversable iterables can be ran multiple times.
        # Hence we can run both the ScSeq instances independently.
        # Still respect the contract that individual ScSeq instances can't be
        # ran multiple times.


class BaseList:
    def __init__(self, l=()):
        self._list = l

    @property
    def length(self):
        return len(self._list)

    def reverse(self) -> Seq:
        return Seq(reversed(self._list))

    def __iter__(self):
        return iter(self._list)

    def __repr__(self):
        return f'{self.__class__.__name__}({self._list!r})'


class List(BaseList, IterableMixin):
    """
    Wrapper around Python lists with a more Scala-esque API

    TODO: Add more modification methods beyond append
    """

    def __init__(self, l=()):
        super().__init__(list(l) if not isinstance(l, list) else l)

    def to_list(self):
        return self

    def append(self, x):
        self._list.append(x)

    def copy(self):
        return List(list(self._l))

    def __copy__(self):
        return self.copy()

    def __deepcopy__(self, memo=None):
        return List(copy.deepcopy(x, memo=memo) for x in self._l)


class FrozenList(BaseList, IterableMixin):
    """
    Immutable wrapper around Python lists
    """

    def __init__(self, l):
        if not isinstance(l, tuple):
            l = tuple(l)
        super().__init__(l)

    def to_frozen_list(self):
        return self


class Dict(dict):
    """
    Extension of Python dict
    """

    @property
    def length(self):
        return len(self)

    def keys(self):
        return Seq(super().keys())

    def values(self):
        return Seq(super().values())

    def items(self):
        return Seq(super().items())

    def map_values(self, func_like: CallableTypes) -> 'Dict':
        func = get_callable(func_like)
        return Dict({k: func(v) for k, v in self.items()})

    def union(self, other: Union['Dict', dict], error_on_overlap=False):
        if error_on_overlap:
            common_keys = set(self.keys()) & set(other.keys())
            if common_keys:
                raise ValueError(f"there are {len(common_keys)} in common when non were expected")

        cp = Dict(self)
        cp.update(other)
        return cp

    def join(self, other: 'Dict', how='inner'):
        if how == 'inner':
            keys = set(self.keys()) & set(other.keys())
        elif how == 'outer':
            keys = set(self.keys()) | set(other.keys())
        elif how == 'left':
            keys = self.keys()
        elif how == 'right':
            keys = other.keys()
        else:
            raise ValueError(f"Invalid join {how!r}. Must be either inner, outer, left or right")

        def gen():
            for key in keys:
                yield key, (self.get(key), other.get(key))

        return Seq(gen())
