"""
Reference implemenations of encoding / decoding functions
"""

import numpy as np
from functools import reduce
from dataclasses import dataclass

@dataclass
class AffineArray:
    size: int
    phase: int
    slope: int
    isinv: bool

    @property
    def array(self) -> np.ndarray:
        vals = np.arange(self.phase, self.phase + self.size)
        if self.isinv:
            return vals // self.slope
        return vals * self.slope

@dataclass
class ModAffineArray:
    base: np.ndarray|None
    n: int
    p: int
    s: int
    m: int
    d: int
    c: int

    @property
    def array(self) -> np.ndarray:
        if self.base is not None:
            return self.base
        vals = np.arange(self.p, self.p + self.n) // self.s
        if self.m is not None:
            vals = vals % self.m
        return vals * self.d + self.c

@dataclass
class DiffArray:
    base: int
    size: int
    diff: np.ndarray 

    def __post_init__(self):
        self.diff = np.array(self.diff)

    @classmethod
    def from_array(cls, ary: np.ndarray) -> 'DiffArray':
        base = int(ary[0])
        size = ary.size
        d = np.diff(ary)

        # find smallest repeat
        L = len(d)
        if L == 0:
            m = 0
        else:
            pi = np.zeros(L, dtype=np.int32)
            k = 0
            for i in range(1, L):
                while k > 0 and d[i] != d[k]:
                    k = pi[k-1]
                if d[i] == d[k]:
                    k += 1
                pi[i] = k
            m = L - int(pi[-1])

        return cls(base, size, d[:m])

    @property
    def array(self) -> np.ndarray:
        buf = np.empty(self.size, dtype=np.int32)
        buf[0] = self.base
        for i in range(1, self.size):
            buf[i] = buf[i-1] + self.diff[(i-1) % self.diff.size]
        return buf


def combine_affine(*arrays: list[np.ndarray]) -> np.ndarray:
    return reduce(np.add.outer, arrays).ravel()

