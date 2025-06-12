from abc import abstractmethod
import math
import numpy as np

class SynthData:
    """Generate some data for a particular time step."""

    @abstractmethod
    def step(step_num: int) -> np.ndarray:
        ...


class Cloud(SynthData):

    def __init__(self, num_points: int, num_steps):
        self.cloud = np.random.normal(size=(num_points, 2))
        self.speeds = np.random.uniform(size=(3,)) * (num_steps ** -1)

    def step(self, step_num): 
        xscale, yscale, rot_sin = np.sin(step_num * self.speeds)
        rot_cos = np.cos(step_num * self.speeds[2])
        scale = np.diag([xscale, yscale])
        rot = np.array([[rot_cos, -rot_sin], [rot_sin, rot_cos]])
        mat = np.einsum('ij, jk -> ik', scale, rot)
        return np.einsum('ik, li -> lk', mat, self.cloud)


class Sinusoidal(SynthData):

    def step(self, step_num: int):
        beg, end = step_num, step_num + 10
        xs = np.arange(beg, end, dtype=np.int32)[None,:]
        top_data = np.array(
            [
                [math.sin(1 + s / 10) for s in range(beg, end)],
                [0.5 * math.sin(1.5 + s / 20) for s in range(beg, end)],
                [1.5 * math.sin(2 + s / 15) for s in range(beg, end)]
            ], dtype=np.float32) 
        return xs, top_data


