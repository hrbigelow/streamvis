from abc import abstractmethod
import time
from .logger import DataLogger
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

    def step(self, step_num: int, num_points: int):
        beg, end = step_num, step_num + num_points 
        xs = np.arange(beg, end, dtype=np.int32)[None,:]
        # [index, point]
        top_data = np.array(
            [
                [math.sin(1 + s / 10) for s in range(beg, end)],
                [0.5 * math.sin(1.5 + s / 20) for s in range(beg, end)],
                [1.5 * math.sin(2 + s / 15) for s in range(beg, end)]
            ], dtype=np.float32) 
        return xs, top_data



def log_data(grpc_uri, scope, delete_existing_names, num_steps):
    """Demo of the Synchronous DataLogger."""
    logger = DataLogger(
        scope=scope, 
        grpc_uri=grpc_uri,
        tensor_type="numpy",
        delete_existing_names=delete_existing_names,
        flush_every=2.0,
    )

    cloud = Cloud(num_points=10000, num_steps=num_steps)
    sinusoidal = Sinusoidal()

    # Call start before any logging
    logger.start()
    logger.write_config({ "start-time": time.time() })

    for step in range(0, num_steps):

        xs, top_data = sinusoidal.step(step, num_points=1)
        logger.write('sinusoidal', x=xs, y=top_data)

        # points = cloud.step(step)
        # xs, ys = points[:,0], points[:,1]
        # logger.write('cloud', x=xs, y=ys, t=step)

        if step % 10 == 0:
            print(f'Logged {step=}')
    print(f'Logged {step=}')

    # blocks until all remaining writes are flushed  
    logger.stop()


async def log_data_async(grpc_uri, scope, delete_existing_names, num_steps):
    """Demo of the AsyncDataLogger."""
    logger = AsyncDataLogger(
        scope=scope, 
        grpc_uri=grpc_uri, 
        tensor_type="numpy", 
        delete_existing_names=delete_existing_names, 
        flush_every=1.0,
    )

    cloud = Cloud(num_points=10000, num_steps=num_steps)
    sinusoidal = Sinusoidal()

    async with logger:
        logger.write_config({ "start-time": time.time() })

        for step in range(0, num_steps):
            # top_data[group, point], where group is a logical grouping of points that
            # form a line, and point is one of those points
            xs, top_data = sinusoidal.step(step)
            await logger.write('sinusoidal', x=xs, y=top_data)

            points = cloud.step(step)
            xs, ys = points[:,0], points[:,1]
            await logger.write('cloud', x=xs, y=ys, t=step)

            if step % 10 == 0:
                print(f'Logged {step=}')

