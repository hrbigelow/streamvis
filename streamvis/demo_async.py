import time
from streamvis.logger import AsyncDataLogger
from .demo_funcs import Cloud, Sinusoidal

async def demo_log_data_async(grpc_uri, scope, num_steps):
    """Demo of the AsyncDataLogger."""
    logger = AsyncDataLogger(
        scope=scope, 
        grpc_uri=grpc_uri, 
        tensor_type="numpy", 
        delete_existing=True, 
        flush_every=1.0,
    )

    cloud = Cloud(num_points=10000, num_steps=num_steps)
    sinusoidal = Sinusoidal()

    async with logger:
        logger.write_config({ "start-time": time.time() })

        for step in range(0, num_steps, 10):
            time.sleep(0.1)
            # top_data[group, point], where group is a logical grouping of points that
            # form a line, and point is one of those points
            xs, top_data = sinusoidal.step(step)
            await logger.write('sinusoidal', x=xs, y=top_data)

            points = cloud.step(step)
            xs, ys = points[:,0], points[:,1]
            await logger.write('cloud', x=xs, y=ys, t=step)

            if step % 10 == 0:
                print(f'Logged {step=}')

