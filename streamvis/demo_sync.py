import time
from .logger import DataLogger
from .demo_funcs import Cloud, Sinusoidal

def demo_log_data(grpc_uri, scope, num_steps):
    """Demo of the Synchronous DataLogger."""
    logger = DataLogger(
        scope=scope, 
        grpc_uri=grpc_uri,
        tensor_type="numpy",
        delete_existing=True,
    )

    cloud = Cloud(num_points=10000, num_steps=num_steps)
    sinusoidal = Sinusoidal()

    logger.init_scope()
    logger.write_config({ "start-time": time.time() })

    for step in range(0, num_steps, 10):
        time.sleep(0.1)

        xs, top_data = sinusoidal.step(step)
        logger.write('sinusoidal', x=xs, y=top_data)

        points = cloud.step(step)
        xs, ys = points[:,0], points[:,1]
        logger.write('cloud', x=xs, y=ys, t=step)

        if step % 10 == 0:
            print(f'Logged {step=}')

        if step % 100 == 0:
            logger.flush_buffer()

    # final flush
    logger.flush_buffer()

