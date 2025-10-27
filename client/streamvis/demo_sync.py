import time
from .logger import DataLogger
from .demo_funcs import Cloud, Sinusoidal

def demo_log_data(grpc_uri, scope, delete_existing_names, num_steps):
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

        xs, top_data = sinusoidal.step(step)
        logger.write('sinusoidal', x=xs, y=top_data)

        points = cloud.step(step)
        xs, ys = points[:,0], points[:,1]
        logger.write('cloud', x=xs, y=ys, t=step)

        if step % 10 == 0:
            print(f'Logged {step=}')

    # blocks until all remaining writes are flushed  
    logger.stop()

