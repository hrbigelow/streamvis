import sys
import time
import jax
import jax.numpy as jnp
from streamvis.logger import DataLogger

def main(grpc_uri: str):
    scope = "__large_write__"
    tensor_type = "jax"
    logger = DataLogger(scope, grpc_uri, tensor_type, True, 3.0)
    logger.start()

    key = jax.random.key(42)
    N = 10_000

    for t in range(100):
        xs = jnp.arange(N)
        ys = jax.random.normal(key, (10, N))
        logger.write("test-name", x=xs, y=ys, t=t)
        time.sleep(0.1)
        print(f"Logged {N*10} elements.")

    logger.stop()

if __name__ == "__main__":
    grpc_uri = sys.argv[1]
    main(grpc_uri)

