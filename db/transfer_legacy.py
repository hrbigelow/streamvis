import os
import json
import asyncpg
import asyncio
from streamvis import script

async def amain():
    grpc_uri = os.getenv("STREAMVIS_GRPC_URI")
    if grpc_uri is None:
        raise RuntimeError("Must define STREAMVIS_GRPC_URI")

    pg_uri = os.getenv("STREAMVIS_PG_URI")
    if pg_uri is None:
        raise RuntimeError("Must define STREAMVIS_PG_URI")
    
    try:
        conn = await asyncpg.connect(pg_uri)
        await conn.set_type_codec(
                'jsonb',
                encoder=json.dumps,
                decoder=json.loads,
                schema="pg_catalog",
        )

    except asyncpg.PostgresError as ex:
        raise RuntimeError(f"Couldn't open Database connection: {ex}") from ex

    # Create the list of all scopes
    tags =  






if __name__ == "__main__":
    main()
