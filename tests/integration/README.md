# Integration tests

These run the storage contract against **Azurite**, Microsoft's local emulator
for Azure Blob and Table storage. No cloud account, no credentials, nothing
leaves the machine: Azurite listens on 127.0.0.1 only.

```bash
docker run -d --name scanb-azurite -p 127.0.0.1:10000-10002:10000-10002 \
    mcr.microsoft.com/azure-storage/azurite \
    azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0 --loose --skipApiVersionCheck

docker start scanb-azurite        # afterwards
```

Then either:

```bash
MSG_TEST_AZURITE=1 .venv/Scripts/python -m pytest tests/integration -q   # the contract, both stores
MSG_TEST_STORE=azure .venv/Scripts/python -m pytest -q                   # the WHOLE suite on Azurite
```

The second is the stronger check and the one to run before shipping a storage
change: every service test replays against real Blob and Table behaviour.
It has already caught what the in-memory store cannot show, such as a write
reaching a tenant's tables before they were created.

Each test run uses its own uniquely named containers and tables
(`tests/store_factory.py`), so runs never collide and nothing needs cleaning up
between them. To reclaim the space, remove the container: `docker rm -f scanb-azurite`.

The connection string in `store_factory.py` is Azurite's published development
account, identical in every copy of the emulator. It is not a secret.
