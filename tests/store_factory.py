"""Builds the store a test runs against.

Memory by default. With MSG_TEST_STORE=azure every test gets a fresh,
uniquely named set of containers and tables in Azurite, so the whole service
suite can be replayed against real Blob and Table semantics:

    docker run -d --name scanb-azurite -p 127.0.0.1:10000-10002:10000-10002 \
        mcr.microsoft.com/azure-storage/azurite azurite --blobHost 0.0.0.0 --tableHost 0.0.0.0 --queueHost 0.0.0.0 --loose --skipApiVersionCheck
    MSG_TEST_STORE=azure .venv/Scripts/python -m pytest -q
"""
from __future__ import annotations

import os
import uuid

# Azurite's published development account. Not a secret: it is the same fixed
# key in every copy of the emulator, and it only ever opens 127.0.0.1.
AZURITE = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
)


def store_kind() -> str:
    return os.getenv("MSG_TEST_STORE", "memory").lower()


def make_store(kind: str | None = None, segment_size: int = 5):
    kind = kind or store_kind()
    if kind == "memory":
        from messaging.storage.memory import MemoryStore
        return MemoryStore(segment_size=segment_size)
    if kind == "azure":
        from azure.data.tables.aio import TableServiceClient
        from azure.storage.blob.aio import BlobServiceClient
        from messaging.storage.azure import AzureStore
        conn = os.getenv("MSG_TEST_AZURITE_CONNECTION", AZURITE)
        return AzureStore(blob_service=BlobServiceClient.from_connection_string(conn),
                          table_service=TableServiceClient.from_connection_string(conn),
                          segment_size=segment_size, namespace="t" + uuid.uuid4().hex[:10])
    raise ValueError(kind)
