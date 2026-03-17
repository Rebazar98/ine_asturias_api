from __future__ import annotations

import asyncio
import io
from functools import partial
from typing import Any

import httpx
import openpyxl

from app.core.logging import get_logger
from app.settings import Settings


DATASET_CATALOG: dict[str, dict[str, str]] = {
    "padron_municipal": {
        "description": "Padrón municipal de habitantes por municipio (Asturias)",
        "url_path": "/estadisticas/temas/poblacion/padron-municipal/padron_municipal_municipios.xlsx",
    },
    "pib_municipal": {
        "description": "PIB a precios corrientes por municipio (Asturias)",
        "url_path": "/estadisticas/temas/economia/contabilidad-municipal/pib_municipal.xlsx",
    },
}


class SADEIClientError(Exception):
    def __init__(self, dataset_id: str, detail: str) -> None:
        super().__init__(detail)
        self.dataset_id = dataset_id
        self.detail = detail


class SADEIClientService:
    """
    Adapter para SADEI. Descarga ficheros Excel por URL conocida.
    No escribe en base de datos. Devuelve list[dict].
    """

    def __init__(self, http_client: httpx.AsyncClient, settings: Settings) -> None:
        self.http_client = http_client
        self.settings = settings
        self.logger = get_logger("app.services.sadei_client")

    async def fetch_dataset(self, dataset_id: str) -> list[dict[str, Any]]:
        entry = DATASET_CATALOG.get(dataset_id)
        if entry is None:
            raise SADEIClientError(dataset_id, f"Unknown SADEI dataset: {dataset_id!r}")

        url = self.settings.sadei_base_url.rstrip("/") + entry["url_path"]
        self.logger.info("sadei_fetch_start", extra={"dataset_id": dataset_id, "url": url})

        try:
            response = await self.http_client.get(
                url, timeout=self.settings.http_timeout_seconds, follow_redirects=True
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise SADEIClientError(
                dataset_id,
                f"HTTP {exc.response.status_code} fetching SADEI dataset {dataset_id!r}",
            ) from exc
        except httpx.RequestError as exc:
            raise SADEIClientError(
                dataset_id,
                f"Network error fetching SADEI dataset {dataset_id!r}: {exc}",
            ) from exc

        content = response.content
        loop = asyncio.get_running_loop()
        rows = await loop.run_in_executor(None, partial(_parse_xlsx, content, dataset_id))
        self.logger.info(
            "sadei_fetch_completed",
            extra={"dataset_id": dataset_id, "rows": len(rows)},
        )
        return rows

    async def list_available_datasets(self) -> list[dict[str, Any]]:
        return [
            {"dataset_id": k, "description": v["description"]} for k, v in DATASET_CATALOG.items()
        ]


def _parse_xlsx(content: bytes, dataset_id: str) -> list[dict[str, Any]]:
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    ws = wb.active
    if ws is None:
        return []

    rows_iter = ws.iter_rows(values_only=True)
    try:
        header_row = next(rows_iter)
    except StopIteration:
        return []

    headers = [
        str(cell).strip() if cell is not None else f"col_{i}" for i, cell in enumerate(header_row)
    ]

    result: list[dict[str, Any]] = []
    for row in rows_iter:
        if all(cell is None for cell in row):
            continue
        record: dict[str, Any] = {"_dataset_id": dataset_id}
        for key, cell in zip(headers, row):
            record[key] = cell
        result.append(record)

    wb.close()
    return result
