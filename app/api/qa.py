from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.dependencies import get_qa_repository
from app.repositories.cartographic_qa import CartographicQARepository
from app.schemas import QAIncidentsResponse


router = APIRouter(prefix="/qa", tags=["qa"])


@router.get("/incidents", response_model=QAIncidentsResponse)
async def list_qa_incidents(
    layer: str | None = Query(None, description="Filter by layer name"),
    severity: str | None = Query(None, description="Filter by severity (error, warning, info)"),
    resolved: bool = Query(False, description="Include resolved incidents"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    qa_repo: CartographicQARepository = Depends(get_qa_repository),
) -> QAIncidentsResponse:
    result = await qa_repo.list_incidents(
        layer=layer,
        severity=severity,
        resolved=resolved,
        page=page,
        page_size=page_size,
    )
    return QAIncidentsResponse(**result)
