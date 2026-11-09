
from datetime import date

from pydantic import BaseModel, Field


class ImportRequest(BaseModel):
    """Modelo para solicitar una importación histórica."""

    start_date: date
    end_date: date
    min_magnitude: float | None = Field(default=None,ge=-2,le=10)
