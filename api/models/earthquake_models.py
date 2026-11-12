
from datetime import datetime
from datetime import date

from pydantic import BaseModel, Field, ConfigDict

class EarthquakeFilters(BaseModel):
    """Filtros disponibles para consultar terremotos."""

    mode: str = Field(default="live", pattern="^(live|replay|all)$")
    start_date: datetime | None = None
    end_date: datetime | None = None
    min_magnitude: float | None = None
    max_magnitude: float | None = None
    country_code: str | None = None


class Pagination(BaseModel):
    """Información de paginación de los resultados."""

    page: int
    page_size: int
    total_items: int
    total_pages: int
    has_next: bool
    has_previous: bool


class EarthquakeResponse(BaseModel):
    """Representación de un terremoto."""

    model_config = ConfigDict(extra="ignore")

    event_id: str
    mode: str
    source: str | None = None
    magnitude: float | None = None
    magnitude_type: str | None = None
    event_type: str | None = None
    place: str | None = None
    longitude: float | None = None
    latitude: float | None = None
    depth_km: float | None = None
    event_time: datetime
    updated: datetime | None = None
    country_code: str | None = None
    country_name: str
    continent: str | None = None
    subregion: str | None = None


class EarthquakeListResponse(BaseModel):
    """Respuesta paginada de terremotos."""

    items: list[EarthquakeResponse]
    pagination: Pagination
    filters: EarthquakeFilters


class EarthquakeAnalysisFilters(BaseModel):
    mode: str = Field(default="all", pattern="^(live|replay|all)$")

    start_date: date
    end_date: date

    min_magnitude: float | None = None
    max_magnitude: float | None = None

    min_depth: float | None = None
    max_depth: float | None = None

    country_code: str | None = None


class EarthquakeSummary(BaseModel):
    total_earthquakes: int

    average_magnitude: float | None
    average_depth: float | None

    average_per_day: float | None
    average_per_hour: float | None

    max_earthquake: EarthquakeResponse | None


class CountryEarthquakeStats(BaseModel):
    country_code: str | None
    country_name: str

    total_earthquakes: int
    average_magnitude: float | None

    max_earthquake: EarthquakeResponse | None


class TimeDistributionItem(BaseModel):
    date: datetime
    count: int


class TimeDistribution(BaseModel):
    granularity: str
    data: list[TimeDistributionItem]


class MagnitudeDepthPoint(BaseModel):
    magnitude: float
    depth: float


class EarthquakeMetricsResponse(BaseModel):

    filters: EarthquakeAnalysisFilters

    summary: EarthquakeSummary

    top_countries: list[CountryEarthquakeStats]

    time_distribution: TimeDistribution

    magnitude_depth: list[MagnitudeDepthPoint]