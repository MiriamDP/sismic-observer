
from datetime import datetime
from datetime import date

from pydantic import BaseModel, Field, ConfigDict,model_validator

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

class EarthquakeComparisonFilters(BaseModel):

    start_day: int = Field(ge=1, le=31)
    start_month: int = Field(ge=1, le=12)

    end_day: int = Field(ge=1, le=31)
    end_month: int = Field(ge=1, le=12)

    first_year: int = Field(ge=1900)
    second_year: int = Field(ge=1900)

    country_code: str | None = None

    min_magnitude: float | None = None
    max_magnitude: float | None = None

    min_depth: float | None = None
    max_depth: float | None = None

    @model_validator(mode="after")
    def validate_filters(self):

        if self.first_year == self.second_year:
            raise ValueError("Los años deben ser diferentes")

        start = self.start_month * 100 + self.start_day
        end = self.end_month * 100 + self.end_day

        if start > end:
            raise ValueError(
                "La fecha inicial debe ser anterior a la final"
            )

        if (
            self.min_magnitude is not None
            and self.max_magnitude is not None
            and self.min_magnitude > self.max_magnitude
        ):
            raise ValueError("Rango de magnitud no válido")

        if (
            self.min_depth is not None
            and self.max_depth is not None
            and self.min_depth > self.max_depth
        ):
            raise ValueError("Rango de profundidad no válido")

        return self


class EarthquakeMetricsData(BaseModel):

    summary: EarthquakeSummary

    top_countries: list[CountryEarthquakeStats]

    time_distribution: TimeDistribution

    magnitude_depth: list[MagnitudeDepthPoint]
    
class EarthquakeComparisonMetricsResponse(BaseModel):

    first_year: int

    second_year: int

    first_period: EarthquakeMetricsData

    second_period: EarthquakeMetricsData



class EarthquakeMetricsResponse(EarthquakeMetricsData):

    filters: EarthquakeAnalysisFilters