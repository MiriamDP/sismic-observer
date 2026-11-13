from pydantic import BaseModel, ConfigDict


class CountryResponse(BaseModel):
    """Representación de un país."""

    model_config = ConfigDict(from_attributes=True)

    adm0_a3: str
    name_es: str
    admin: str
    continent: str | None = None
    subregion: str | None = None