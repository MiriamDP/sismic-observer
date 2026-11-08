'''Modulo para enriquecer geograficamente los datos de usgs'''
import json
import logging

from pathlib import Path
from shapely.geometry import shape, Point
from shapely import STRtree, make_valid

logger = logging.getLogger(__name__)


class Geography:
    '''Clase para la identificación de países según sus coordenadas'''

    def __init__(self, geojson_path):
        self.geojson_path = Path(geojson_path)
        self.geometries = []
        self.country_codes = []
        self.countries = []
        self.tree = None

    def load(self):
        '''Para leer y cargar los datos de GeoJSON'''

        if not self.geojson_path.exists():
            raise FileNotFoundError(f"No existe el catálogo: {self.geojson_path}")

        with self.geojson_path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        features = data.get("features", [])

        seen_codes = set()

        for feature in features:

            properties = feature.get("properties", {})
            adm0_a3 = properties.get("ADM0_A3")

            if not adm0_a3:
                logger.warning("Entidad sin ADM0_A3. Se omite.")
                continue

            adm0_a3 = str(adm0_a3).strip()

            if adm0_a3 in seen_codes:
                raise ValueError(f"ADM0_A3 duplicado en Natural Earth: {adm0_a3}")

            seen_codes.add(adm0_a3)

            geometry_data = feature.get("geometry")

            if not geometry_data:
                logger.warning("País %s sin geometría", adm0_a3)
                continue

            geometry = shape(geometry_data)

            if geometry.geom_type not in ("Polygon", "MultiPolygon"):
                logger.warning("Geometría no poligonal: %s",adm0_a3)
                continue

            if not geometry.is_valid:
                geometry = make_valid(geometry)

            self.geometries.append(geometry)
            self.country_codes.append(adm0_a3)

            self.countries.append({
                "adm0_a3": adm0_a3,
                "name_es": properties.get("NAME_ES"),
                "admin": properties.get("ADMIN"),
                "continent": properties.get("CONTINENT"),
                "subregion": properties.get("SUBREGION")
            })

        if not self.geometries:
            raise ValueError("No se encontraron geometrías válidas")

        self.tree = STRtree(self.geometries)

        logger.info("Catálogo geográfico cargado: %s países", len(self.geometries))

        return self.countries

    def find_country(self, longitude, latitude):
        '''Identifica el país segun sus coordenadas'''

        if self.tree is None:
            raise RuntimeError("El catálogo geográfico no está cargado")

        point = Point(longitude, latitude)

        # Primera búsqueda: punto estrictamente interior.
        matches = self.tree.query(point, predicate="within")

        # Segunda búsqueda: incluye puntos en fronteras.
        if len(matches) == 0:

            matches = self.tree.query(point, predicate="covered_by")

        if len(matches) == 0:
            return None

        # Resolución determinista de varias coincidencias.
        codes = sorted(self.country_codes[int(index)] for index in matches)

        return codes[0]
