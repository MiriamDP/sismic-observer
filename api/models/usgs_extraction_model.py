from dataclasses import dataclass
from datetime import datetime


@dataclass
class USGSExtraction:

    data: dict
    original_body: str
    request_url: str
    requested_at: datetime
    completed_at: datetime
    http_status: int

    @property
    def features(self):
        return self.data["features"]