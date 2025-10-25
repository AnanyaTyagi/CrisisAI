from pydantic import BaseModel, Field
from typing import Optional, Literal, Dict, Any

HazardType = Literal["fire", "earthquake", "flood", "weather_alert", "unknown"]

from typing import Optional, Dict, Any, Literal
from pydantic import BaseModel
from datetime import datetime

from typing import Optional, Dict, Any, Literal
from pydantic import BaseModel
from datetime import datetime

class RawEvent(BaseModel):
    id: str
    source: Literal['usgs', 'nws', 'firms', 'camera']  # MUST include 'camera'
    received_at: datetime
    when: Optional[datetime] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    hazard: str  # keep as str so 'fire' is allowed
    payload: Optional[Dict[str, Any]] = None

class ConfirmedIncident(BaseModel):
    id: str
    lat: float
    lon: float
    when: datetime
    hazard: str  # keep as str so 'fire' is allowed
    confidence: float
    fused_from: Dict[str, Any]

class IncidentCandidate(BaseModel):
    key: str               # spatial-temporal key to dedupe/fuse (e.g., cell+minute)
    lat: float
    lon: float
    when: datetime
    hazard: HazardType
    score: float = 0.0
    sources: Dict[str, Any] = Field(default_factory=dict)  # {source: eventId}
