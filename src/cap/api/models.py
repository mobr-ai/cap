from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional, Any

class QueryType(str, Enum):
    SELECT = "SELECT"
    ASK = "ASK"
    CONSTRUCT = "CONSTRUCT"
    DESCRIBE = "DESCRIBE"

class QueryRequest(BaseModel):
    query: str
    type: QueryType = QueryType.SELECT

class QueryResponse(BaseModel):
    results: dict

class GraphCreateRequest(BaseModel):
    graph_uri: str
    turtle_data: str = Field(..., description="RDF data in Turtle format")

class GraphUpdateRequest(BaseModel):
    insert_data: Optional[str] = Field(None, description="Triples to insert in Turtle format")
    delete_data: Optional[str] = Field(None, description="Triples to delete in Turtle format")
    prefixes: Optional[dict[str, str]] = Field(None, description="Prefix mappings")

class GraphResponse(BaseModel):
    data: dict[str, Any]

class SuccessResponse(BaseModel):
    success: bool