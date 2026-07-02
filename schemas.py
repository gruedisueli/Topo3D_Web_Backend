from pydantic import BaseModel, Field
from typing import List
from enum import Enum

class ObjectType(str, Enum):
    SPHERE = "sphere"
    BOX = "box"
    CYLINDER = "cylinder"

class Force(BaseModel):
    model_config = {"extra":"forbid"}
    index: int = Field(..., ge=0)
    vector: List[float] = Field(..., max_items=3)

class OptimizationParams(BaseModel):
    model_config = {"extra":"forbid"}
    nelx: int = Field(...)
    nely: int = Field(...)
    nelz: int = Field(...)
    volfrac: float = Field(..., ge=0.0, le=1.0)
    penal: float = Field(..., ge=1.0, le=5.0)
    rmin: float = Field(..., ge=0.5, le=10.0)
    tolx: float = Field(..., ge=0.001, le=0.999)
    maxloop: int = Field(..., ge=1, le=2000)
    obstacles: List[int] = Field(..., max_items=262144)#64x64x64 max number of voxels due to computation constraints on our resources (could be greater with better GPU)
    supports: List[int] = Field(..., max_items=262144)
    forces: List[Force] = Field(..., max_items=262144)