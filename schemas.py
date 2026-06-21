from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from uuid import UUID
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
    nelx: int = Field(..., ge=5, le=256)
    nely: int = Field(..., ge=5, le=256)
    nelz: int = Field(..., ge=5, le=256)
    volfrac: float = Field(..., ge=0.0, le=1.0)
    penal: float = Field(..., ge=1.0, le=5.0)
    rmin: float = Field(..., ge=0.5, le=10.0)
    tolx: float = Field(..., ge=0.001, le=0.999)
    maxloop: int = Field(..., ge=1, le=2000)
    # pitch: Optional[float] = Field(1.0, ge=0.01, le=1.0)
    # invert_design_space: Optional[bool] = Field(False)
    # design_space_stl_id: Optional[UUID] = None
    obstacles: List[int] = Field(..., max_items=262144)#64x64x64 max number of voxels due to computation constraints on our resources (could be greater with better GPU)
    supports: List[int] = Field(..., max_items=262144)
    forces: List[Force] = Field(..., max_items=262144)