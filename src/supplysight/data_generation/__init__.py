"""Reproducible synthetic operational data for SupplySight."""

from supplysight.data_generation.config import PROFILES, GenerationConfig
from supplysight.data_generation.generate import generate_dataset

__all__ = ["GenerationConfig", "PROFILES", "generate_dataset"]
