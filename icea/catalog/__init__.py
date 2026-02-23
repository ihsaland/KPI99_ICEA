"""Instance and region catalogs per cloud provider. See catalog/README.md for format."""
from icea.catalog.loader import (
    get_providers,
    get_regions,
    get_instance_types,
    get_instance_by_id,
)

__all__ = ["get_providers", "get_regions", "get_instance_types", "get_instance_by_id"]
