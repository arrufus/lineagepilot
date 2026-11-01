"""Configuration for dbt parser including PII patterns and layer inference."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
import re


@dataclass
class PIIPattern:
    """Pattern for detecting PII in column names."""

    pii_type: str
    patterns: list[str]
    _compiled: list[re.Pattern[str]] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        """Compile regex patterns."""
        self._compiled = [re.compile(p, re.IGNORECASE) for p in self.patterns]

    def matches(self, column_name: str) -> bool:
        """Check if column name matches any pattern."""
        return any(p.search(column_name) for p in self._compiled)


@dataclass
class LayerPattern:
    """Pattern for inferring architecture layer."""

    layer: str
    name_patterns: list[str]
    path_patterns: list[str]
    _name_compiled: list[re.Pattern[str]] = field(default_factory=list, repr=False)
    _path_compiled: list[re.Pattern[str]] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        """Compile regex patterns."""
        self._name_compiled = [re.compile(p, re.IGNORECASE) for p in self.name_patterns]
        self._path_compiled = [re.compile(p, re.IGNORECASE) for p in self.path_patterns]

    def matches_name(self, name: str) -> bool:
        """Check if model name matches layer pattern."""
        return any(p.search(name) for p in self._name_compiled)

    def matches_path(self, path: str) -> bool:
        """Check if file path matches layer pattern."""
        return any(p.search(path) for p in self._path_compiled)


# Default PII detection patterns
DEFAULT_PII_PATTERNS: list[PIIPattern] = [
    PIIPattern(
        pii_type="email",
        patterns=[
            r"(?:^|_)e?mail(?:_|$)",
            r"(?:^|_)email_address(?:_|$)",
            r"(?:^|_)user_email(?:_|$)",
            r"(?:^|_)customer_email(?:_|$)",
        ],
    ),
    PIIPattern(
        pii_type="phone",
        patterns=[
            r"(?:^|_)phone(?:_|$)",
            r"(?:^|_)mobile(?:_|$)",
            r"(?:^|_)cell(?:_|$)",
            r"(?:^|_)telephone(?:_|$)",
            r"(?:^|_)tel(?:_|$)",
            r"(?:^|_)phone_number(?:_|$)",
        ],
    ),
    PIIPattern(
        pii_type="ssn",
        patterns=[
            r"(?:^|_)ssn(?:_|$)",
            r"(?:^|_)social_security(?:_|$)",
            r"(?:^|_)social_security_number(?:_|$)",
        ],
    ),
    PIIPattern(
        pii_type="address",
        patterns=[
            r"(?:^|_)address(?:_|$)",
            r"(?:^|_)street(?:_|$)",
            r"(?:^|_)city(?:_|$)",
            r"(?:^|_)zip(?:_|$)",
            r"(?:^|_)zip_code(?:_|$)",
            r"(?:^|_)postal(?:_|$)",
            r"(?:^|_)postal_code(?:_|$)",
            r"(?:^|_)state(?:_|$)",
            r"(?:^|_)country(?:_|$)",
        ],
    ),
    PIIPattern(
        pii_type="name",
        patterns=[
            r"(?:^|_)first_name(?:_|$)",
            r"(?:^|_)last_name(?:_|$)",
            r"(?:^|_)full_name(?:_|$)",
            r"(?:^|_)customer_name(?:_|$)",
            r"(?:^|_)user_name(?:_|$)",
            r"(?:^|_)person_name(?:_|$)",
            r"(?:^|_)given_name(?:_|$)",
            r"(?:^|_)family_name(?:_|$)",
            r"(?:^|_)surname(?:_|$)",
        ],
    ),
    PIIPattern(
        pii_type="date_of_birth",
        patterns=[
            r"(?:^|_)dob(?:_|$)",
            r"(?:^|_)birth_?date(?:_|$)",
            r"(?:^|_)date_of_birth(?:_|$)",
            r"(?:^|_)birthday(?:_|$)",
        ],
    ),
    PIIPattern(
        pii_type="credit_card",
        patterns=[
            r"(?:^|_)credit_card(?:_|$)",
            r"(?:^|_)card_number(?:_|$)",
            r"(?:^|_)cc_num(?:_|$)",
            r"(?:^|_)card_num(?:_|$)",
            r"(?:^|_)pan(?:_|$)",  # Primary Account Number
        ],
    ),
    PIIPattern(
        pii_type="bank_account",
        patterns=[
            r"(?:^|_)bank_account(?:_|$)",
            r"(?:^|_)account_number(?:_|$)",
            r"(?:^|_)routing_number(?:_|$)",
            r"(?:^|_)iban(?:_|$)",
            r"(?:^|_)swift(?:_|$)",
        ],
    ),
    PIIPattern(
        pii_type="ip_address",
        patterns=[
            r"(?:^|_)ip_addr(?:ess)?(?:_|$)",
            r"(?:^|_)client_ip(?:_|$)",
            r"(?:^|_)user_ip(?:_|$)",
            r"(?:^|_)source_ip(?:_|$)",
        ],
    ),
    PIIPattern(
        pii_type="device_id",
        patterns=[
            r"(?:^|_)device_id(?:_|$)",
            r"(?:^|_)device_identifier(?:_|$)",
            r"(?:^|_)udid(?:_|$)",
            r"(?:^|_)idfa(?:_|$)",
            r"(?:^|_)gaid(?:_|$)",
        ],
    ),
]

# Default layer inference patterns
DEFAULT_LAYER_PATTERNS: list[LayerPattern] = [
    LayerPattern(
        layer="bronze",
        name_patterns=[
            r"^raw_",
            r"^source_",
            r"^src_",
            r"_raw$",
        ],
        path_patterns=[
            r"/raw/",
            r"/sources/",
            r"/bronze/",
            r"/landing/",
        ],
    ),
    LayerPattern(
        layer="silver",
        name_patterns=[
            r"^stg_",
            r"^staging_",
            r"^int_",
            r"^intermediate_",
            r"^base_",
            r"^prep_",
        ],
        path_patterns=[
            r"/staging/",
            r"/intermediate/",
            r"/silver/",
            r"/transform/",
            r"/prep/",
        ],
    ),
    LayerPattern(
        layer="gold",
        name_patterns=[
            r"^dim_",
            r"^fct_",
            r"^fact_",
            r"^mart_",
            r"^rpt_",
            r"^report_",
            r"^agg_",
            r"^metric_",
        ],
        path_patterns=[
            r"/marts/",
            r"/gold/",
            r"/presentation/",
            r"/reports/",
            r"/analytics/",
            r"/core/",
        ],
    ),
]

# Tags that indicate layer
LAYER_TAGS: dict[str, str] = {
    "raw": "bronze",
    "bronze": "bronze",
    "landing": "bronze",
    "staging": "silver",
    "silver": "silver",
    "intermediate": "silver",
    "transform": "silver",
    "mart": "gold",
    "marts": "gold",
    "gold": "gold",
    "presentation": "gold",
    "analytics": "gold",
}


@dataclass
class DbtParserConfig:
    """Configuration for the dbt parser."""

    # Required paths
    manifest_path: Path

    # Optional paths
    catalog_path: Optional[Path] = None

    # Project identification
    project_name: Optional[str] = None

    # PII detection
    pii_patterns: list[PIIPattern] = field(default_factory=lambda: DEFAULT_PII_PATTERNS)
    pii_detection_enabled: bool = True

    # Layer inference
    layer_patterns: list[LayerPattern] = field(default_factory=lambda: DEFAULT_LAYER_PATTERNS)
    layer_tags: dict[str, str] = field(default_factory=lambda: LAYER_TAGS.copy())

    # Processing options
    include_sources: bool = True
    include_seeds: bool = True
    include_snapshots: bool = True
    include_tests: bool = False  # Usually not needed for architecture analysis

    # URN configuration
    urn_prefix: str = "urn:dab:dbt"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DbtParserConfig":
        """Create config from dictionary."""
        # Convert path strings to Path objects
        manifest_path = Path(data["manifest_path"])
        catalog_path = Path(data["catalog_path"]) if data.get("catalog_path") else None

        return cls(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
            project_name=data.get("project_name"),
            pii_detection_enabled=data.get("pii_detection_enabled", True),
            include_sources=data.get("include_sources", True),
            include_seeds=data.get("include_seeds", True),
            include_snapshots=data.get("include_snapshots", True),
            include_tests=data.get("include_tests", False),
        )

    def validate(self) -> list[str]:
        """Validate the configuration."""
        errors: list[str] = []

        if not self.manifest_path.exists():
            errors.append(f"Manifest file not found: {self.manifest_path}")
        elif not self.manifest_path.suffix == ".json":
            errors.append(f"Manifest file must be JSON: {self.manifest_path}")

        if self.catalog_path:
            if not self.catalog_path.exists():
                errors.append(f"Catalog file not found: {self.catalog_path}")
            elif not self.catalog_path.suffix == ".json":
                errors.append(f"Catalog file must be JSON: {self.catalog_path}")

        return errors
