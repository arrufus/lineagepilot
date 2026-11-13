"""DbtParserConfig and the PII and layer patterns.

Ported from data-capsule ``backend/tests/parsers/test_dbt_parser.py``.
"""

from __future__ import annotations

from support import DBT_FIXTURES

from lineagepilot_core.parsers import DbtParserConfig, LayerPattern, PIIPattern
from lineagepilot_core.parsers.dbt_config import DEFAULT_PII_PATTERNS

MANIFEST_PATH = DBT_FIXTURES / "pilot-1.12" / "manifest.json"
CATALOG_PATH = DBT_FIXTURES / "pilot-1.12" / "catalog.json"


class TestDbtParserConfig:
    def test_from_dict_minimal(self):
        config = DbtParserConfig.from_dict({"manifest_path": str(MANIFEST_PATH)})
        assert config.manifest_path == MANIFEST_PATH
        assert config.catalog_path is None
        assert config.pii_detection_enabled is True

    def test_from_dict_with_catalog_and_options(self):
        config = DbtParserConfig.from_dict(
            {
                "manifest_path": str(MANIFEST_PATH),
                "catalog_path": str(CATALOG_PATH),
                "project_name": "test_project",
                "pii_detection_enabled": False,
            }
        )
        assert config.catalog_path == CATALOG_PATH
        assert config.project_name == "test_project"
        assert config.pii_detection_enabled is False

    def test_defaults_are_copies(self):
        config = DbtParserConfig(manifest_path=MANIFEST_PATH)
        config.pii_patterns.clear()
        config.layer_tags.clear()
        fresh = DbtParserConfig(manifest_path=MANIFEST_PATH)
        assert fresh.pii_patterns == DEFAULT_PII_PATTERNS
        assert fresh.layer_tags["staging"] == "silver"

    def test_validate_ok(self):
        config = DbtParserConfig(manifest_path=MANIFEST_PATH, catalog_path=CATALOG_PATH)
        assert config.validate() == []

    def test_validate_missing_manifest(self, tmp_path):
        errors = DbtParserConfig(manifest_path=tmp_path / "nonexistent.json").validate()
        assert len(errors) == 1
        assert "Manifest file not found" in errors[0]

    def test_validate_wrong_extension(self, tmp_path):
        bad = tmp_path / "manifest.yaml"
        bad.touch()
        errors = DbtParserConfig(manifest_path=bad).validate()
        assert errors == [f"Manifest file must be JSON: {bad}"]

    def test_validate_catalog(self, tmp_path):
        bad = tmp_path / "catalog.yaml"
        bad.touch()
        missing = DbtParserConfig(manifest_path=MANIFEST_PATH, catalog_path=tmp_path / "x.json")
        wrong = DbtParserConfig(manifest_path=MANIFEST_PATH, catalog_path=bad)
        assert "Catalog file not found" in missing.validate()[0]
        assert "Catalog file must be JSON" in wrong.validate()[0]


class TestPIIPattern:
    def test_email(self):
        pattern = PIIPattern(
            pii_type="email", patterns=[r"(?:^|_)e?mail(?:_|$)", r"(?:^|_)email_address(?:_|$)"]
        )
        assert pattern.matches("email")
        assert pattern.matches("user_email")
        assert pattern.matches("email_address")
        assert pattern.matches("mail")
        assert not pattern.matches("emailing")
        assert not pattern.matches("mailing_list")

    def test_phone(self):
        pattern = PIIPattern(
            pii_type="phone", patterns=[r"(?:^|_)phone(?:_|$)", r"(?:^|_)mobile(?:_|$)"]
        )
        assert pattern.matches("phone")
        assert pattern.matches("phone_number")
        assert pattern.matches("mobile")
        assert pattern.matches("user_phone")
        assert not pattern.matches("telephone_company")

    def test_ssn(self):
        pattern = PIIPattern(
            pii_type="ssn", patterns=[r"(?:^|_)ssn(?:_|$)", r"(?:^|_)social_security(?:_|$)"]
        )
        assert pattern.matches("ssn")
        assert pattern.matches("user_ssn")
        assert pattern.matches("social_security")
        assert pattern.matches("social_security_number")

    def test_case_insensitive(self):
        pattern = PIIPattern(pii_type="email", patterns=[r"(?:^|_)email(?:_|$)"])
        assert pattern.matches("EMAIL")
        assert pattern.matches("Email")
        assert pattern.matches("USER_EMAIL")


class TestLayerPattern:
    def test_bronze_names(self):
        pattern = LayerPattern(
            layer="bronze",
            name_patterns=[r"^raw_", r"^source_", r"_raw$"],
            path_patterns=[r"/raw/", r"/sources/"],
        )
        assert pattern.matches_name("raw_customers")
        assert pattern.matches_name("source_orders")
        assert pattern.matches_name("customers_raw")
        assert not pattern.matches_name("stg_customers")

    def test_silver_names(self):
        pattern = LayerPattern(
            layer="silver",
            name_patterns=[r"^stg_", r"^int_", r"^staging_"],
            path_patterns=[r"/staging/", r"/intermediate/"],
        )
        assert pattern.matches_name("stg_customers")
        assert pattern.matches_name("int_customer_orders")
        assert pattern.matches_name("staging_orders")
        assert not pattern.matches_name("dim_customers")

    def test_gold_names(self):
        pattern = LayerPattern(
            layer="gold",
            name_patterns=[r"^dim_", r"^fct_", r"^mart_"],
            path_patterns=[r"/marts/", r"/gold/"],
        )
        assert pattern.matches_name("dim_customers")
        assert pattern.matches_name("fct_orders")
        assert pattern.matches_name("mart_sales")
        assert not pattern.matches_name("stg_customers")

    def test_paths(self):
        pattern = LayerPattern(
            layer="gold", name_patterns=[], path_patterns=[r"/marts/", r"/gold/"]
        )
        assert pattern.matches_path("models/marts/core/dim_customers.sql")
        assert pattern.matches_path("/gold/analytics/report.sql")
        assert not pattern.matches_path("models/staging/stg_customers.sql")


class TestPIIPatternChanges:
    """Task 1.6: ip_address is no longer claimed by address; geography is location."""

    def detect(self, name: str) -> str | None:
        return next((p.pii_type for p in DEFAULT_PII_PATTERNS if p.matches(name)), None)

    def test_ip_address(self):
        assert self.detect("ip_address") == "ip_address"
        assert self.detect("client_ip") == "ip_address"

    def test_geography_is_location(self):
        for name in ("city", "billing_state", "country", "zip_code", "postal"):
            assert self.detect(name) == "location", name

    def test_addresses_stay_addresses(self):
        assert self.detect("street") == "address"
        assert self.detect("shipping_address") == "address"
