"""Tests for configuration loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from rds_schema_exporter.config import Config, load_config


# ---------------------------------------------------------------------------
# Default config
# ---------------------------------------------------------------------------


def test_load_default_config():
    """load_config(None) returns a Config with all defaults."""
    config = load_config(None)

    assert isinstance(config, Config)
    assert config.connection.host == "localhost"
    assert config.connection.port == 3306
    assert config.connection.user == ""
    assert config.connection.password == ""
    assert config.output.target == "local"
    assert config.output.local_path == str(Path.home() / "SchemaDump")
    assert config.output.s3_bucket == ""
    assert config.output.s3_prefix == ""
    assert config.metadata.account_id == ""
    assert config.metadata.region == ""
    assert config.metadata.instance_id == ""
    assert config.filtering.exclude_databases == []
    assert config.notifications.slack_webhook_url == ""
    assert config.behavior.strip_auto_increment is True
    assert config.behavior.strip_definer is False


# ---------------------------------------------------------------------------
# Load from YAML
# ---------------------------------------------------------------------------


def test_load_config_from_yaml(tmp_path: Path):
    """Write a full YAML config, load it, verify every field."""
    yaml_content = """\
connection:
  host: mydb.example.com
  port: 3307
  user: admin
  password: secret123

output:
  target: s3
  local_path: /tmp/schemas
  s3_bucket: my-bucket
  s3_prefix: exports/

metadata:
  account_id: "999888777666"
  region: ap-southeast-1
  instance_id: staging-db

filtering:
  exclude_databases:
    - temp_db
    - scratch

notifications:
  slack_webhook_url: https://hooks.slack.com/services/XXX

behavior:
  strip_auto_increment: false
  strip_definer: true
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml_content)

    config = load_config(str(config_file))

    assert config.connection.host == "mydb.example.com"
    assert config.connection.port == 3307
    assert config.connection.user == "admin"
    assert config.connection.password == "secret123"
    assert config.output.target == "s3"
    assert config.output.local_path == "/tmp/schemas"
    assert config.output.s3_bucket == "my-bucket"
    assert config.output.s3_prefix == "exports/"
    assert config.metadata.account_id == "999888777666"
    assert config.metadata.region == "ap-southeast-1"
    assert config.metadata.instance_id == "staging-db"
    assert config.filtering.exclude_databases == ["temp_db", "scratch"]
    assert config.notifications.slack_webhook_url == "https://hooks.slack.com/services/XXX"
    assert config.behavior.strip_auto_increment is False
    assert config.behavior.strip_definer is True


# ---------------------------------------------------------------------------
# Missing file
# ---------------------------------------------------------------------------


def test_load_config_missing_file():
    """Loading a non-existent file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        load_config("/nonexistent/path/to/config.yaml")


# ---------------------------------------------------------------------------
# Partial YAML — unspecified fields keep defaults
# ---------------------------------------------------------------------------


def test_config_partial_yaml(tmp_path: Path):
    """A YAML file that only sets a few fields; the rest stay at defaults."""
    yaml_content = """\
connection:
  host: partial-host.example.com

behavior:
  strip_definer: true
"""
    config_file = tmp_path / "partial.yaml"
    config_file.write_text(yaml_content)

    config = load_config(str(config_file))

    # Specified fields
    assert config.connection.host == "partial-host.example.com"
    assert config.behavior.strip_definer is True

    # Defaults preserved
    assert config.connection.port == 3306
    assert config.connection.user == ""
    assert config.connection.password == ""
    assert config.output.target == "local"
    assert config.output.local_path == str(Path.home() / "SchemaDump")
    assert config.metadata.account_id == ""
    assert config.filtering.exclude_databases == []
    assert config.notifications.slack_webhook_url == ""
    assert config.behavior.strip_auto_increment is True


def test_config_empty_yaml(tmp_path: Path):
    """An empty YAML file returns all defaults."""
    config_file = tmp_path / "empty.yaml"
    config_file.write_text("")

    config = load_config(str(config_file))

    assert config.connection.host == "localhost"
    assert config.output.target == "local"
