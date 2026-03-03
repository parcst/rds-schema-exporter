"""Tests for the Teleport integration module."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import click
import pytest

from rds_schema_exporter.teleport import (
    ALL_SENTINEL,
    TeleportTunnel,
    _wait_for_tunnel_port,
    find_tsh,
    get_clusters,
    get_logged_in_user,
    interactive_select,
    list_mysql_databases,
    start_tunnel,
    stop_tunnel,
)


# ---------------------------------------------------------------------------
# find_tsh
# ---------------------------------------------------------------------------


class TestFindTsh:
    def test_override_exists(self, tmp_path):
        tsh = tmp_path / "tsh"
        tsh.touch()
        assert find_tsh(str(tsh)) == str(tsh)

    def test_override_missing(self):
        with pytest.raises(FileNotFoundError, match="Configured tsh path not found"):
            find_tsh("/nonexistent/tsh")

    @patch("rds_schema_exporter.teleport.shutil.which", return_value="/usr/local/bin/tsh")
    def test_which_found(self, mock_which):
        assert find_tsh() == "/usr/local/bin/tsh"

    @patch("rds_schema_exporter.teleport.shutil.which", return_value=None)
    @patch("rds_schema_exporter.teleport.Path.is_file", return_value=True)
    def test_teleport_connect_fallback(self, mock_is_file, mock_which):
        result = find_tsh()
        assert "Teleport Connect" in result

    @patch("rds_schema_exporter.teleport.shutil.which", return_value=None)
    @patch("rds_schema_exporter.teleport.Path.is_file", return_value=False)
    def test_not_found(self, mock_is_file, mock_which):
        with pytest.raises(FileNotFoundError, match="Could not find tsh"):
            find_tsh()


# ---------------------------------------------------------------------------
# get_clusters
# ---------------------------------------------------------------------------


class TestGetClusters:
    def test_profiles_found(self, tmp_path):
        with patch("rds_schema_exporter.teleport._TSH_DIR", tmp_path):
            (tmp_path / "prod.teleport.sh.yaml").touch()
            (tmp_path / "nonprod.teleport.sh.yaml").touch()
            clusters = get_clusters("/usr/bin/tsh")
            assert sorted(clusters) == [
                "nonprod.teleport.sh",
                "prod.teleport.sh",
            ]

    def test_no_profiles(self, tmp_path):
        with patch("rds_schema_exporter.teleport._TSH_DIR", tmp_path):
            with pytest.raises(RuntimeError, match="No Teleport cluster profiles found"):
                get_clusters("/usr/bin/tsh")


# ---------------------------------------------------------------------------
# get_logged_in_user
# ---------------------------------------------------------------------------


class TestGetLoggedInUser:
    @patch("rds_schema_exporter.teleport.subprocess.run")
    def test_returns_username(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout=json.dumps({"active": {"username": "alice@example.com"}}),
        )
        assert get_logged_in_user("/usr/bin/tsh") == "alice@example.com"

    @patch("rds_schema_exporter.teleport.subprocess.run")
    def test_top_level_username(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout=json.dumps({"username": "bob@example.com"}),
        )
        assert get_logged_in_user("/usr/bin/tsh") == "bob@example.com"

    @patch("rds_schema_exporter.teleport.subprocess.run")
    def test_no_username(self, mock_run):
        mock_run.return_value = MagicMock(stdout=json.dumps({}))
        with pytest.raises(RuntimeError, match="Could not determine Teleport username"):
            get_logged_in_user("/usr/bin/tsh")


# ---------------------------------------------------------------------------
# list_mysql_databases
# ---------------------------------------------------------------------------

_SAMPLE_DB_LS = [
    {
        "metadata": {"name": "prod-mysql"},
        "spec": {
            "protocol": "mysql",
            "uri": "prod-mysql.xxx.us-east-1.rds.amazonaws.com:3306",
            "aws": {
                "account_id": "111111111111",
                "region": "us-east-1",
                "rds": {"instance_id": "prod-mysql"},
            },
        },
    },
    {
        "metadata": {"name": "prod-postgres"},
        "spec": {
            "protocol": "postgres",
            "uri": "prod-postgres.xxx.us-east-1.rds.amazonaws.com:5432",
            "aws": {
                "account_id": "111111111111",
                "region": "us-east-1",
                "rds": {"instance_id": "prod-postgres"},
            },
        },
    },
    {
        "metadata": {"name": "staging-mysql"},
        "spec": {
            "protocol": "mysql",
            "uri": "staging-mysql.xxx.us-west-2.rds.amazonaws.com:3306",
            "aws": {
                "account_id": "222222222222",
                "region": "us-west-2",
                "rds": {"instance_id": "staging-mysql"},
            },
        },
    },
]


class TestListMysqlDatabases:
    @patch("rds_schema_exporter.teleport.subprocess.run")
    def test_filters_mysql_only(self, mock_run):
        mock_run.return_value = MagicMock(stdout=json.dumps(_SAMPLE_DB_LS))
        result = list_mysql_databases("/usr/bin/tsh", "prod.teleport.sh")

        assert len(result) == 2
        assert result[0]["name"] == "prod-mysql"
        assert result[0]["account_id"] == "111111111111"
        assert result[0]["region"] == "us-east-1"
        assert result[0]["instance_id"] == "prod-mysql"
        assert result[1]["name"] == "staging-mysql"

    @patch("rds_schema_exporter.teleport.subprocess.run")
    def test_no_mysql_databases(self, mock_run):
        postgres_only = [
            {
                "metadata": {"name": "pg"},
                "spec": {"protocol": "postgres", "uri": "pg:5432", "aws": {}},
            }
        ]
        mock_run.return_value = MagicMock(stdout=json.dumps(postgres_only))
        with pytest.raises(RuntimeError, match="No MySQL databases found"):
            list_mysql_databases("/usr/bin/tsh", "prod.teleport.sh")


# ---------------------------------------------------------------------------
# interactive_select
# ---------------------------------------------------------------------------


class TestInteractiveSelect:
    def test_single_item_auto_selects(self):
        result = interactive_select(["only-one"], "Pick")
        assert result == "only-one"

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="No items to select from"):
            interactive_select([], "Pick")

    @patch("rds_schema_exporter.teleport.click.prompt", return_value=2)
    def test_multi_item_selection(self, mock_prompt):
        result = interactive_select(["a", "b", "c"], "Pick one")
        assert result == "b"

    @patch("rds_schema_exporter.teleport.click.prompt", return_value=1)
    def test_allow_all_returns_sentinel(self, mock_prompt):
        result = interactive_select(["a", "b", "c"], "Pick", allow_all=True)
        assert result == ALL_SENTINEL

    @patch("rds_schema_exporter.teleport.click.prompt", return_value=3)
    def test_allow_all_picks_item(self, mock_prompt):
        result = interactive_select(["a", "b", "c"], "Pick", allow_all=True)
        assert result == "b"

    def test_single_item_ignores_all(self):
        result = interactive_select(["only-one"], "Pick", allow_all=True)
        assert result == "only-one"


# ---------------------------------------------------------------------------
# start_tunnel / _wait_for_tunnel_port
# ---------------------------------------------------------------------------


class TestStartTunnel:
    @patch("rds_schema_exporter.teleport.subprocess.Popen")
    @patch("rds_schema_exporter.teleport.subprocess.run")
    def test_start_tunnel(self, mock_run, mock_popen):
        # Simulate tunnel output
        mock_proc = MagicMock()
        mock_proc.stdout.readline.side_effect = [
            "Listening on 127.0.0.1:54321\n",
        ]
        mock_popen.return_value = mock_proc

        tunnel = start_tunnel("/usr/bin/tsh", "my-db", "alice@example.com")

        assert tunnel.host == "127.0.0.1"
        assert tunnel.port == 54321
        assert tunnel.db_name == "my-db"
        assert tunnel.db_user == "alice@example.com"
        assert tunnel.process is mock_proc

        # Verify db login was called
        mock_run.assert_called_once_with(
            ["/usr/bin/tsh", "db", "login", "my-db", "--db-user=alice@example.com"],
            check=True,
        )

    def test_wait_for_port_timeout(self):
        mock_proc = MagicMock()
        mock_proc.stdout.readline.return_value = ""
        mock_proc.poll.return_value = 0  # process exited

        with pytest.raises(RuntimeError, match="Timed out waiting for tsh tunnel port"):
            _wait_for_tunnel_port(mock_proc)


# ---------------------------------------------------------------------------
# stop_tunnel
# ---------------------------------------------------------------------------


class TestStopTunnel:
    @patch("rds_schema_exporter.teleport.subprocess.run")
    def test_stop_tunnel(self, mock_run):
        mock_proc = MagicMock()
        tunnel = TeleportTunnel(
            process=mock_proc,
            host="127.0.0.1",
            port=54321,
            db_name="my-db",
            db_user="alice@example.com",
        )

        stop_tunnel("/usr/bin/tsh", tunnel)

        mock_proc.terminate.assert_called_once()
        mock_proc.wait.assert_called_once_with(timeout=5)
        mock_run.assert_called_once_with(
            ["/usr/bin/tsh", "db", "logout", "my-db"],
            capture_output=True,
            timeout=10,
        )

    @patch("rds_schema_exporter.teleport.subprocess.run")
    def test_stop_tunnel_force_kill_on_timeout(self, mock_run):
        mock_proc = MagicMock()
        mock_proc.wait.side_effect = subprocess.TimeoutExpired(cmd="tsh", timeout=5)
        tunnel = TeleportTunnel(
            process=mock_proc,
            host="127.0.0.1",
            port=54321,
            db_name="my-db",
            db_user="alice@example.com",
        )

        stop_tunnel("/usr/bin/tsh", tunnel)

        mock_proc.terminate.assert_called_once()
        mock_proc.kill.assert_called_once()
