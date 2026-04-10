"""Tests for PID validation and normalisation."""

import pytest

from app.schemas.pid import detect_pid_type, normalise_pid
from app.services.pid_validator import validate_and_normalise


class TestDetectPidType:
    def test_doi_plain(self):
        assert detect_pid_type("10.1234/example-dataset") == "doi"

    def test_doi_with_prefix(self):
        assert detect_pid_type("doi:10.1234/example-dataset") == "doi"

    def test_doi_with_https_url(self):
        assert detect_pid_type("https://doi.org/10.1234/example-dataset") == "doi"

    def test_doi_with_http_url(self):
        assert detect_pid_type("http://doi.org/10.1234/example-dataset") == "doi"

    def test_doi_url_encoded_slash(self):
        assert detect_pid_type("10.1234%2Fexample-dataset") == "doi"

    def test_urn_nbn(self):
        assert detect_pid_type("urn:nbn:fi:csc-kata20140602161238071481") == "urn_nbn"

    def test_urn_nbn_uppercase(self):
        assert detect_pid_type("URN:NBN:NL:UI:17-XXXX") == "urn_nbn"

    def test_ark(self):
        assert detect_pid_type("ark:/12345/fk4example") == "ark"

    def test_handle(self):
        assert detect_pid_type("20.500.11918/12345") == "handle"

    def test_handle_not_doi(self):
        # Must not match DOI pattern
        result = detect_pid_type("20.500.11918/abc")
        assert result == "handle"

    def test_invalid_returns_none(self):
        assert detect_pid_type("not-a-pid") is None

    def test_empty_returns_none(self):
        assert detect_pid_type("") is None

    def test_just_random_url(self):
        assert detect_pid_type("https://example.com/data") is None


class TestNormalisePid:
    def test_strips_https_doi_prefix(self):
        assert normalise_pid("https://doi.org/10.1234/x") == "10.1234/x"

    def test_strips_doi_colon_prefix(self):
        assert normalise_pid("doi:10.1234/x") == "10.1234/x"

    def test_strips_whitespace(self):
        assert normalise_pid("  10.1234/x  ") == "10.1234/x"

    def test_url_decodes(self):
        assert normalise_pid("10.1234%2Fx") == "10.1234/x"

    def test_passthrough_handle(self):
        assert normalise_pid("20.500.11918/abc") == "20.500.11918/abc"


class TestValidateAndNormalise:
    def test_valid_doi(self):
        pid_type, value = validate_and_normalise("10.1234/my-dataset")
        assert pid_type == "doi"
        assert value == "10.1234/my-dataset"

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Unsupported PID"):
            validate_and_normalise("ftp://bad-pid")

    def test_doi_url_normalised(self):
        pid_type, value = validate_and_normalise("https://doi.org/10.1234/my-dataset")
        assert pid_type == "doi"
        assert value == "10.1234/my-dataset"
