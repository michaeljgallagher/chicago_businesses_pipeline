from datetime import datetime

import polars as pl
import pytest
from chicago_businesses_pipeline.utils.processing import (
    join_licenses_owners,
    transform_licenses,
    transform_owners,
)


@pytest.fixture
def owners_df():
    return pl.DataFrame(
        {
            "account_number": [1, 2],
            "legal_name": ["ABC", "123"],
            "owner_first_name": ["John", "Jane"],
            "owner_middle_initial": ["A", "B"],
            "owner_last_name": ["Doe", "Smith"],
            "suffix": [None, None],
            "legal_entity_owner": [None, None],
            "title": ["PRESIDENT", "MEMBER"],
        }
    )


@pytest.fixture
def licenses_df():
    return pl.DataFrame(
        {
            "id": ["123-456", "234-567"],
            "license_id": [11111, 2222222],
            "account_number": [1, 2],
            "site_number": ["1", "1"],
            "legal_name": ["ABC", "123"],
            "doing_business_as_name": ["ABC", "123"],
            "address": ["123 FAKE ST", "742 EVERGREEN TERRACE"],
            "city": ["CHICAGO", "JOLIET"],
            "state": ["IL", "IL"],
            "zip_code": ["60601", "60403"],
            "ward": [None, None],
            "precinct": [None, None],
            "ward_precinct": [None, None],
            "police_district": [None, None],
            "license_code": ["1010", "1010"],
            "license_description": [
                "Limited Business License",
                "Limited Business License",
            ],
            "business_activity_id": [None, None],
            "business_activity": [None, None],
            "license_number": ["999", "888"],
            "application_type": ["RENEW", "ISSUE"],
            "application_created_date": [
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 5, 1, 0, 0),
            ],
            "application_requirements_complete": [
                datetime(2024, 1, 2, 0, 0),
                datetime(2024, 5, 2, 0, 0),
            ],
            "payment_date": [datetime(2024, 1, 2, 0, 0), datetime(2024, 5, 2, 0, 0)],
            "conditional_approval": ["N", "N"],
            "license_start_date": [
                datetime(2024, 1, 2, 0, 0),
                datetime(2024, 5, 2, 0, 0),
            ],
            "expiration_date": [datetime(2024, 1, 2, 0, 0), datetime(2024, 5, 2, 0, 0)],
            "license_approved_for_issuance": [
                datetime(2024, 1, 2, 0, 0),
                datetime(2024, 5, 2, 0, 0),
            ],
            "date_issued": [datetime(2024, 1, 2, 0, 0), datetime(2024, 5, 2, 0, 0)],
            "license_status": ["AAI", "AAI"],
            "license_status_change_date": [
                datetime(2024, 1, 2, 0, 0),
                datetime(2024, 5, 2, 0, 0),
            ],
            "ssa": [None, None],
            "latitude": [42.346247, -71.097773],
            "longitude": [53.430833, -2.960833],
            "location": ["(42.346247, -71.097773)", "(53.430833, -2.960833)"],
        }
    )


def test_transform_owners(owners_df):
    transformed_df = transform_owners(owners_df)
    assert transformed_df["owner_first_name"].to_list() == ["JOHN", "JANE"]
    assert transformed_df["owner_middle_initial"].to_list() == ["A", "B"]
    assert transformed_df["owner_last_name"].to_list() == ["DOE", "SMITH"]
    assert transformed_df["owner_full_name"].to_list() == ["DOE,JOHN A", "SMITH,JANE B"]


def test_transform_licenses(licenses_df):
    transformed_df = transform_licenses(licenses_df)
    assert "location" not in transformed_df.columns
    assert transformed_df["application_created_date"].dtype == pl.Date


def test_join_licenses_owners(owners_df, licenses_df):
    joined_df = join_licenses_owners(licenses_df, owners_df)
    assert joined_df.shape[0] == 2
    assert "legal_name_right" not in joined_df.columns
