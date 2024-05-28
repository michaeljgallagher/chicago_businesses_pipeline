from polars.datatypes import Datetime, Float64, String

# Schemas should match as closely as possible to how they're listed in the API documentation.
# Most numerical fields are casted to `String` type.
# This is to ensure data integrity since we aren't doing any numerical calculations with the values.
# See `Fields` sections on each dataset's page.
# Owners documentation: https://data.cityofchicago.org/dataset/Business-Owners/ezma-pppn
# Licenses documentation: https://data.cityofchicago.org/dataset/Business-Licenses/r5kz-chrr

OWNERS_SCHEMA = {
    "account_number": String,
    "legal_name": String,
    "owner_first_name": String,
    "owner_middle_initial": String,
    "owner_last_name": String,
    "suffix": String,
    "legal_entity_owner": String,
    "title": String,
}

LICENSES_SCHEMA = {
    "id": String,
    "license_id": String,
    "account_number": String,
    "site_number": String,
    "legal_name": String,
    "doing_business_as_name": String,
    "address": String,
    "city": String,
    "state": String,
    "zip_code": String,
    "ward": String,
    "precinct": String,
    "ward_precinct": String,
    "police_district": String,
    "license_code": String,
    "license_description": String,
    "business_activity_id": String,
    "business_activity": String,
    "license_number": String,
    "application_type": String,
    "application_created_date": Datetime,
    "application_requirements_complete": Datetime,
    "payment_date": Datetime,
    "conditional_approval": String,
    "license_start_date": Datetime,
    "expiration_date": Datetime,
    "license_approved_for_issuance": Datetime,
    "date_issued": Datetime,
    "license_status": String,
    "license_status_change_date": Datetime,
    "ssa": String,
    "latitude": Float64,
    "longitude": Float64,
    "location": String,
}
