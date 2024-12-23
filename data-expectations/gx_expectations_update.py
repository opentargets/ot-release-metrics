"""Add or update expectations for a specific asset context."""

import great_expectations as gx
from utils import add_expectations_for_table

# TODO: Make abstract
prioritisation_validation = add_expectations_for_table(
    asset_name="targetPrioritisation",
    expectations=[
        gx.expectations.ExpectColumnValuesToNotBeNull(column="targetId"),
        gx.expectations.ExpectColumnDistinctValuesToBeInSet(
            column="isSecreted",
            value_set=[0, 1]
        )
    ]
)