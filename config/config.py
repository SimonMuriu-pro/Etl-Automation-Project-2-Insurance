import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "insurance_db"),
    "user": os.getenv("DB_USER", "your_username"),
    "password": os.getenv("DB_PASSWORD", "your_password"),
}

SCHEMA_NAME = os.getenv("DB_SCHEMA", "public")

CLEANING_CONFIG = {
    "drop_column_threshold": 0.5,
    "drop_row_threshold": 0.5,

    "tables": {
        "insurance_customers": {
            "columns": {
                "customerid": {"dtype": "string", "critical": True, "impute": "skip"},
                "name": {"dtype": "string", "impute": "mode"},
                "age": {"dtype": "int", "impute": "median"},
                "gender": {"dtype": "string", "impute": "mode"},
                "address": {"dtype": "string", "impute": "mode"},
                "contact": {"dtype": "string", "impute": "mode"},
                "occupation": {"dtype": "string", "impute": "mode"},
                "annualincome": {"dtype": "float", "impute": "median"},
                "tenure": {"dtype": "int", "impute": "median"},
            }
        },

        "insurance_claims": {
            "columns": {
                "claimid": {"dtype": "string", "critical": True, "impute": "skip"},
                "policyid": {"dtype": "string", "critical": True, "impute": "skip"},
                "claimdate": {"dtype": "datetime", "impute": "mode"},
                "claimamount": {"dtype": "float", "impute": "median"},
                "claimstatus": {"dtype": "string", "impute": "mode"},
                "reason": {"dtype": "string", "impute": "mode"},
            }
        },

        "insurance_feedback": {
            "columns": {
                "feedbackid": {"dtype": "string", "critical": True, "impute": "skip"},
                "customerid": {"dtype": "string", "critical": True, "impute": "skip"},
                "satisfaction": {"dtype": "int", "impute": "median"},
                "comments": {"dtype": "string", "impute": "mode"},
            }
        },

        "insurance_payments": {
            "columns": {
                "paymentid": {"dtype": "string", "critical": True, "impute": "skip"},
                "policyid": {"dtype": "string", "critical": True, "impute": "skip"},
                "paymentdate": {"dtype": "datetime", "impute": "mode"},
                "paymentamount": {"dtype": "float", "impute": "median"},
                "method": {"dtype": "string", "impute": "mode"},
            }
        },

        "insurance_policies": {
            "columns": {
                "policyid": {"dtype": "string", "critical": True, "impute": "skip"},
                "customerid": {"dtype": "string", "critical": True, "impute": "skip"},
                "policytype": {"dtype": "string", "impute": "mode"},
                "premiumamount": {"dtype": "float", "impute": "median"},
                "policystartdate": {"dtype": "datetime", "impute": "mode"},
                "policyenddate": {"dtype": "datetime", "impute": "mode"},
                "coverage": {"dtype": "string", "impute": "mode"},
                "amount": {"dtype": "float", "impute": "median"},
            }
        },
    }
}
