import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.reconcile import Base, ReconciliationResult
from app.report_generator import ReportGenerator

# Create an in-memory SQLite database for testing
engine = create_engine('sqlite:///:memory:')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

@pytest.fixture
def mock_report_generator():
    rg = ReportGenerator(db_url='sqlite:///:memory:')
    # Override the _get_session to use our test session
    # Actually, ReportGenerator recreates its own engine in init.
    # We can just let it use its in-memory db, but we need to populate it.
    
    # Let's populate some test data
    session = rg._get_session()
    
    records = [
        ReconciliationResult(
            trade_id="TRD1001",
            ticker="AAPL",
            status="MATCHED",
            reconciliation_timestamp=datetime(2023, 10, 25, 10, 30),
            asset_class="Equity",
            highest_severity=None
        ),
        ReconciliationResult(
            trade_id="TRD1002",
            ticker="GOOG",
            status="MISMATCHED",
            reconciliation_timestamp=datetime(2023, 10, 26, 11, 45),
            asset_class="Equity",
            highest_severity="CRITICAL"
        ),
        ReconciliationResult(
            trade_id="TRD1003",
            ticker="TSLA",
            status="MISMATCHED",
            reconciliation_timestamp=datetime(2023, 10, 26, 15, 20),
            asset_class="Equity",
            highest_severity="WARNING"
        ),
        ReconciliationResult(
            trade_id="FX1004",
            ticker="EURUSD",
            status="MATCHED",
            reconciliation_timestamp=datetime(2023, 10, 27, 9, 15),
            asset_class="FX",
            highest_severity=None
        )
    ]
    session.add_all(records)
    session.commit()
    session.close()
    
    return rg

def test_no_filters(mock_report_generator):
    data, count = mock_report_generator.get_filtered_audit_log()
    assert count == 4
    assert len(data) == 4
    # Most recent first
    assert data[0]['trade_id'] == "FX1004"

def test_date_range_filter(mock_report_generator):
    data, count = mock_report_generator.get_filtered_audit_log(start_date="2023-10-26", end_date="2023-10-26")
    assert count == 2
    ids = [d['trade_id'] for d in data]
    assert "TRD1002" in ids
    assert "TRD1003" in ids

def test_trade_id_filter(mock_report_generator):
    data, count = mock_report_generator.get_filtered_audit_log(trade_id="1002")
    assert count == 1
    assert data[0]['trade_id'] == "TRD1002"

def test_asset_class_filter(mock_report_generator):
    data, count = mock_report_generator.get_filtered_audit_log(asset_class="FX")
    assert count == 1
    assert data[0]['trade_id'] == "FX1004"

def test_severity_filter(mock_report_generator):
    data, count = mock_report_generator.get_filtered_audit_log(highest_severity="CRITICAL")
    assert count == 1
    assert data[0]['trade_id'] == "TRD1002"

def test_status_filter(mock_report_generator):
    data, count = mock_report_generator.get_filtered_audit_log(status="MATCHED")
    assert count == 2
    ids = [d['trade_id'] for d in data]
    assert "TRD1001" in ids
    assert "FX1004" in ids

def test_combined_filters(mock_report_generator):
    data, count = mock_report_generator.get_filtered_audit_log(
        start_date="2023-10-25", 
        end_date="2023-10-27", 
        asset_class="Equity",
        status="MISMATCHED",
        highest_severity="WARNING"
    )
    assert count == 1
    assert data[0]['trade_id'] == "TRD1003"
