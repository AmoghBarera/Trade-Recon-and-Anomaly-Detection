import os
import pytest
from datetime import datetime, date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.reconcile import Base, ReconciliationResult, EODSignOff
from app.pdf_generator import PDFGenerator

# Create an in-memory SQLite database for testing
engine = create_engine('sqlite:///:memory:')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

@pytest.fixture
def mock_pdf_generator(tmpdir):
    # Overwrite the reports dir to use pytest tmpdir
    pg = PDFGenerator(db_url='sqlite:///:memory:')
    pg.reports_dir = str(tmpdir)
    Base.metadata.create_all(pg.engine)
    
    # Populate some test data
    session = pg._get_session()
    
    today = datetime.utcnow().date()
    # Today's trades
    records = [
        ReconciliationResult(
            trade_id="PDF1001",
            ticker="AAPL",
            status="MATCHED",
            reconciliation_timestamp=datetime.combine(today, datetime.min.time()),
            highest_severity=None
        ),
        ReconciliationResult(
            trade_id="PDF1002",
            ticker="GOOG",
            status="MISMATCHED",
            reconciliation_timestamp=datetime.utcnow(),
            highest_severity="CRITICAL",
            mismatch_details=[{"field": "price", "reason": "Price mismatch", "severity": "CRITICAL"}]
        ),
        ReconciliationResult(
            trade_id="PDF1003",
            ticker="TSLA",
            status="MISMATCHED",
            reconciliation_timestamp=datetime.utcnow(),
            highest_severity="WARNING"
        )
    ]
    session.add_all(records)
    
    # Add a sign-off
    signoff = EODSignOff(
        reviewer_name="Jane Doe",
        report_date=today.strftime('%Y-%m-%d'),
        signoff_timestamp=datetime.utcnow()
    )
    session.add(signoff)
    
    session.commit()
    session.close()
    
    return pg, today

def test_generate_eod_report(mock_pdf_generator):
    pg, today = mock_pdf_generator
    
    # Generate the report
    filepath = pg.generate_eod_report(target_date=today)
    
    # Verify the file was created
    assert os.path.exists(filepath)
    
    # Verify it has some content (size > 0)
    assert os.path.getsize(filepath) > 1000  # A basic PDF should be larger than 1KB
    
    # Verify the filename format
    expected_filename = f"{today.strftime('%Y-%m-%d')}_reconciliation_report.pdf"
    assert filepath.endswith(expected_filename)

def test_generate_eod_report_no_data(mock_pdf_generator):
    pg, _ = mock_pdf_generator
    
    # Generate report for a future date with no data
    future_date = date(2099, 1, 1)
    filepath = pg.generate_eod_report(target_date=future_date)
    
    # It should still generate successfully
    assert os.path.exists(filepath)
    assert os.path.getsize(filepath) > 0
