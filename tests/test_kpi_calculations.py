import pytest
from datetime import datetime, timedelta
from app.report_generator import ReportGenerator
from app.reconcile import ReconciliationResult

@pytest.fixture
def kpi_test_data():
    rg = ReportGenerator(db_url='sqlite:///:memory:')
    session = rg._get_session()
    
    now = datetime.utcnow()
    
    # "Today" trades (3 total: 2 matched+no_anomaly, 1 mismatched) => 66.66%
    today_trades = [
        ReconciliationResult(
            trade_id="KPI1", status="MATCHED", is_anomalous=False, highest_severity=None,
            reconciliation_timestamp=now
        ),
        ReconciliationResult(
            trade_id="KPI2", status="MATCHED", is_anomalous=None, highest_severity=None,
            reconciliation_timestamp=now
        ),
        ReconciliationResult(
            trade_id="KPI3", status="MISMATCHED", is_anomalous=False, highest_severity="WARNING",
            reconciliation_timestamp=now
        )
    ]
    
    # "Older than today but within 7 days" trades (2 total: 1 matched+anomaly, 1 matched+no_anomaly)
    # Total for 7 days = 3 (today) + 2 (older) = 5
    # Auto-matched for 7 days = 2 (today) + 1 (older, no anomaly) = 3 => 60.0%
    older_trades = [
        ReconciliationResult(
            trade_id="KPI4", status="MATCHED", is_anomalous=True, highest_severity=None,
            reconciliation_timestamp=now - timedelta(days=2)
        ),
        ReconciliationResult(
            trade_id="KPI5", status="MATCHED", is_anomalous=False, highest_severity=None,
            reconciliation_timestamp=now - timedelta(days=3)
        )
    ]
    
    # "Older than 7 days" trades (Should not affect 7d KPI)
    very_old_trades = [
        ReconciliationResult(
            trade_id="KPI6", status="MATCHED", is_anomalous=False, highest_severity=None,
            reconciliation_timestamp=now - timedelta(days=10)
        )
    ]
    
    session.add_all(today_trades + older_trades + very_old_trades)
    session.commit()
    session.close()
    
    return rg

def test_reconciliation_kpis(kpi_test_data):
    rg = kpi_test_data
    
    kpis = rg.get_reconciliation_kpis()
    
    # Today: 2 / 3 = 66.666...
    assert round(kpis['today_rate'], 2) == 66.67
    
    # 7-day: 3 / 5 = 60.0
    assert round(kpis['7d_rate'], 2) == 60.00
