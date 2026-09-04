import pytest
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import json

from app.utils import parse_timestamp, is_within_tolerance, is_timestamp_within_drift, calculate_pnl_consistency
from app.reconcile import ReconciliationEngine, ReconciliationResult, Base

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
EXECUTIONS_CSV = os.path.join(DATA_DIR, 'executions.csv')
CONFIRMATIONS_CSV = os.path.join(DATA_DIR, 'broker_confirmations.csv')
PNL_SNAPSHOT_CSV = os.path.join(DATA_DIR, 'pnl_snapshot.csv')

@pytest.fixture(scope="function")
def in_memory_db():
    """ This fixture now yields ONLY the session object. """
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session  
    session.close()
    Base.metadata.drop_all(engine)

@pytest.fixture(scope="function")
def reconciliation_engine(in_memory_db):
    """ This fixture now takes the session from in_memory_db and passes it to the engine. """
    # in_memory_db is now the session object itself
    shared_session = in_memory_db
    return ReconciliationEngine(db_url='sqlite:///:memory:', db_session=shared_session)



def load_and_prepare_trade_data():
    try:
        executions_df = pd.read_csv(EXECUTIONS_CSV)
        confirmations_df = pd.read_csv(CONFIRMATIONS_CSV)
        pnl_df = pd.read_csv(PNL_SNAPSHOT_CSV)
    except FileNotFoundError as e:
        pytest.fail(f"Required CSV file not found: {e}")

    merged_df = pd.merge(executions_df, confirmations_df, on='trade_id', suffixes=('_exec', '_conf'), how='outer')
    merged_df = pd.merge(merged_df, pnl_df, on='trade_id', how='left')

    test_cases = []
    for _, row in merged_df.iterrows():
        trade_id = row['trade_id']
        ticker = row.get('ticker_exec') or row.get('ticker_conf')

        exec_data = {
            'trade_id': trade_id,
            'ticker': ticker,
            'quantity': row.get('quantity_exec'),
            'price': row.get('price_exec'),
            'timestamp': row.get('timestamp_exec')
        } if pd.notna(row.get('quantity_exec')) else None

        conf_data = {
            'trade_id': trade_id,
            'ticker': ticker,
            'quantity': row.get('quantity_conf'),
            'price': row.get('price_conf'),
            'timestamp': row.get('timestamp_conf')
        } if pd.notna(row.get('quantity_conf')) else None

        pnl_data = {
            'trade_id': trade_id,
            'pnl_impact': row.get('pnl_impact'),
            'commission': row.get('commission')
        } if pd.notna(row.get('pnl_impact')) else None

        expected_status = 'PENDING'
        expected_mismatches = []

        if exec_data and conf_data:
            is_matched = True

            qty_matched, _ = is_within_tolerance(exec_data['quantity'], conf_data['quantity'], tolerance=0.0)
            if not qty_matched:
                expected_mismatches.append({
                    'field': 'quantity',
                    'execution': exec_data['quantity'],
                    'confirmation': conf_data['quantity'],
                    'reason': 'Quantity mismatch'
                })
                is_matched = False

            price_matched, _ = is_within_tolerance(exec_data['price'], conf_data['price'], tolerance=0.5, is_percentage=True)
            if not price_matched:
                expected_mismatches.append({
                    'field': 'price',
                    'execution': exec_data['price'],
                    'confirmation': conf_data['price'],
                    'reason': 'Price mismatch'
                })
                is_matched = False

            exec_ts = parse_timestamp(exec_data['timestamp'])
            conf_ts = parse_timestamp(conf_data['timestamp'])
            ts_matched, _ = is_timestamp_within_drift(exec_ts, conf_ts, drift_ms=100)
            if not ts_matched:
                expected_mismatches.append({
                    'field': 'timestamp',
                    'execution': exec_data['timestamp'],
                    'confirmation': conf_data['timestamp'],
                    'reason': 'Timestamp drift beyond 100ms'
                })
                is_matched = False

            if pnl_data and pd.notna(pnl_data['pnl_impact']) and pd.notna(pnl_data['commission']):
                try:
                    exec_price_num = float(exec_data['price'])
                    exec_qty_num = int(exec_data['quantity'])
                    pnl_impact_num = float(pnl_data['pnl_impact'])
                    commission_num = float(pnl_data['commission'])

                    if not calculate_pnl_consistency(exec_price_num, exec_qty_num, commission_num, pnl_impact_num, threshold=1.0):
                        expected_mismatches.append({
                            'field': 'pnl_consistency',
                            'calculated_pnl': round((exec_price_num * exec_qty_num) - commission_num, 2),
                            'reported_pnl_impact': pnl_impact_num,
                            'reason': 'PnL consistency check failed'
                        })
                        is_matched = False
                except (ValueError, TypeError):
                    pass

            expected_status = 'MISMATCHED' if not is_matched else 'MATCHED'
        elif exec_data or conf_data:
            expected_status = 'PENDING_INCOMPLETE'

        test_cases.append((
            trade_id,
            exec_data,
            conf_data,
            pnl_data,
            expected_status,
            expected_mismatches
        ))
    return test_cases

@pytest.mark.parametrize(
    "trade_id, exec_data, conf_data, pnl_data, expected_status, expected_mismatches",
    load_and_prepare_trade_data()
)

def test_full_reconciliation_flow(
    reconciliation_engine,
    trade_id, exec_data, conf_data, pnl_data, expected_status, expected_mismatches
):
    # Get the shared session directly from the pre-configured engine
    session = reconciliation_engine.db_session

    if exec_data:
        reconciliation_engine.process_message('executions', exec_data)
    if conf_data:
        reconciliation_engine.process_message('confirmations', conf_data)
    if pnl_data:
        reconciliation_engine.process_message('pnl_snapshot', pnl_data)

    result = session.query(ReconciliationResult).filter_by(trade_id=trade_id).first()

    if expected_status == 'PENDING_INCOMPLETE':
        assert result is None
    else:
        assert result is not None
        assert result.status == expected_status

        actual_mismatches = [{k: v for k, v in d.items() if k != 'severity'} for d in result.mismatch_details]
        actual_mismatches_json = sorted([json.dumps(d, sort_keys=True) for d in actual_mismatches])
        expected_mismatches_json = sorted([json.dumps(d, sort_keys=True) for d in expected_mismatches])

        assert actual_mismatches_json == expected_mismatches_json

def test_asset_class_tolerance(reconciliation_engine):
    """
    Test that two trades with identical raw deviation get different
    pass/fail outcomes depending on their asset class.
    """
    now = datetime.utcnow()
    
    # Equity trade (0.5% price tolerance limit)
    base_exec = {
        'trade_id': 'T_EQUITY',
        'ticker': 'TEST',
        'quantity': 100,
        'price': 100.0,
        'timestamp': now.isoformat() + 'Z',
        'asset_class': 'equity'
    }
    
    # 0.75% deviation in price (0.75 diff on 100.0 base)
    conf_equity = base_exec.copy()
    conf_equity['price'] = 100.75

    reconciliation_engine.process_message('executions', base_exec)
    reconciliation_engine.process_message('confirmations', conf_equity)

    # Fixed income trade (1.0% price tolerance limit)
    base_exec_fi = base_exec.copy()
    base_exec_fi['trade_id'] = 'T_FI'
    base_exec_fi['asset_class'] = 'fixed_income'
    
    conf_fi = base_exec_fi.copy()
    conf_fi['price'] = 100.75

    reconciliation_engine.process_message('executions', base_exec_fi)
    reconciliation_engine.process_message('confirmations', conf_fi)

    session = reconciliation_engine.db_session
    result_equity = session.query(ReconciliationResult).filter_by(trade_id='T_EQUITY').first()
    result_fi = session.query(ReconciliationResult).filter_by(trade_id='T_FI').first()

    assert result_equity is not None
    assert result_fi is not None
    
    # Equity limit is 0.5%, so 0.75% deviation should fail
    assert result_equity.status == 'MISMATCHED'
    
    # Fixed income limit is 1.0%, so 0.75% deviation should pass
    assert result_fi.status == 'MATCHED'

def test_severity_tiering(reconciliation_engine):
    now = datetime.utcnow()
    # Equity tolerance is 0.5% (0.005)
    
    # 1. INFO: < 2x breach (e.g. 0.75% breach on 100 is 0.75, which is 1.5x of 0.5)
    t_info_exec = {'trade_id': 'T_INFO', 'ticker': 'INFO', 'quantity': 100, 'price': 100.0, 'timestamp': now.isoformat() + 'Z', 'asset_class': 'equity'}
    t_info_conf = t_info_exec.copy()
    t_info_conf['price'] = 100.75 
    reconciliation_engine.process_message('executions', t_info_exec)
    reconciliation_engine.process_message('confirmations', t_info_conf)

    # 2. WARNING: 2x - 5x breach (e.g. 2.0% breach on 100 is 2.0, which is 4x of 0.5)
    t_warn_exec = {'trade_id': 'T_WARN', 'ticker': 'WARN', 'quantity': 100, 'price': 100.0, 'timestamp': now.isoformat() + 'Z', 'asset_class': 'equity'}
    t_warn_conf = t_warn_exec.copy()
    t_warn_conf['price'] = 102.0
    reconciliation_engine.process_message('executions', t_warn_exec)
    reconciliation_engine.process_message('confirmations', t_warn_conf)

    # 3. CRITICAL: > 5x breach (e.g. 5.0% breach on 100 is 5.0, which is 10x of 0.5)
    t_crit_exec = {'trade_id': 'T_CRIT', 'ticker': 'CRIT', 'quantity': 100, 'price': 100.0, 'timestamp': now.isoformat() + 'Z', 'asset_class': 'equity'}
    t_crit_conf = t_crit_exec.copy()
    t_crit_conf['price'] = 105.0
    reconciliation_engine.process_message('executions', t_crit_exec)
    reconciliation_engine.process_message('confirmations', t_crit_conf)

    session = reconciliation_engine.db_session
    res_info = session.query(ReconciliationResult).filter_by(trade_id='T_INFO').first()
    res_warn = session.query(ReconciliationResult).filter_by(trade_id='T_WARN').first()
    res_crit = session.query(ReconciliationResult).filter_by(trade_id='T_CRIT').first()

    assert res_info.highest_severity == 'INFO'
    assert res_warn.highest_severity == 'WARNING'
    assert res_crit.highest_severity == 'CRITICAL'

