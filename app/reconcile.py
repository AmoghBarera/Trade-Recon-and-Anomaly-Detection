import threading
import time
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Float, DateTime, Boolean, JSON, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.sqlite import JSON as SQLiteJSON
import json
import pandas as pd # Import pandas here for pd.notna
from .utils import parse_timestamp, is_within_tolerance, is_timestamp_within_drift, calculate_pnl_consistency
import os
import yaml
from .anomaly_detector import AnomalyDetector
Base = declarative_base()

class Escalation(Base):
    __tablename__ = 'escalations'
    id = Column(Integer, primary_key=True)
    trade_id = Column(String, nullable=False)
    severity = Column(String, nullable=False)
    reason = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)

class ReconciliationResult(Base):
    """
    SQLAlchemy model for storing reconciliation results.
    Using JSON type for mismatch_details to store flexible mismatch info.
    """
    __tablename__ = 'reconciliation_results'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_id = Column(String, unique=True, nullable=False)
    ticker = Column(String)
    status = Column(String, nullable=False) # 'MATCHED', 'MISMATCHED', 'PENDING'
    execution_data = Column(SQLiteJSON) # Store as JSON
    confirmation_data = Column(SQLiteJSON) # Store as JSON
    pnl_data = Column(SQLiteJSON) # Store as JSON
    mismatch_details = Column(SQLiteJSON) # Store details of mismatches as JSON
    reconciliation_timestamp = Column(DateTime, default=datetime.utcnow)
    anomaly_score = Column(Float, nullable=True)
    is_anomalous = Column(Boolean, nullable=True)
    asset_class = Column(String, nullable=True)
    applied_tolerance = Column(String, nullable=True)
    highest_severity = Column(String, nullable=True)

    def __repr__(self):
        return f"<ReconciliationResult(trade_id='{self.trade_id}', status='{self.status}')>"

class EODSignOff(Base):
    __tablename__ = 'eod_signoffs'
    id = Column(Integer, primary_key=True)
    reviewer_name = Column(String, nullable=False)
    report_date = Column(String, nullable=False) # e.g. YYYY-MM-DD
    signoff_timestamp = Column(DateTime, default=datetime.utcnow)

class ReconciliationEngine:
    """
    Core engine for real-time trade reconciliation.
    Manages incoming trade data and performs reconciliation checks.
    Uses an in-memory store for pending trades and SQLAlchemy for persistence.
    """
    def __init__(self, db_url='sqlite:///./reports/reconciliation.db', db_session=None):
        self.trade_store = {}
        self.trade_store_lock = threading.Lock()
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)

        # This is the key change:
        if db_session:
            # If a session is provided (from your test), use it.
            self.session = db_session
        else:
            # Otherwise (in your main app), create a new one.
            Session = sessionmaker(bind=self.engine)
            self.session = Session()

        # Keep a public reference for tests to use
        self.db_session = self.session

        self.anomaly_detector = AnomalyDetector(data_dir='./data')

        self.tolerances = self._load_tolerances('./config/tolerances.yaml')

        print(f"ReconciliationEngine initialized with DB: {db_url}")
        
        # Metrics (set externally)
        self.total_trades_counter = None
        self.matched_trades_counter = None
        self.mismatched_trades_counter = None
        self.anomalies_counter = None
        self.in_memory_store_size_gauge = None
        self.in_memory_store_size_gauge = None
        self.reconciliation_latency_histogram = None
        self.open_escalations_gauge = None

    def _load_tolerances(self, config_path):
        default_tolerances = {'default': {'price_pct': 0.5, 'timestamp_ms': 100}}
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f) or default_tolerances
        except FileNotFoundError:
            print(f"Warning: Tolerance config {config_path} not found. Using defaults.")
            return default_tolerances

    def set_metrics_collectors(self, total_trades_counter, matched_trades_counter,
                               mismatched_trades_counter, in_memory_store_size_gauge,
                               reconciliation_latency_histogram, anomalies_counter=None,
                               open_escalations_gauge=None):
        self.total_trades_counter = total_trades_counter
        self.matched_trades_counter = matched_trades_counter
        self.mismatched_trades_counter = mismatched_trades_counter
        self.in_memory_store_size_gauge = in_memory_store_size_gauge
        self.reconciliation_latency_histogram = reconciliation_latency_histogram
        self.anomalies_counter = anomalies_counter
        self.open_escalations_gauge = open_escalations_gauge

    def process_message(self, topic: str, message: dict):
        trade_id = message.get('trade_id')
        if not trade_id:
            print(f"Warning: Message from topic {topic} missing 'trade_id': {message}")
            return
        trade_id = str(trade_id)

        with self.trade_store_lock:
            if trade_id not in self.trade_store:
                self.trade_store[trade_id] = {
                    'execution': None, 'confirmation': None, 'pnl': None,
                    'status': 'PENDING', 'start_time': time.time()
                }
                if self.in_memory_store_size_gauge:
                    self.in_memory_store_size_gauge.inc()

            if topic == 'executions':
                self.trade_store[trade_id]['execution'] = message
            elif topic == 'confirmations':
                self.trade_store[trade_id]['confirmation'] = message
            elif topic == 'pnl_snapshot':
                self.trade_store[trade_id]['pnl'] = message
            else:
                print(f"Unknown topic: {topic} for trade_id {trade_id}")
                return

            self._attempt_reconciliation(trade_id)

    def _attempt_reconciliation(self, trade_id: str):
        trade_data = self.trade_store.get(trade_id)
        if not trade_data:
            print(f"Trade {trade_id} not found in store for reconciliation")
            return

        execution = trade_data.get('execution')
        confirmation = trade_data.get('confirmation')
        pnl = trade_data.get('pnl')

        if execution and confirmation:
            self._perform_reconciliation_and_save(trade_id, execution, confirmation, pnl)
        else:
            print(f"Trade {trade_id} not ready. Missing execution or confirmation.")

    def _perform_reconciliation_and_save(self, trade_id: str, execution: dict, confirmation: dict, pnl: dict = None):
        mismatches = []
        is_matched = True

        if self.total_trades_counter:
            self.total_trades_counter.inc()

        # --- Anomaly Detection ---
        is_anomalous, anomaly_score = self.anomaly_detector.score_trade(execution, confirmation)
        if is_anomalous:
            print(f"🚨 ANOMALY DETECTED for Trade {trade_id}! Score: {anomaly_score:.4f}")
            if self.anomalies_counter:
                self.anomalies_counter.inc()

        # --- Reconciliation Checks ---
        asset_class = execution.get('asset_class')
        profile_name = asset_class.lower() if asset_class and asset_class.lower() in self.tolerances else 'default'
        tol_config = self.tolerances.get(profile_name, self.tolerances.get('default'))

        def add_mismatch(field, reason, breach_factor, **kwargs):
            if breach_factor == float('inf') or breach_factor > 5:
                severity = 'CRITICAL'
            elif breach_factor >= 2:
                severity = 'WARNING'
            else:
                severity = 'INFO'
            mismatch = {'field': field, 'reason': reason, 'severity': severity}
            mismatch.update(kwargs)
            mismatches.append(mismatch)

        qty_matched, qty_bf = is_within_tolerance(execution['quantity'], confirmation['quantity'], tolerance=0.0)
        if not qty_matched:
            add_mismatch('quantity', 'Quantity mismatch', qty_bf, execution=execution['quantity'], confirmation=confirmation['quantity'])
            is_matched = False
            
        price_matched, price_bf = is_within_tolerance(execution['price'], confirmation['price'], tolerance=tol_config['price_pct'], is_percentage=True)
        if not price_matched:
            add_mismatch('price', f"Price mismatch (> {tol_config['price_pct']}%)", price_bf, execution=execution['price'], confirmation=confirmation['price'])
            is_matched = False
            
        exec_ts = parse_timestamp(execution['timestamp'])
        conf_ts = parse_timestamp(confirmation['timestamp'])
        ts_matched, ts_bf = is_timestamp_within_drift(exec_ts, conf_ts, drift_ms=tol_config['timestamp_ms'])
        if not ts_matched:
            add_mismatch('timestamp', f"Timestamp drift beyond {tol_config['timestamp_ms']}ms", ts_bf, execution=execution['timestamp'], confirmation=confirmation['timestamp'])
            is_matched = False
            
        if pnl and pd.notna(pnl.get('pnl_impact')) and pd.notna(pnl.get('commission')):
            try:
                exec_price_num = float(execution['price'])
                exec_qty_num = int(execution['quantity'])
                pnl_impact_num = float(pnl['pnl_impact'])
                commission_num = float(pnl['commission'])
                if not calculate_pnl_consistency(exec_price_num, exec_qty_num, commission_num, pnl_impact_num, threshold=1.0):
                    add_mismatch('pnl_consistency', 'PnL consistency check failed', float('inf'), calculated_pnl=round((exec_price_num * exec_qty_num) - commission_num, 2), reported_pnl_impact=pnl_impact_num)
                    is_matched = False
            except (ValueError, TypeError) as e:
                add_mismatch('pnl_calculation_error', f'Error converting PnL related values: {e}', float('inf'))
                is_matched = False

        highest_severity = None
        for m in mismatches:
            s = m['severity']
            if s == 'CRITICAL':
                highest_severity = 'CRITICAL'
                esc = Escalation(trade_id=trade_id, severity=s, reason=m['reason'])
                self.session.add(esc)
                if self.open_escalations_gauge:
                    self.open_escalations_gauge.inc()
            elif s == 'WARNING' and highest_severity != 'CRITICAL':
                highest_severity = 'WARNING'
            elif s == 'INFO' and highest_severity not in ('CRITICAL', 'WARNING'):
                highest_severity = 'INFO'

        status = 'MISMATCHED' if not is_matched else 'MATCHED'
        print(f"Reconciliation for Trade {trade_id}: Status = {status}")
        if mismatches:
            print(f"  Mismatches: {mismatches}")
            if self.mismatched_trades_counter: self.mismatched_trades_counter.inc()
        else:
            if self.matched_trades_counter: self.matched_trades_counter.inc()
        
        if 'start_time' in self.trade_store[trade_id] and self.reconciliation_latency_histogram:
            latency = time.time() - self.trade_store[trade_id]['start_time']
            self.reconciliation_latency_histogram.observe(latency)

        session = self.session
        try:
            existing = session.query(ReconciliationResult).filter_by(trade_id=trade_id).first()
            if existing:
                existing.ticker = execution.get('ticker', 'N/A')
                existing.status = status
                existing.execution_data = execution
                existing.confirmation_data = confirmation
                existing.pnl_data = pnl
                existing.mismatch_details = mismatches
                existing.reconciliation_timestamp = datetime.utcnow()
                existing.anomaly_score = anomaly_score
                existing.is_anomalous = is_anomalous
                existing.asset_class = asset_class
                existing.applied_tolerance = profile_name
                existing.highest_severity = highest_severity
                print(f"Updated trade {trade_id} in DB.")
            else:
                new_result = ReconciliationResult(
                    trade_id=trade_id, ticker=execution.get('ticker', 'N/A'), status=status,
                    execution_data=execution, confirmation_data=confirmation,
                    pnl_data=pnl, mismatch_details=mismatches,
                    anomaly_score=anomaly_score, is_anomalous=is_anomalous,
                    asset_class=asset_class, applied_tolerance=profile_name,
                    highest_severity=highest_severity
                )
                session.add(new_result)
                print(f"Inserted trade {trade_id} into DB.")
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"DB error for trade {trade_id}: {e}")
        # NOTE: We do NOT close the session here, as it's managed by the fixture or the main app.