import os
import threading
import time
from flask import Flask, render_template, send_file, request, jsonify
from .consumer import TradeDataConsumer
from .reconcile import ReconciliationEngine
from .report_generator import ReportGenerator
from prometheus_client import start_http_server, Counter, Gauge, Histogram

app = Flask(__name__, template_folder='../reports/templates', static_folder='../reports')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
DB_URL = 'sqlite:///./reports/reconciliation.db'

TOTAL_TRADES_PROCESSED = Counter('traderecon_total_trades_processed', 'Total number of trades processed')
MATCHED_TRADES_COUNT = Counter('traderecon_matched_trades_total', 'Total number of trades that matched')
MISMATCHED_TRADES_COUNT = Counter('traderecon_mismatched_trades_total', 'Total number of trades that mismatched')
TRADECON_ANOMALIES_TOTAL = Counter('traderecon_anomalies_total', 'Total number of anomalous trades detected')
IN_MEMORY_STORE_SIZE = Gauge('traderecon_in_memory_store_size', 'Current size of the in-memory trade store')
RECONCILIATION_LATENCY_SECONDS = Histogram('traderecon_reconciliation_latency_seconds', 'Latency of trade reconciliation (seconds)')

TEST_HTTP_REQUESTS_TOTAL = Counter('traderecon_http_requests_total', 'Total HTTP requests to Flask app')
OPEN_ESCALATIONS = Gauge('traderecon_open_escalations', 'Count of open CRITICAL escalations')

reconciliation_engine = ReconciliationEngine(db_url=DB_URL)
report_generator = ReportGenerator(db_url=DB_URL, template_dir='./reports/templates')
reconciliation_engine.set_metrics_collectors(
    total_trades_counter=TOTAL_TRADES_PROCESSED,
    matched_trades_counter=MATCHED_TRADES_COUNT,
    mismatched_trades_counter=MISMATCHED_TRADES_COUNT,
    in_memory_store_size_gauge=IN_MEMORY_STORE_SIZE,
    reconciliation_latency_histogram=RECONCILIATION_LATENCY_SECONDS,
    anomalies_counter=TRADECON_ANOMALIES_TOTAL,
    open_escalations_gauge=OPEN_ESCALATIONS
)

consumer_threads = []

@app.route('/')
def index():
    TEST_HTTP_REQUESTS_TOTAL.inc()
    html_report_content = report_generator.generate_html_report()
    return html_report_content

@app.route('/api/reconciliation_status')
def get_reconciliation_status():
    TEST_HTTP_REQUESTS_TOTAL.inc()
    df = report_generator.fetch_all_reconciliation_results()
    return jsonify(df.to_dict(orient='records'))

@app.route('/download/csv')
def download_csv():
    TEST_HTTP_REQUESTS_TOTAL.inc() # Also increment for CSV downloads
    csv_filename = 'reconciliation_report.csv'
    report_generator.generate_csv_report(filename=csv_filename)
    csv_filepath = os.path.join(report_generator.report_output_dir, csv_filename)
    return send_file(csv_filepath, as_attachment=True, download_name='TradeRecon_Report.csv', mimetype='text/csv')

import math
import pandas as pd

@app.route('/audit-log')
def audit_log():
    TEST_HTTP_REQUESTS_TOTAL.inc()
    page = request.args.get('page', 1, type=int)
    per_page = 50
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    trade_id = request.args.get('trade_id')
    asset_class = request.args.get('asset_class')
    highest_severity = request.args.get('highest_severity')
    status = request.args.get('status')

    data, total_count = report_generator.get_filtered_audit_log(
        page=page, per_page=per_page, start_date=start_date, end_date=end_date,
        trade_id=trade_id, asset_class=asset_class, highest_severity=highest_severity, status=status
    )
    
    total_pages = math.ceil(total_count / per_page)
    
    return render_template('audit_log.html', 
                           data=data, 
                           page=page, 
                           total_pages=total_pages,
                           request_args=request.args)

@app.route('/audit-log/export')
def export_audit_log():
    TEST_HTTP_REQUESTS_TOTAL.inc()
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    trade_id = request.args.get('trade_id')
    asset_class = request.args.get('asset_class')
    highest_severity = request.args.get('highest_severity')
    status = request.args.get('status')

    # Get all results for export, without pagination
    data, _ = report_generator.get_filtered_audit_log(
        page=1, per_page=1000000, start_date=start_date, end_date=end_date,
        trade_id=trade_id, asset_class=asset_class, highest_severity=highest_severity, status=status
    )
    
    df = pd.DataFrame(data)
    if not df.empty:
        df['mismatch_summary'] = df['mismatch_details'].apply(
            lambda x: ', '.join([f"{d['field']}: {d['reason']}" for d in x]) if isinstance(x, list) and x else 'N/A'
        )
    
    csv_filename = 'audit_log_export.csv'
    csv_filepath = os.path.join(report_generator.report_output_dir, csv_filename)
    df.to_csv(csv_filepath, index=False)
    
    return send_file(csv_filepath, as_attachment=True, download_name='Audit_Log_Export.csv', mimetype='text/csv')

@app.route('/audit-log/<trade_id>')
def trade_detail(trade_id):
    TEST_HTTP_REQUESTS_TOTAL.inc()
    trade = report_generator.get_trade_by_id(trade_id)
    if not trade:
        return "Trade not found", 404
        
    mismatched_fields = [m.get('field') for m in trade.mismatch_details] if trade.mismatch_details else []
    
    return render_template('trade_detail.html', trade=trade, mismatched_fields=mismatched_fields)

from .pdf_generator import PDFGenerator
from .reconcile import EODSignOff
from datetime import datetime as dt_module

pdf_generator = PDFGenerator(db_url=DB_URL)

from flask import redirect

@app.route('/sign-off', methods=['POST'])
def sign_off():
    TEST_HTTP_REQUESTS_TOTAL.inc()
    reviewer_name = request.form.get('reviewer_name')
    if not reviewer_name:
        return "Reviewer name is required", 400
        
    date_str = dt_module.utcnow().date().strftime('%Y-%m-%d')
    
    session = reconciliation_engine.session
    try:
        signoff = EODSignOff(reviewer_name=reviewer_name, report_date=date_str)
        session.add(signoff)
        session.commit()
    except Exception as e:
        session.rollback()
        return f"Error saving sign-off: {e}", 500
        
    # Redirect back to the dashboard
    return request.environ.get('HTTP_REFERER') and redirect(request.environ.get('HTTP_REFERER')) or "Sign-off successful! <a href='/'>Back to Dashboard</a>"

@app.route('/generate-eod-report')

def generate_eod_report():
    TEST_HTTP_REQUESTS_TOTAL.inc()
    try:
        # Defaults to today
        filepath = pdf_generator.generate_eod_report()
        return send_file(filepath, as_attachment=True)
    except Exception as e:
        return f"Error generating PDF: {e}", 500

def start_consumers():
    print("Starting Kafka consumers...")
    execution_consumer = TradeDataConsumer(
        topic='executions',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f'traderecon_exec_group_{int(time.time())}',
        reconcile_engine=reconciliation_engine
    )
    confirmation_consumer = TradeDataConsumer(
        topic='confirmations',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f'traderecon_conf_group_{int(time.time())}',
        reconcile_engine=reconciliation_engine
    )
    pnl_consumer = TradeDataConsumer(
        topic='pnl_snapshot',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f'traderecon_pnl_group_{int(time.time())}',
        reconcile_engine=reconciliation_engine
    )

    consumer_threads.append(execution_consumer)
    consumer_threads.append(confirmation_consumer)
    consumer_threads.append(pnl_consumer)

    for consumer in consumer_threads:
        consumer.start()
        time.sleep(0.5)

    print("All Kafka consumer threads started.")

def stop_consumers():
    print("Stopping Kafka consumers...")
    for consumer in consumer_threads:
        consumer.stop()
    for consumer in consumer_threads:
        consumer.join()
    print("All Kafka consumer threads stopped.")

if __name__ == '__main__':
    start_http_server(8000, addr='0.0.0.0')

    print("Prometheus metrics server started on port 8000.")

    consumer_thread = threading.Thread(target=start_consumers)
    consumer_thread.start()

    try:
        app.run(debug=False, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        print("Flask app shutting down.")
    finally:
        stop_consumers()
        consumer_thread.join()
        print("Application gracefully shut down.")
