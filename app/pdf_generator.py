import os
from datetime import datetime, date
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .reconcile import ReconciliationResult, EODSignOff

class PDFGenerator:
    def __init__(self, db_url='sqlite:///./reports/reconciliation.db'):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.reports_dir = './reports/eod'
        os.makedirs(self.reports_dir, exist_ok=True)

    def _get_session(self):
        return self.Session()

    def generate_eod_report(self, target_date: date = None):
        """
        Generates an EOD PDF report for the target date. 
        Defaults to today's date if none provided.
        Returns the filepath of the generated PDF.
        """
        if target_date is None:
            target_date = datetime.utcnow().date()
            
        date_str = target_date.strftime('%Y-%m-%d')
        filename = f"{date_str}_reconciliation_report.pdf"
        filepath = os.path.join(self.reports_dir, filename)

        session = self._get_session()
        try:
            # Query for the target date
            # Since timestamps are datetime objects, we filter between start and end of day
            start_dt = datetime.combine(target_date, datetime.min.time())
            end_dt = datetime.combine(target_date, datetime.max.time())
            
            trades = session.query(ReconciliationResult).filter(
                ReconciliationResult.reconciliation_timestamp >= start_dt,
                ReconciliationResult.reconciliation_timestamp <= end_dt
            ).all()
            
            sign_off = session.query(EODSignOff).filter_by(report_date=date_str).order_by(EODSignOff.id.desc()).first()

            total_trades = len(trades)
            matched_count = sum(1 for t in trades if t.status == 'MATCHED')
            match_rate = (matched_count / total_trades * 100) if total_trades > 0 else 0.0
            
            severity_counts = {'INFO': 0, 'WARNING': 0, 'CRITICAL': 0, 'NONE': 0}
            critical_trades = []
            
            for t in trades:
                sev = t.highest_severity
                if sev in severity_counts:
                    severity_counts[sev] += 1
                else:
                    severity_counts['NONE'] += 1
                    
                if sev == 'CRITICAL':
                    critical_trades.append(t)

            self._build_pdf(filepath, date_str, total_trades, match_rate, severity_counts, critical_trades, sign_off)
            return filepath
            
        except Exception as e:
            print(f"Error generating PDF report: {e}")
            raise
        finally:
            session.close()

    def _build_pdf(self, filepath, date_str, total_trades, match_rate, severity_counts, critical_trades, sign_off):
        doc = SimpleDocTemplate(filepath, pagesize=letter)
        styles = getSampleStyleSheet()
        
        # Custom styles
        title_style = ParagraphStyle(
            'TitleStyle',
            parent=styles['Heading1'],
            alignment=1, # Center
            spaceAfter=20
        )
        heading_style = styles['Heading2']
        normal_style = styles['Normal']
        
        elements = []
        
        # Title
        elements.append(Paragraph(f"End of Day Reconciliation Report", title_style))
        elements.append(Paragraph(f"Date: {date_str}", styles['Normal']))
        elements.append(Paragraph(f"Generated at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}", styles['Normal']))
        elements.append(Spacer(1, 20))
        
        # Summary Section
        elements.append(Paragraph("Summary", heading_style))
        summary_data = [
            ["Metric", "Value"],
            ["Total Trades Processed", str(total_trades)],
            ["Match Rate", f"{match_rate:.2f}%"]
        ]
        summary_table = Table(summary_data, colWidths=[200, 100])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(summary_table)
        elements.append(Spacer(1, 20))
        
        # Severity Breakdown
        elements.append(Paragraph("Mismatches by Severity", heading_style))
        severity_data = [
            ["Severity", "Count"],
            ["CRITICAL", str(severity_counts['CRITICAL'])],
            ["WARNING", str(severity_counts['WARNING'])],
            ["INFO", str(severity_counts['INFO'])]
        ]
        severity_table = Table(severity_data, colWidths=[200, 100])
        severity_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(severity_table)
        elements.append(Spacer(1, 20))
        
        # Critical Escalations
        elements.append(Paragraph("CRITICAL Escalations", heading_style))
        if critical_trades:
            crit_data = [["Trade ID", "Ticker", "Mismatches"]]
            for t in critical_trades:
                mismatch_summary = ", ".join([f"{m['field']}: {m['reason']}" for m in (t.mismatch_details or [])])
                crit_data.append([t.trade_id, t.ticker or 'N/A', mismatch_summary])
            
            crit_table = Table(crit_data, colWidths=[100, 60, 300])
            crit_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.red),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('VALIGN', (0, 0), (-1, -1), 'TOP')
            ]))
            elements.append(crit_table)
        else:
            elements.append(Paragraph("No CRITICAL escalations for this date.", normal_style))
            
        elements.append(Spacer(1, 40))
        
        # Sign-off Section
        elements.append(Paragraph("Review & Sign-Off", heading_style))
        if sign_off:
            elements.append(Paragraph(f"<b>Reviewer Name:</b> {sign_off.reviewer_name}", normal_style))
            elements.append(Paragraph(f"<b>Sign-off Timestamp (UTC):</b> {sign_off.signoff_timestamp.strftime('%Y-%m-%d %H:%M:%S')}", normal_style))
            elements.append(Paragraph(f"<b>Status:</b> Approved", normal_style))
        else:
            elements.append(Paragraph("<b>Status:</b> PENDING SIGN-OFF", normal_style))
            elements.append(Paragraph("No reviewer sign-off recorded for this date.", normal_style))

        # Build PDF
        doc.build(elements)
