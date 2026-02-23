import base64
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Image, Spacer, Table, TableStyle
from reportlab.lib import colors


def build_pdf_bytes(data: dict, chart_b64: str | None) -> bytes:
    """
    Build a PDF report with data and chart image.
    
    Args:
        data: Dictionary containing report data
        chart_b64: Base64 encoded chart image
    
    Returns:
        PDF content as bytes
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    elements = []
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1f77b4'),
        spaceAfter=30,
    )

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    elements = []
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1f77b4'),
        spaceAfter=30,
    )
    
    # Title
    title = Paragraph(f"Hourly Report (Last {data.get('hours', 'N/A')}h)", title_style)
    elements.append(title)
    
    # Generated at timestamp
    generated_at = Paragraph(
        f"<b>Generated:</b> {data.get('generated_at', 'N/A')}",
        styles['Normal']
    )
    elements.append(generated_at)
    elements.append(Spacer(1, 0.3 * inch))
    
    # Chart image if available
    if chart_b64:
        try:
            chart_bytes = base64.b64decode(chart_b64)
            chart_buffer = BytesIO(chart_bytes)
            chart_img = Image(chart_buffer, width=6 * inch, height=1.8 * inch)
            elements.append(chart_img)
            elements.append(Spacer(1, 0.3 * inch))
        except Exception as e:
            elements.append(Paragraph(f"<i>Chart image could not be embedded: {str(e)}</i>", styles['Normal']))
            elements.append(Spacer(1, 0.3 * inch))
    
    # Summary statistics
    totals = data.get('totals', {})
    yield_pct = data.get('yield_pct')
    ppm = data.get('ppm', 'N/A')
    
    summary_data = [
        ['Metric', 'Value'],
        ['Total Pieces', str(totals.get('total_pieces', 'N/A'))],
        ['OK', str(totals.get('total_ok', 'N/A'))],
        ['NOK', str(totals.get('total_nok', 'N/A'))],
        ['Quality rate (%)', f"{yield_pct or 'N/A'}%"],
        ['PPM', str(ppm)],
    ]
    
    summary_table = Table(summary_data, colWidths=[2.5 * inch, 2.5 * inch])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    
    elements.append(summary_table)
    elements.append(Spacer(1, 0.2 * inch))

    # Place defected chart here, after summary and before machine details
    defected_chart_b64 = data.get('defected_chart_b64')
    if defected_chart_b64:
        try:
            defected_bytes = base64.b64decode(defected_chart_b64)
            defected_buffer = BytesIO(defected_bytes)
            defected_img = Image(defected_buffer, width=3.5 * inch, height=1.8 * inch)
            elements.append(Paragraph('<b>Total Defected Pieces (NOK)</b>', styles['Heading3']))
            elements.append(defected_img)
            elements.append(Spacer(1, 0.2 * inch))
        except Exception as e:
            elements.append(Paragraph(f"<i>Defected chart could not be embedded: {str(e)}</i>", styles['Normal']))
            elements.append(Spacer(1, 0.2 * inch))

    # Per-machine detail table
    rows = data.get('rows', [])
    if rows:
        table_data = [[
            'Machine', 'Pieces', 'OK', 'NOK', 'Quality rate (%)', 'PPM', 'Avg cycle (ms)', 'Last event'
        ]]
        for r in rows:
            yield_str = 'N/A' if r.get('yield_pct') is None else f"{r.get('yield_pct')}%"
            avg_cycle = '—' if r.get('avg_cycle_ms') is None else str(r.get('avg_cycle_ms'))
            last_evt = '—' if r.get('last_event') is None else str(r.get('last_event'))
            table_data.append([
                r.get('machine_id'),
                str(r.get('pieces', 0)),
                str(r.get('ok', 0)),
                str(r.get('nok', 0)),
                yield_str,
                str(r.get('ppm', 0.0)),
                avg_cycle,
                last_evt,
            ])

        # Ajustement des largeurs pour éviter le chevauchement
        # Nouvelle répartition : élargir 'Quality rate (%)', ajuster les autres
        detail_table = Table(table_data, colWidths=[1.0*inch, 0.7*inch, 0.7*inch, 0.7*inch, 1.3*inch, 0.7*inch, 1.1*inch, 1.4*inch])
        detail_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e9ecef')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('ALIGN', (1, 1), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        elements.append(Paragraph('<b>Machine details</b>', styles['Heading3']))
        elements.append(Spacer(1, 0.05 * inch))
        elements.append(detail_table)

    # Build PDF
    doc.build(elements)
    return buffer.getvalue()
