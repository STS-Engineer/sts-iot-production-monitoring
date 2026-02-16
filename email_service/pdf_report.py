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
        ['Yield', f"{yield_pct or 'N/A'}%"],
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
    
    # Build PDF
    doc.build(elements)
    return buffer.getvalue()
