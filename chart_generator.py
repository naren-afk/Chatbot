import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Any, List
import io
import base64
from datetime import datetime
import json
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
import tempfile
import os
import sys

class ChartGenerator:
    """Generates charts and visualizations for manufacturing data"""
    
    def __init__(self):
        # Set matplotlib style
        plt.style.use('default')
        sns.set_palette("husl")
        
    def generate_charts(self, analysis: Dict[str, Any], machine_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate charts based on analysis and data"""
        try:
            charts = []
            
            if not machine_data or 'combined_data' not in machine_data or machine_data['combined_data'] is None:
                return charts
            
            df = machine_data['combined_data']
            machine_name = machine_data.get('machine', 'Unknown')
            
            # Add machine identifier to chart titles
            self.machine_name = machine_name
            
            # Generate charts based on analysis intent
            intent = analysis.get('intent', 'summary')
            chart_types = analysis.get('chart_types', ['bar'])
            
            if intent == 'summary' or 'summary' in analysis.get('analysis_type', ''):
                charts.extend(self._generate_summary_charts(df))
            
            if intent == 'comparison':
                charts.extend(self._generate_comparison_charts(df))
            
            if intent == 'trend':
                charts.extend(self._generate_trend_charts(df))
            
            # Generate specific chart types if requested
            if 'bar' in chart_types:
                charts.extend(self._generate_bar_charts(df))
            
            if 'line' in chart_types:
                charts.extend(self._generate_line_charts(df))
            
            return charts[:4]  # Limit to 4 charts max
            
        except Exception as e:
            logging.error(f"Error generating charts: {str(e)}")
            return []
    
    def _generate_summary_charts(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Generate summary charts"""
        charts = []
        
        try:
            # OEE Distribution Chart
            if 'avg_oee' in df.columns:
                chart_data = self._create_oee_chart(df)
                if chart_data:
                    charts.append(chart_data)
            
            # Production vs Rejected Parts
            if 'total_part_count' in df.columns and 'total_part_rejected' in df.columns:
                chart_data = self._create_production_quality_chart(df)
                if chart_data:
                    charts.append(chart_data)
            
        except Exception as e:
            logging.error(f"Error generating summary charts: {str(e)}")
        
        return charts
    
    def _generate_comparison_charts(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Generate comparison charts"""
        charts = []
        
        try:
            # Monthly comparison if data spans multiple months
            if 'date' in df.columns:
                chart_data = self._create_monthly_comparison_chart(df)
                if chart_data:
                    charts.append(chart_data)
            
            # Shift comparison
            if 'Shift' in df.columns:
                chart_data = self._create_shift_comparison_chart(df)
                if chart_data:
                    charts.append(chart_data)
            
        except Exception as e:
            logging.error(f"Error generating comparison charts: {str(e)}")
        
        return charts
    
    def _generate_trend_charts(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Generate trend charts"""
        charts = []
        
        try:
            if 'date' in df.columns:
                # OEE trend over time
                if 'avg_oee' in df.columns:
                    chart_data = self._create_oee_trend_chart(df)
                    if chart_data:
                        charts.append(chart_data)
                
                # Energy consumption trend
                if 'avg_total_energy' in df.columns:
                    chart_data = self._create_energy_trend_chart(df)
                    if chart_data:
                        charts.append(chart_data)
            
        except Exception as e:
            logging.error(f"Error generating trend charts: {str(e)}")
        
        return charts
    
    def _generate_bar_charts(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Generate bar charts"""
        charts = []
        
        try:
            # Machine status distribution
            if 'Machine Status' in df.columns:
                chart_data = self._create_machine_status_chart(df)
                if chart_data:
                    charts.append(chart_data)
            
        except Exception as e:
            logging.error(f"Error generating bar charts: {str(e)}")
        
        return charts
    
    def _generate_line_charts(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Generate line charts"""
        charts = []
        
        try:
            if 'date' in df.columns and 'energyCost' in df.columns:
                chart_data = self._create_cost_trend_chart(df)
                if chart_data:
                    charts.append(chart_data)
            
        except Exception as e:
            logging.error(f"Error generating line charts: {str(e)}")
        
        return charts
    
    def _create_oee_chart(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create OEE distribution chart"""
        try:
            plt.figure(figsize=(10, 6))
            
            oee_data = df['avg_oee'].dropna()
            if oee_data.empty:
                return None
            
            # Create histogram with better styling
            n, bins, patches = plt.hist(oee_data, bins=15, alpha=0.7, color='steelblue', edgecolor='black')
            
            # Color bars based on OEE performance
            for i, (patch, bin_val) in enumerate(zip(patches, bins[:-1])):
                if bin_val < 70:
                    patch.set_facecolor('lightcoral')
                elif bin_val < 85:
                    patch.set_facecolor('gold')
                else:
                    patch.set_facecolor('lightgreen')
            
            machine_title = f'OEE Performance Distribution - {getattr(self, "machine_name", "Machine")}'
            plt.title(machine_title, fontsize=16, fontweight='bold')
            plt.xlabel('OEE (%)')
            plt.ylabel('Number of Records')
            plt.grid(True, alpha=0.3)
            
            # Add statistics
            mean_oee = oee_data.mean()
            plt.axvline(mean_oee, color='red', linestyle='--', linewidth=2, label=f'Average: {mean_oee:.1f}%')
            
            # Add performance zones
            plt.axvspan(0, 70, alpha=0.1, color='red', label='Below Standard')
            plt.axvspan(70, 85, alpha=0.1, color='orange', label='Good')
            plt.axvspan(85, 100, alpha=0.1, color='green', label='Excellent')
            
            plt.legend()
            plt.tight_layout()
            
            # Convert to base64
            img_base64 = self._plt_to_base64()
            plt.close()
            
            return {
                'type': 'histogram',
                'title': machine_title,
                'image': img_base64,
                'description': f'{getattr(self, "machine_name", "Machine")}: OEE ranges from {oee_data.min():.1f}% to {oee_data.max():.1f}% with average of {mean_oee:.1f}%. Total records: {len(oee_data)}'
            }
            
        except Exception as e:
            logging.error(f"Error creating OEE chart: {str(e)}")
            plt.close()
            return None
    
    def _create_production_quality_chart(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create production vs quality chart"""
        try:
            plt.figure(figsize=(10, 6))
            
            total_produced = df['total_part_count'].sum()
            total_rejected = df['total_part_reject'].sum()
            total_good = total_produced - total_rejected
            
            labels = ['Good Parts', 'Rejected Parts']
            sizes = [total_good, total_rejected]
            colors = ['lightgreen', 'lightcoral']
            
            plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
            plt.title('Production Quality Overview', fontsize=16, fontweight='bold')
            plt.axis('equal')
            
            img_base64 = self._plt_to_base64()
            plt.close()
            
            quality_rate = (total_good / total_produced * 100) if total_produced > 0 else 0
            
            return {
                'type': 'pie',
                'title': 'Production Quality Overview',
                'image': img_base64,
                'description': f'Quality rate: {quality_rate:.1f}% ({total_good:,} good parts out of {total_produced:,} total)'
            }
            
        except Exception as e:
            logging.error(f"Error creating production quality chart: {str(e)}")
            plt.close()
            return None
    
    def _create_monthly_comparison_chart(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create monthly comparison chart"""
        try:
            df_clean = df.dropna(subset=['date'])
            if df_clean.empty:
                return None
            
            # Group by month
            df_clean['Month'] = df_clean['date'].dt.to_period('M')
            monthly_data = df_clean.groupby('Month').agg({
                'total_part_count': 'sum',
                'avg_oee': 'mean'
            }).reset_index()
            
            if len(monthly_data) < 2:
                return None
            
            fig, ax1 = plt.subplots(figsize=(12, 6))
            
            # Production bars
            months = [str(m) for m in monthly_data['Month']]
            ax1.bar(months, monthly_data['total_part_count'], alpha=0.7, color='steelblue', label='Parts Produced')
            ax1.set_xlabel('Month')
            ax1.set_ylabel('Parts Produced', color='steelblue')
            ax1.tick_params(axis='y', labelcolor='steelblue')
            
            # OEE line
            ax2 = ax1.twinx()
            ax2.plot(months, monthly_data['avg_oee'], color='red', marker='o', linewidth=2, label='Average OEE (%)')
            ax2.set_ylabel('OEE (%)', color='red')
            ax2.tick_params(axis='y', labelcolor='red')
            
            plt.title('Monthly Production and OEE Comparison', fontsize=16, fontweight='bold')
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            img_base64 = self._plt_to_base64()
            plt.close()
            
            return {
                'type': 'combo',
                'title': 'Monthly Production and OEE Comparison',
                'image': img_base64,
                'description': f'Production and efficiency trends across {len(monthly_data)} months'
            }
            
        except Exception as e:
            logging.error(f"Error creating monthly comparison chart: {str(e)}")
            plt.close()
            return None
    
    def _create_shift_comparison_chart(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create shift comparison chart"""
        try:
            shift_data = df.groupby('Shift').agg({
                'total_part_count': 'sum',
                'avg_oee': 'mean',
                'total_part_reject': 'sum'
            }).reset_index()
            
            if shift_data.empty:
                return None
            
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
            
            # Production by shift
            ax1.bar(shift_data['Shift'], shift_data['Part Produced'], color='lightblue')
            ax1.set_title('Production by Shift')
            ax1.set_xlabel('Shift')
            ax1.set_ylabel('Parts Produced')
            
            # OEE by shift
            ax2.bar(shift_data['Shift'], shift_data['OEE (%)'], color='lightgreen')
            ax2.set_title('Average OEE by Shift')
            ax2.set_xlabel('Shift')
            ax2.set_ylabel('OEE (%)')
            
            plt.tight_layout()
            
            img_base64 = self._plt_to_base64()
            plt.close()
            
            return {
                'type': 'bar',
                'title': 'Shift Performance Comparison',
                'image': img_base64,
                'description': f'Performance comparison across {len(shift_data)} shifts'
            }
            
        except Exception as e:
            logging.error(f"Error creating shift comparison chart: {str(e)}")
            plt.close()
            return None
    
    def _create_oee_trend_chart(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create OEE trend over time"""
        try:
            df_clean = df.dropna(subset=['Date', 'OEE (%)'])
            if df_clean.empty:
                return None
            
            # Daily average OEE
            daily_oee = df_clean.groupby(df_clean['Date'].dt.date)['OEE (%)'].mean().reset_index()
            
            plt.figure(figsize=(12, 6))
            plt.plot(daily_oee['Date'], daily_oee['OEE (%)'], marker='o', linewidth=2, markersize=4)
            plt.title('OEE Trend Over Time', fontsize=16, fontweight='bold')
            plt.xlabel('Date')
            plt.ylabel('OEE (%)')
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            img_base64 = self._plt_to_base64()
            plt.close()
            
            return {
                'type': 'line',
                'title': 'OEE Trend Over Time',
                'image': img_base64,
                'description': f'Daily OEE trends over {len(daily_oee)} days'
            }
            
        except Exception as e:
            logging.error(f"Error creating OEE trend chart: {str(e)}")
            plt.close()
            return None
    
    def _create_energy_trend_chart(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create energy consumption trend"""
        try:
            df_clean = df.dropna(subset=['Date', 'Total_energy (KwH)'])
            if df_clean.empty:
                return None
            
            daily_energy = df_clean.groupby(df_clean['Date'].dt.date)['Total_energy (KwH)'].sum().reset_index()
            
            plt.figure(figsize=(12, 6))
            plt.fill_between(daily_energy['Date'], daily_energy['Total_energy (KwH)'], alpha=0.7, color='orange')
            plt.plot(daily_energy['Date'], daily_energy['Total_energy (KwH)'], color='darkorange', linewidth=2)
            plt.title('Energy Consumption Trend', fontsize=16, fontweight='bold')
            plt.xlabel('Date')
            plt.ylabel('Energy (KwH)')
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            img_base64 = self._plt_to_base64()
            plt.close()
            
            return {
                'type': 'area',
                'title': 'Energy Consumption Trend',
                'image': img_base64,
                'description': f'Daily energy consumption over {len(daily_energy)} days'
            }
            
        except Exception as e:
            logging.error(f"Error creating energy trend chart: {str(e)}")
            plt.close()
            return None
    
    def _create_machine_status_chart(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create machine status distribution chart"""
        try:
            status_counts = df['Machine Status'].value_counts()
            
            plt.figure(figsize=(10, 6))
            colors = plt.cm.Set3(range(len(status_counts)))
            bars = plt.bar(status_counts.index, status_counts.values, color=colors)
            plt.title('Machine Status Distribution', fontsize=16, fontweight='bold')
            plt.xlabel('Machine Status')
            plt.ylabel('Count')
            plt.xticks(rotation=45)
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height)}',
                        ha='center', va='bottom')
            
            plt.tight_layout()
            
            img_base64 = self._plt_to_base64()
            plt.close()
            
            return {
                'type': 'bar',
                'title': 'Machine Status Distribution',
                'image': img_base64,
                'description': f'Distribution of machine status across {status_counts.sum()} records'
            }
            
        except Exception as e:
            logging.error(f"Error creating machine status chart: {str(e)}")
            plt.close()
            return None
    
    def _create_cost_trend_chart(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create production cost trend chart"""
        try:
            df_clean = df.dropna(subset=['Date', 'Production Cost'])
            if df_clean.empty:
                return None
            
            daily_cost = df_clean.groupby(df_clean['Date'].dt.date)['Production Cost'].sum().reset_index()
            
            plt.figure(figsize=(12, 6))
            plt.plot(daily_cost['Date'], daily_cost['Production Cost'], 
                    marker='s', linewidth=2, markersize=4, color='purple')
            plt.title('Production Cost Trend', fontsize=16, fontweight='bold')
            plt.xlabel('date')
            plt.ylabel('Production Cost (₹)')
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            img_base64 = self._plt_to_base64()
            plt.close()
            
            return {
                'type': 'line',
                'title': 'Production Cost Trend',
                'image': img_base64,
                'description': f'Daily production costs over {len(daily_cost)} days'
            }
            
        except Exception as e:
            logging.error(f"Error creating cost trend chart: {str(e)}")
            plt.close()
            return None
    
    def _plt_to_base64(self) -> str:
        """Convert matplotlib plot to base64 string"""
        try:
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode()
            buffer.close()
            return image_base64
        except Exception as e:
            logging.error(f"Error converting plot to base64: {str(e)}")
            return ""
    
    def generate_pdf_report(self, content: str, charts: List[Dict[str, Any]]) -> io.BytesIO:
        """Generate PDF report with content and charts"""
        try:
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            styles = getSampleStyleSheet()
            story = []
            
            # Title
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Title'],
                fontSize=18,
                spaceAfter=30,
                textColor=colors.darkblue
            )
            story.append(Paragraph("Manufacturing Data Analysis Report", title_style))
            story.append(Spacer(1, 12))
            
            # Date
            story.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
            story.append(Spacer(1, 12))
            
            # Content
            content_paragraphs = content.split('\n')
            for paragraph in content_paragraphs:
                if paragraph.strip():
                    story.append(Paragraph(paragraph, styles['Normal']))
                    story.append(Spacer(1, 6))
            
            story.append(Spacer(1, 20))
            
            # Charts
            for i, chart in enumerate(charts):
                if chart.get('image'):
                    try:
                        # Create temporary image file
                        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
                        image_data = base64.b64decode(chart['image'])
                        temp_file.write(image_data)
                        temp_file.close()
                        
                        # Add chart title
                        story.append(Paragraph(f"Chart {i+1}: {chart.get('title', 'Chart')}", styles['Heading2']))
                        story.append(Spacer(1, 6))
                        
                        # Add image
                        img = Image(temp_file.name, width=6*inch, height=3.6*inch)
                        story.append(img)
                        story.append(Spacer(1, 6))
                        
                        # Add description
                        if chart.get('description'):
                            story.append(Paragraph(chart['description'], styles['Normal']))
                            story.append(Spacer(1, 12))
                        
                        # Clean up temp file
                        os.unlink(temp_file.name)
                        
                    except Exception as e:
                        logging.error(f"Error adding chart to PDF: {str(e)}")
                        continue
            
            # Build PDF
            doc.build(story)
            buffer.seek(0)
            return buffer
            
        except Exception as e:
            logging.error(f"Error generating PDF report: {str(e)}")
            # Return empty buffer
            buffer = io.BytesIO()
            return buffer
