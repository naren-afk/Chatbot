import os
import json
import logging
from flask import render_template, request, jsonify, send_file, flash
from app import app
import sys
from data_processor import DataProcessor
from llama_client import LlamaClient
from chart_generator import ChartGenerator
import tempfile
import base64
from io import BytesIO

# Initialize components
data_processor = DataProcessor()
llama_client = LlamaClient()
chart_generator = ChartGenerator()

@app.route('/')
def index():
    """Main dashboard page"""
    try:
        # Get available machines
        machines = data_processor.sam_get_available_machines()
        return render_template('index.html', machines=machines)
    except Exception as e:
        logging.error(f"Error loading dashboard: {str(e)}")
        flash(f"Error loading dashboard: {str(e)}", 'error')
        return render_template('index.html', machines=[])

@app.route('/api/machines')
def get_machines():
    """API endpoint to get available machines"""
    try:
        machines = data_processor.sam_get_available_machines()
        return jsonify({'machines': machines})
    except Exception as e:
        logging.error(f"Error fetching machines: {str(e)}")
        return jsonify({'error': str(e)}), 500

# @app.route('/api/machine-files/<machine>')
# def get_machine_files(machine):
#     """API endpoint to get files for a specific machine"""
#     try:
#         files = data_processor.get_machine_files(machine)
#         return jsonify({'files': files})
#     except Exception as e:
#         logging.error(f"Error fetching files for machine {machine}: {str(e)}")
#         return jsonify({'error': str(e)}), 500

@app.route('/api/chat', methods=['POST'])
def chat():
    """Handle chat requests"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        query = data.get('query', '').strip()
        machine = data.get('machine', '').strip()
        
        if not query:
            return jsonify({'error': 'No query provided'}), 400
        
        if not machine:
            return jsonify({'error': 'No machine selected'}), 400
        
        # Process the query
        response = process_chat_query(query, machine)
        return jsonify(response)
        
    except Exception as e:
        logging.error(f"Error processing chat request: {str(e)}")
        return jsonify({'error': f'Error processing request: {str(e)}'}), 500

def process_chat_query(query, machine):
    """Process a natural language query about machine data"""
    try:
        # Get machine data - use specific file if date mentioned in query
        machine_data = data_processor.get_specific_file_data(machine, query)

        if not machine_data:
            return {
                'response': f"No data found for machine {machine}. Please check if the machine folder exists and contains CSV files.",
                'charts': [],
                'type': 'error'
            }
        
        # Analyze the query with Llama
        analysis = llama_client.analyze_query(query, machine, machine_data)
       
        # Generate response text
        response_text = llama_client.generate_response(query, machine, machine_data, analysis)
        
        # Add file-specific context if query was for specific time period
        if machine_data.get('query_specific'):
            requested_file = machine_data.get('requested_file', '')
            response_text = f"**Analysis for {requested_file}:**\n\n{response_text}"
        
        # Generate charts if needed
        charts = []
        if analysis.get('needs_chart', False):
            chart_data = chart_generator.generate_charts(analysis, machine_data)
            charts = chart_data
        
        return {
            'response': response_text,
            'type': 'success',
            'analysis': analysis,
            'charts': charts,
        }
        
    except Exception as e:
        logging.error(f"Error processing query '{query}' for machine '{machine}': {str(e)}")
        return {
            'response': f"I encountered an error while processing your request: {str(e)}",
            'charts': [],
            'type': 'error'
        }

@app.route('/api/export-pdf', methods=['POST'])
def export_pdf():
    """Export chat response as PDF"""
    try:
        data = request.get_json()
        content = data.get('content', '')
        charts = data.get('charts', [])
        
        # Generate PDF
        pdf_buffer = chart_generator.generate_pdf_report(content, charts)
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
        temp_file.write(pdf_buffer.getvalue())
        temp_file.close()
        
        return send_file(temp_file.name, as_attachment=True, download_name='manufacturing_report.pdf')
        
    except Exception as e:
        logging.error(f"Error exporting PDF: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found_error(error):
    return render_template('index.html', machines=[]), 404

@app.errorhandler(500)
def internal_error(error):
    logging.error(f"Internal server error: {str(error)}")
    return render_template('index.html', machines=[]), 500
