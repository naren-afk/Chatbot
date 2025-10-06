import logging
import json
import re
from typing import Dict, Any, List
import subprocess
import os
import requests

class LlamaClient:
    """Client for interacting with local Vocalis model via LM Studio or llama-cpp"""

    def __init__(self):
        self.model_path = os.getenv("LLAMA_MODEL_PATH", "vocalis")  # Name used in LM Studio
        self.max_tokens = 2048
        
    def analyze_query(self, query: str, machine: str, machine_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze user query to determine intent and required actions"""
        try:
            # Prepare context about available data
            context = self._prepare_data_context(machine_data)
            
            analysis_prompt = f"""
            Analyze this manufacturing data query and provide a structured response.
            
            Machine: {machine}
            Available data: {context}
            
            User query: "{query}"
            
            Please analyze and respond with JSON format:
            {{
                "intent": "summary|comparison|trend|specific_metric|report",
                "time_period": "specific dates or periods mentioned",
                "metrics": ["list of specific metrics requested"],
                "needs_chart": true/false,
                "chart_types": ["bar", "line", "pie", "comparison"],
                "analysis_type": "descriptive analysis needed"
            }}
            """
            
            response = self._call_llama(analysis_prompt)
            
            # Try to parse JSON response, fallback to basic analysis
            try:
                analysis = json.loads(response)
            except:
                analysis = self._fallback_analysis(query)
            
            return analysis
            
        except Exception as e:
            logging.error(f"Error analyzing query: {str(e)}")
            return self._fallback_analysis(query)
    
    def generate_response(self, query: str, machine: str, machine_data: Dict[str, Any], analysis: Dict[str, Any]) -> str:
        """Generate natural language response based on data and analysis"""
        try:
            # Prepare data summary
            data_summary = self._prepare_data_summary(machine_data)
            
            response_prompt = f"""
            You are a manufacturing data analyst AI assistant. Generate a comprehensive response to the user's query about machine {machine}.
            
            User Query: "{query}"
            
            Analysis Context: {json.dumps(analysis, indent=2)}
            
            Data Summary: {data_summary}
            
            Instructions:
            1. Provide a clear, professional response
            2. Include specific numbers and insights from the data
            3. Mention key performance indicators (OEE, production rates, quality metrics)
            4. Highlight any notable trends or patterns
            5. Keep the response informative but concise
            6. Use manufacturing terminology appropriately
            
            Response:
            """
            
            response = self._call_llama(response_prompt)
            return response.strip()
            
        except Exception as e:
            logging.error(f"Error generating response: {str(e)}")
            return self._fallback_response(query, machine, machine_data)
    
    def _call_llama(self, prompt: str) -> str:
        """Call the locally running Vocalis model (via LM Studio API or fallback methods)"""
        try:
            # Primary: LM Studio HTTP API
            try:
                response = requests.post("http://192.168.56.1:1234/v1/completions", json={
                    "prompt": prompt,
                    "max_tokens": self.max_tokens,
                    "temperature": 0.7,
                    "model": self.model_path
                }, timeout=60)
                if response.status_code == 200:
                    return response.json()['choices'][0]['text']
                else:
                    logging.warning(f"LM Studio returned error: {response.text}")
            except Exception as e:
                logging.warning(f"LM Studio call failed: {str(e)}")

            # Secondary: Try direct llama-cpp-python
            try:
                result = subprocess.run([
                    'python3', '-c',
                    f'''
import sys
try:
    from llama_cpp import Llama
    model = Llama(model_path="./Vocalis-q4_k_m.gguf", verbose=False)
    response = model("""{prompt}""", max_tokens=800, temperature=0.7)
    print(response["choices"][0]["text"])
except Exception as e:
    print(f"ERROR: {{e}}")
'''
                ], capture_output=True, text=True, timeout=45)

                if result.returncode == 0 and result.stdout.strip() and not result.stdout.startswith('ERROR:'):
                    logging.info("Used llama-cpp-python Vocalis for response")
                    return result.stdout.strip()
            except Exception as e:
                logging.warning(f"llama-cpp-python error: {str(e)}")

            # Tertiary: Try alternative model paths
            for model_path in ["./models/Vocalis-q4_k_m.gguf", "./Vocalis-q4_k_m.gguf", "vocalis"]:
                try:
                    result = subprocess.run([
                        'ollama', 'run', model_path, prompt
                    ], capture_output=True, text=True, timeout=30)

                    if result.returncode == 0 and result.stdout.strip():
                        logging.info(f"Used fallback model path: {model_path}")
                        return result.stdout.strip()
                except Exception:
                    continue

            logging.info("Falling back to rule-based response system")
            return self._rule_based_response(prompt)

        except Exception as e:
            logging.error(f"Error in Llama integration: {str(e)}")
            return self._rule_based_response(prompt)

    def _rule_based_response(self, prompt: str) -> str:
        """Generate intelligent rule-based response when AI model is unavailable"""
        prompt_lower = prompt.lower()
        
        # Analyze for data requests
        if "analyze" in prompt_lower and "json" in prompt_lower:
            # This is an analysis request
            if any(word in prompt_lower for word in ['summary', 'overview', 'report']):
                return json.dumps({
                    "intent": "summary",
                    "time_period": "current",
                    "metrics": ["OEE", "Production", "Quality", "Energy"],
                    "needs_chart": True,
                    "chart_types": ["bar", "pie", "line"],
                    "files_needed": [],
                    "analysis_type": "comprehensive_summary"
                })
            elif any(word in prompt_lower for word in ['compare', 'comparison', 'vs']):
                return json.dumps({
                    "intent": "comparison",
                    "time_period": "multi_period",
                    "metrics": ["OEE", "Production", "Quality"],
                    "needs_chart": True,
                    "chart_types": ["bar", "line", "comparison"],
                    "files_needed": [],
                    "analysis_type": "comparative_analysis"
                })
            elif any(word in prompt_lower for word in ['trend', 'over time']):
                return json.dumps({
                    "intent": "trend",
                    "time_period": "time_series",
                    "metrics": ["OEE", "Energy", "Production"],
                    "needs_chart": True,
                    "chart_types": ["line", "area"],
                    "files_needed": [],
                    "analysis_type": "trend_analysis"
                })
            else:
                return json.dumps({
                    "intent": "specific_metric",
                    "time_period": "current",
                    "metrics": ["OEE", "Production"],
                    "needs_chart": True,
                    "chart_types": ["bar"],
                    "files_needed": [],
                    "analysis_type": "metric_analysis"
                })
        
        # Generate natural language responses
    
    
    def _prepare_data_context(self, machine_data: Dict[str, Any]) -> str:
        """Prepare context about available data"""
        try:
            if not machine_data or 'files' not in machine_data:
                return "No data available"
            
            files_info = []
            for filename, file_data in machine_data['files'].items():
                files_info.append(f"{filename}: {file_data['shape'][0]} records")
            
            return f"Files: {', '.join(files_info)}"
            
        except Exception:
            return "Data context unavailable"
    
    def _prepare_data_summary(self, machine_data: Dict[str, Any]) -> str:
        """Prepare data summary for response generation"""
        try:
            if not machine_data or 'summary' not in machine_data:
                return "No summary available"
            
            summary = machine_data['summary']
            summary_text = f"""
            Total Records: {summary.get('total_records', 0)}
            Parts Produced: {summary.get('total_parts_produced', 0)}
            Parts Rejected: {summary.get('total_parts_rejected', 0)}
            Average OEE: {summary.get('average_oee', 0):.2f}%
            Quality Rate: {summary.get('quality_rate', 0):.2f}%
            Total Energy: {summary.get('total_energy', 0):.2f} KwH
            Total Cost: ₹{summary.get('total_cost', 0):.2f}
            """
            
            return summary_text
            
        except Exception:
            return "Summary unavailable"
    
    def _fallback_analysis(self, query: str) -> Dict[str, Any]:
        """Fallback analysis when AI model fails"""
        query_lower = query.lower()
        
        # Determine intent
        if any(word in query_lower for word in ['summary', 'overview', 'report']):
            intent = "summary"
        elif any(word in query_lower for word in ['compare', 'comparison', 'vs', 'versus']):
            intent = "comparison"
        elif any(word in query_lower for word in ['trend', 'over time', 'change']):
            intent = "trend"
        else:
            intent = "specific_metric"
        
        # Determine if charts are needed
        needs_chart = any(word in query_lower for word in ['chart', 'graph', 'visual', 'plot', 'show'])
        
        return {
            "intent": intent,
            "time_period": "all",
            "metrics": ["OEE", "Production", "Quality"],
            "needs_chart": needs_chart,
            "chart_types": ["bar", "line"],
            "analysis_type": "basic_analysis"
        }
    
    def _fallback_response(self, query: str, machine: str, machine_data: Dict[str, Any]) -> str:
        """Generate comprehensive fallback response when AI model fails"""
        try:
            if not machine_data or 'summary' not in machine_data:
                return f"No data available for machine {machine}. Please ensure the machine folder contains CSV files."
            
            summary = machine_data['summary']
            query_lower = query.lower()
            
            # Generate detailed report based on query type
            if any(word in query_lower for word in ['summary', 'overview', 'report']):
                return self._generate_comprehensive_report(machine, summary, machine_data)
            elif any(word in query_lower for word in ['compare', 'comparison']):
                return self._generate_comparison_report(machine, summary, machine_data)
            elif 'quality' in query_lower:
                return self._generate_quality_report(machine, summary)
            elif 'oee' in query_lower:
                return self._generate_oee_report(machine, summary)
            elif 'energy' in query_lower:
                return self._generate_energy_report(machine, summary)
            elif 'cost' in query_lower:
                return self._generate_cost_report(machine, summary)
            else:
                return self._generate_basic_report(machine, summary)
                
        except Exception as e:
            logging.error(f"Error generating fallback response: {str(e)}")
            return f"Unable to process data for machine {machine}. Please check the data files and try again."
    
    def _generate_comprehensive_report(self, machine: str, summary: Dict[str, Any], machine_data: Dict[str, Any]) -> str:
        """Generate a comprehensive manufacturing report"""
        total_produced = summary.get('total_parts_produced', 0)
        total_rejected = summary.get('total_parts_rejected', 0)
        quality_rate = summary.get('quality_rate', 0)
        avg_oee = summary.get('average_oee', 0)
        total_energy = summary.get('total_energy', 0)
        total_cost = summary.get('total_cost', 0)
        
        # Calculate additional metrics
        efficiency_rating = "Excellent" if avg_oee >= 85 else "Good" if avg_oee >= 75 else "Needs Improvement"
        quality_rating = "Excellent" if quality_rate >= 95 else "Good" if quality_rate >= 90 else "Needs Attention"
        
        report = f"""
**Manufacturing Analytics Report - Machine {machine}**

**Executive Summary:**
Machine {machine} has processed {summary.get('total_records', 0)} production records showing overall operational performance. The analysis reveals key insights into production efficiency, quality control, and resource utilization.

**Production Performance:**
• Total Parts Produced: {total_produced:,} units
• Parts Rejected: {total_rejected:,} units ({(total_rejected/max(total_produced,1)*100):.1f}% rejection rate)
• Quality Rate: {quality_rate:.1f}% ({quality_rating})
• Net Good Production: {total_produced - total_rejected:,} units

**Operational Efficiency:**
• Average OEE: {avg_oee:.1f}% ({efficiency_rating})
• Energy Consumption: {total_energy:.1f} KwH
• Energy per Unit: {(total_energy/max(total_produced,1)):.2f} KwH/part
• Production Cost: ₹{total_cost:,.2f}
• Cost per Unit: ₹{(total_cost/max(total_produced,1)):.2f}/part

**Workforce Analysis:**
• Number of Operators: {summary.get('unique_operators', 0)}
• Shift Coverage: {summary.get('unique_shifts', 0)} shifts
• Product Varieties: {summary.get('unique_parts', 0)} different parts

**Detailed Data Analysis:**
"""
        
        # Add detailed analysis of actual data
        report += self._generate_detailed_data_analysis(machine_data)
        
        report += "\n**Operational Recommendations:**\n"
        
        # Add recommendations based on performance
        if avg_oee < 75:
            report += "• Priority: Improve OEE through maintenance optimization and downtime reduction\n"
        if quality_rate < 90:
            report += "• Priority: Implement quality control measures to reduce rejection rates\n"
        if total_energy/max(total_produced,1) > 1.0:
            report += "• Consider energy efficiency improvements to reduce power consumption per unit\n"
        
        report += "\n**Machine Status Distribution:**\n"
        if 'machine_status_breakdown' in summary:
            for status, count in summary['machine_status_breakdown'].items():
                percentage = (count / summary.get('total_records', 1)) * 100
                report += f"• {status}: {count} records ({percentage:.1f}%)\n"
        
        if 'monthly_breakdown' in summary and summary['monthly_breakdown']:
            report += "\n**Monthly Performance Trends:**\n"
            for month, data in summary['monthly_breakdown'].items():
                month_production = data.get('Part Produced', 0)
                month_oee = data.get('OEE (%)', 0)
                report += f"• {month}: {month_production:,} parts, {month_oee:.1f}% OEE\n"
        
        return report.strip()
    
    def _generate_detailed_data_analysis(self, machine_data: Dict[str, Any]) -> str:
        """Generate detailed analysis of the actual data content"""
        analysis = ""
        
        if not machine_data or 'files' not in machine_data:
            return "No detailed data available for analysis.\n"
        
        # Analyze each file
        for filename, file_data in machine_data['files'].items():
            if 'dataframe' not in file_data:
                continue
                
            df = file_data['dataframe']
            analysis += f"\n**File Analysis: {filename}**\n"
            
            # Date range analysis
            if 'Date' in df.columns:
                date_range = file_data.get('date_range', {})
                start_date = date_range.get('start', 'Unknown')
                end_date = date_range.get('end', 'Unknown')
                days = date_range.get('days', 0)
                analysis += f"• Data Period: {start_date} to {end_date} ({days} days)\n"
            
            # Operator analysis
            if 'Operator Name' in df.columns:
                operators = df['Operator Name'].unique()
                analysis += f"• Operators: {', '.join(operators)} ({len(operators)} total)\n"
                
                # Most productive operator
                if 'Part Produced' in df.columns:
                    operator_production = df.groupby('Operator Name')['Part Produced'].sum()
                    top_operator = operator_production.idxmax()
                    top_production = operator_production.max()
                    analysis += f"• Top Producer: {top_operator} ({top_production:,} parts)\n"
            
            # Part type analysis
            if 'Part Name' in df.columns:
                parts = df['Part Name'].unique()
                analysis += f"• Part Types: {', '.join(parts)} ({len(parts)} varieties)\n"
                
                if 'Part Produced' in df.columns:
                    part_production = df.groupby('Part Name')['Part Produced'].sum()
                    top_part = part_production.idxmax()
                    top_part_qty = part_production.max()
                    analysis += f"• Most Produced Part: {top_part} ({top_part_qty:,} units)\n"
            
            # Shift analysis
            if 'Shift' in df.columns:
                shifts = df['Shift'].unique()
                analysis += f"• Shifts: {', '.join(shifts)}\n"
                
                if 'Part Produced' in df.columns:
                    shift_production = df.groupby('Shift')['Part Produced'].sum()
                    best_shift = shift_production.idxmax()
                    best_shift_qty = shift_production.max()
                    analysis += f"• Most Productive Shift: {best_shift} ({best_shift_qty:,} parts)\n"
            
            # Performance metrics
            if 'OEE (%)' in df.columns:
                max_oee = df['OEE (%)'].max()
                min_oee = df['OEE (%)'].min()
                analysis += f"• OEE Range: {min_oee:.1f}% to {max_oee:.1f}%\n"
            
            # Quality insights
            if 'Part Produced' in df.columns and 'Part Rejected' in df.columns:
                total_produced = df['Part Produced'].sum()
                total_rejected = df['Part Rejected'].sum()
                if total_produced > 0:
                    file_quality_rate = ((total_produced - total_rejected) / total_produced) * 100
                    analysis += f"• Quality Rate: {file_quality_rate:.1f}% ({total_rejected:,} rejected out of {total_produced:,})\n"
            
            # Maintenance events
            if 'Maintenance Flag' in df.columns:
                maintenance_events = df['Maintenance Flag'].sum() if df['Maintenance Flag'].dtype in ['int64', 'float64'] else 0
                if maintenance_events > 0:
                    analysis += f"• Maintenance Events: {maintenance_events} recorded\n"
            
            analysis += "\n"
        
        return analysis
    
    def _generate_comparison_report(self, machine: str, summary: Dict[str, Any], machine_data: Dict[str, Any]) -> str:
        """Generate comparison analysis report"""
        if 'monthly_breakdown' not in summary or len(summary['monthly_breakdown']) < 2:
            return self._generate_basic_report(machine, summary)
        
        monthly_data = summary['monthly_breakdown']
        months = list(monthly_data.keys())
        
        report = f"""
**Comparative Analysis Report - Machine {machine}**

**Period Comparison Overview:**
Analyzing performance across {len(months)} time periods to identify trends and variations in manufacturing efficiency.

**Production Comparison:**
"""
        
        for i, (month, data) in enumerate(monthly_data.items()):
            production = data.get('Part Produced', 0)
            oee = data.get('OEE (%)', 0)
            energy = data.get('Total_energy (KwH)', 0)
            
            if i > 0:
                prev_month, prev_data = list(monthly_data.items())[i-1]
                prod_change = ((production - prev_data.get('Part Produced', 0)) / max(prev_data.get('Part Produced', 1), 1)) * 100
                oee_change = oee - prev_data.get('OEE (%)', 0)
                
                report += f"• {month}: {production:,} parts ({prod_change:+.1f}% vs {prev_month}), OEE: {oee:.1f}% ({oee_change:+.1f}%)\n"
            else:
                report += f"• {month}: {production:,} parts, OEE: {oee:.1f}%\n"
        
        # Find best and worst performing periods
        best_month = max(monthly_data.items(), key=lambda x: x[1].get('OEE (%)', 0))
        worst_month = min(monthly_data.items(), key=lambda x: x[1].get('OEE (%)', 0))
        
        report += f"""
**Key Insights:**
• Best Performance: {best_month[0]} with {best_month[1].get('OEE (%)', 0):.1f}% OEE
• Lowest Performance: {worst_month[0]} with {worst_month[1].get('OEE (%)', 0):.1f}% OEE
• Performance Variation: {best_month[1].get('OEE (%)', 0) - worst_month[1].get('OEE (%)', 0):.1f}% OEE difference

**Trend Analysis:**
The data shows {'consistent' if (best_month[1].get('OEE (%)', 0) - worst_month[1].get('OEE (%)', 0)) < 10 else 'variable'} performance across time periods, indicating {'stable operations' if (best_month[1].get('OEE (%)', 0) - worst_month[1].get('OEE (%)', 0)) < 10 else 'opportunities for process optimization'}.
"""
        
        return report.strip()
    
    def _generate_quality_report(self, machine: str, summary: Dict[str, Any]) -> str:
        """Generate quality-focused report"""
        total_produced = summary.get('total_parts_produced', 0)
        total_rejected = summary.get('total_parts_rejected', 0)
        quality_rate = summary.get('quality_rate', 0)
        
        return f"""
**Quality Analysis Report - Machine {machine}**

**Quality Performance Summary:**
• Total Production: {total_produced:,} parts
• Rejected Parts: {total_rejected:,} parts
• Quality Rate: {quality_rate:.2f}%
• Defect Rate: {100-quality_rate:.2f}%

**Quality Assessment:**
{self._get_quality_assessment(quality_rate)}

**Quality Recommendations:**
{self._get_quality_recommendations(quality_rate, total_rejected)}
"""
    
    def _generate_oee_report(self, machine: str, summary: Dict[str, Any]) -> str:
        """Generate OEE-focused report"""
        avg_oee = summary.get('average_oee', 0)
        
        return f"""
**Overall Equipment Effectiveness (OEE) Report - Machine {machine}**

**OEE Performance:**
• Current OEE: {avg_oee:.1f}%
• Industry Benchmark: 85% (World Class)
• Performance Gap: {85 - avg_oee:.1f}%

**OEE Analysis:**
{self._get_oee_analysis(avg_oee)}

**Improvement Opportunities:**
{self._get_oee_recommendations(avg_oee)}
"""
    
    def _generate_energy_report(self, machine: str, summary: Dict[str, Any]) -> str:
        """Generate energy consumption report"""
        total_energy = summary.get('total_energy', 0)
        total_produced = summary.get('total_parts_produced', 0)
        energy_per_unit = total_energy / max(total_produced, 1)
        
        return f"""
**Energy Consumption Report - Machine {machine}**

**Energy Usage Summary:**
• Total Energy Consumed: {total_energy:.1f} KwH
• Total Parts Produced: {total_produced:,} units
• Energy Efficiency: {energy_per_unit:.3f} KwH per part
• Estimated Energy Cost: ₹{total_energy * 0.12:.2f} (@ ₹0.12/KwH)

**Energy Performance Analysis:**
{self._get_energy_analysis(energy_per_unit)}
"""
    
    def _generate_cost_report(self, machine: str, summary: Dict[str, Any]) -> str:
        """Generate cost analysis report"""
        total_cost = summary.get('total_cost', 0)
        total_produced = summary.get('total_parts_produced', 0)
        cost_per_unit = total_cost / max(total_produced, 1)
        
        return f"""
**Production Cost Analysis - Machine {machine}**

**Cost Performance:**
• Total Production Cost: ₹{total_cost:,.2f}
• Total Units Produced: {total_produced:,}
• Cost per Unit: ₹{cost_per_unit:.2f}
• Average Daily Cost: ₹{total_cost / max(summary.get('total_records', 1), 1):.2f}

**Cost Efficiency Analysis:**
{self._get_cost_analysis(cost_per_unit)}
"""
    
    def _generate_basic_report(self, machine: str, summary: Dict[str, Any]) -> str:
        """Generate basic summary report"""
        return f"""
**Manufacturing Summary - Machine {machine}**

**Key Metrics:**
• Total Production: {summary.get('total_parts_produced', 0):,} parts
• Quality Rate: {summary.get('quality_rate', 0):.1f}%
• Average OEE: {summary.get('average_oee', 0):.1f}%
• Energy Consumption: {summary.get('total_energy', 0):.1f} KwH
• Total Cost: ₹{summary.get('total_cost', 0):,.2f}

**Operational Overview:**
The machine has processed {summary.get('total_records', 0)} production records with {summary.get('unique_operators', 0)} operators across {summary.get('unique_shifts', 0)} shifts, producing {summary.get('unique_parts', 0)} different part types.
"""
    
    def _get_quality_assessment(self, quality_rate: float) -> str:
        """Get quality performance assessment"""
        if quality_rate >= 99:
            return "Exceptional quality performance - exceeding industry standards"
        elif quality_rate >= 95:
            return "Excellent quality control - meeting high-performance benchmarks"
        elif quality_rate >= 90:
            return "Good quality performance - within acceptable manufacturing standards"
        elif quality_rate >= 85:
            return "Average quality performance - room for improvement"
        else:
            return "Below standard quality performance - immediate attention required"
    
    def _get_quality_recommendations(self, quality_rate: float, rejected_parts: int) -> str:
        """Get quality improvement recommendations"""
        if quality_rate < 90:
            return "• Implement enhanced quality control procedures\n• Review and optimize manufacturing processes\n• Increase inspection frequency\n• Provide additional operator training"
        elif quality_rate < 95:
            return "• Fine-tune process parameters\n• Implement preventive maintenance schedule\n• Monitor critical quality points"
        else:
            return "• Maintain current quality standards\n• Continue monitoring for consistent performance\n• Share best practices across other machines"
    
    def _get_oee_analysis(self, oee: float) -> str:
        """Get OEE performance analysis"""
        if oee >= 85:
            return "World-class OEE performance - excellent operational efficiency"
        elif oee >= 75:
            return "Good OEE performance - above average manufacturing efficiency"
        elif oee >= 65:
            return "Average OEE performance - typical for manufacturing operations"
        else:
            return "Below average OEE performance - significant improvement opportunities"
    
    def _get_oee_recommendations(self, oee: float) -> str:
        """Get OEE improvement recommendations"""
        if oee < 65:
            return "• Focus on reducing planned and unplanned downtime\n• Optimize changeover procedures\n• Implement predictive maintenance\n• Address quality issues to reduce rework"
        elif oee < 75:
            return "• Reduce minor stops and speed losses\n• Improve operator efficiency\n• Optimize maintenance scheduling"
        else:
            return "• Fine-tune performance parameters\n• Implement continuous improvement practices\n• Benchmark against best-in-class operations"
    
    def _get_energy_analysis(self, energy_per_unit: float) -> str:
        """Get energy efficiency analysis"""
        if energy_per_unit < 0.8:
            return "Excellent energy efficiency - well below industry averages"
        elif energy_per_unit < 1.2:
            return "Good energy performance - within efficient operating range"
        elif energy_per_unit < 1.8:
            return "Average energy consumption - opportunities for improvement"
        else:
            return "High energy consumption - immediate efficiency improvements needed"
    
    def _get_cost_analysis(self, cost_per_unit: float) -> str:
        """Get cost performance analysis"""
        if cost_per_unit < 5.0:
            return "Excellent cost efficiency - highly competitive production costs"
        elif cost_per_unit < 10.0:
            return "Good cost performance - within competitive manufacturing range"
        elif cost_per_unit < 15.0:
            return "Average cost performance - room for optimization"
        else:
            return "High production costs - cost reduction initiatives recommended"
