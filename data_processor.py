import os
import sys
import calendar
import pandas as pd
import logging
from datetime import datetime , date
import glob
import re
from dotenv import load_dotenv
import requests
from azure.data.tables import TableServiceClient
from dateutil.relativedelta import relativedelta
from typing import List
from typing import List, Dict, Any, Optional

load_dotenv() 
storage_conn = os.environ.get("storage_conn")
table_name = os.environ.get("table_name")

class DataProcessor:
    """Handles data loading and processing from machine folders"""
    
    def __init__(self, data_folder='data'):
        self.data_folder = data_folder
        self.ensure_data_folder()
        
    def ensure_data_folder(self):
        """Ensure data folder exists"""
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
            logging.info(f"Created data folder: {self.data_folder}")
    
    #sam
    def sam_get_available_machines(self) -> List[str]:
        """Get list of available machine folders"""
        try:
            url = "https://k2-oee-prod.azurewebsites.net/api/customer_devices?code=HOt2d_jYSqwXuML8FZ-OAK7UlrGnGsbTemJck3HhNHF1AzFuwOK3lQ=="  # get all devices of the customer
            payload = {
                "custID": "G46RK"  # i will get this from UI input 
            }
            response = requests.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            machines = [device["device_name"] for device in data.get("devices", [])]

            return machines
        except Exception as e:
            logging.error(f"Error getting available machines: {str(e)}")
            return []
    
    def get_available_machines(self) -> List[str]:
        """Get list of available machine folders"""
        try:
            machines = []
            if os.path.exists(self.data_folder):
                for item in os.listdir(self.data_folder):
                    item_path = os.path.join(self.data_folder, item)
                    if os.path.isdir(item_path) and item.startswith('M'):
                        machines.append(item)
            
            # Sort machines naturally (M1, M2, M10, etc.)
            machines.sort(key=lambda x: int(re.findall(r'\d+', x)[0]) if re.findall(r'\d+', x) else 0)
            return machines
        except Exception as e:
            logging.error(f"Error getting available machines: {str(e)}")
            return []
    
    def get_machine_files(self, machine: str) -> List[Dict[str, Any]]:
        """Get list of CSV files for a specific machine"""
        try:
            machine_path = os.path.join(self.data_folder, machine)
            if not os.path.exists(machine_path):
                return []
            
            files = []
            csv_files = glob.glob(os.path.join(machine_path, "*.csv"))
            
            for file_path in csv_files:
                filename = os.path.basename(file_path)
                file_info = {
                    'filename': filename,
                    'path': file_path,
                    'size': os.path.getsize(file_path),
                    'modified': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                }
                
                # Try to extract date information from filename
                date_match = re.search(r'([a-zA-Z]+)_(\d{4})', filename)
                if date_match:
                    month, year = date_match.groups()
                    file_info['month'] = month.lower()
                    file_info['year'] = int(year)
                
                files.append(file_info)
            
            # Sort by year and month
            files.sort(key=lambda x: (x.get('year', 0), x.get('month', '')))
            return files
            
        except Exception as e:
            logging.error(f"Error getting files for machine {machine}: {str(e)}")
            return []
    
    def get_machine_data(self, machine: str) -> Dict[str, Any]:
        """Load and return all data for a specific machine"""
        try:
            machine_path = os.path.join(self.data_folder, machine)
            if not os.path.exists(machine_path):
                return {}
            
            data = {
                'machine': machine,
                'files': {},
                'combined_data': None,
                'summary': {}
            }
            
            csv_files = glob.glob(os.path.join(machine_path, "*.csv"))
            all_data_frames = []
            
            for file_path in csv_files:
                try:
                    filename = os.path.basename(file_path)
                    df = pd.read_csv(file_path)
                    
                    # Clean and validate data
                    df = self.clean_dataframe(df)
                    
                    # Store file data
                    data['files'][filename] = {
                        'dataframe': df,
                        'shape': df.shape,
                        'columns': df.columns.tolist(),
                        'date_range': self.get_date_range(df),
                        'summary': self.get_file_summary(df)
                    }
                    
                    all_data_frames.append(df)
                    logging.info(f"Loaded {filename} with shape {df.shape}")
                    
                except Exception as e:
                    logging.error(f"Error loading file {file_path}: {str(e)}")
                    continue
            
            # Combine all data
            if all_data_frames:
                data['combined_data'] = pd.concat(all_data_frames, ignore_index=True)
                data['summary'] = self.get_machine_summary(data['combined_data'])
            
            return data
            
        except Exception as e:
            logging.error(f"Error getting data for machine {machine}: {str(e)}")
            return {}
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize dataframe"""
        try:
            # Convert Date column to datetime if it exists
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
            # Convert numeric columns
            numeric_columns = [
                'Ai_partcount', 
                'avg_apparent_power',                          
                'avg_avail',
                'avg_current',
                'avg_energy_consumption',
                'avg_oee',
                'avg_perf',
                'avg_quality',
                'avg_reactive_power',
                'avg_real_power',
                'avg_total_energy',
                'total_part_count',
                'total_part_reject',
                'total_running',
                'total_off',
                'total_idle',
                'powerUnitCost',
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Remove rows with all NaN values
            df = df.dropna(how='all')
            
            return df
            
        except Exception as e:
            logging.error(f"Error cleaning dataframe: {str(e)}")
            return df
    
    def sam_clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize dataframe"""
        try:
            # Convert Date column to datetime if it exists
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
            # Define allowed numeric columns
            numeric_columns = [
                'Ai_partcount', 
                'avg_apparent_power',                          
                'avg_avail',
                'avg_current',
                'avg_energy_consumption',
                'avg_oee',
                'avg_perf',
                'avg_quality',
                'avg_reactive_power',
                'avg_real_power',
                'avg_total_energy',
                'total_part_count',
                'total_part_reject',
                'total_running',
                'total_off',
                'total_idle',
                'powerUnitCost',
            ]
            
            # Convert to numeric where needed
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Keep only desired columns if present
            keep_columns = ['date'] + [col for col in numeric_columns if col in df.columns]
            df = df[keep_columns]

            # Drop rows that are completely empty
            df = df.dropna(how='all')

            return df

        except Exception as e:
            logging.error(f"Error cleaning dataframe: {str(e)}")
            return df

    def get_date_range(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get date range information from dataframe"""
        try:
            if 'date' in df.columns and not df['date'].isna().all():
                return {
                    'start': df['date'].min().isoformat() if pd.notna(df['date'].min()) else None,
                    'end': df['date'].max().isoformat() if pd.notna(df['date'].max()) else None,
                    'days': len(df['date'].dt.date.unique()) if 'date' in df.columns else 0
                }
            return {'start': None, 'end': None, 'days': 0}
        except Exception:
            return {'start': None, 'end': None, 'days': 0}
    
    def get_file_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics for a file"""
        try:
            summary = {
                'total_records': len(df),
                'total_parts_produced': df['Ai_partcount'].sum() if 'Ai_partcount' in df.columns else 0,
                'total_parts_rejected': df['total_part_reject'].sum() if 'total_part_reject' in df.columns else 0,
                'average_oee': df['avg_oee'].mean() if 'avg_oee' in df.columns else 0,
                'total_energy': df['avg_total_energy'].sum() if 'avg_total_energy' in df.columns else 0,
                # 'total_cost': df['Production Cost'].sum() if 'Production Cost' in df.columns else 0,
                # 'unique_operators': df['Operator Name'].nunique() if 'Operator Name' in df.columns else 0,
                # 'unique_parts': df['Part Name'].nunique() if 'Part Name' in df.columns else 0,
                # 'unique_shifts': df['Shift'].nunique() if 'Shift' in df.columns else 0
            }
            
            # Calculate quality rate
            if summary['total_parts_produced'] > 0:
                summary['quality_rate'] = ((summary['total_parts_produced'] - summary['total_parts_rejected']) / summary['total_parts_produced']) * 100
            else:
                summary['quality_rate'] = 0
            
            return summary
            
        except Exception as e:
            logging.error(f"Error generating file summary: {str(e)}")
            return {}
    
    def sam_get_file_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics for a file"""
        try:
            produced = df['Ai_partcount'].sum() if 'Ai_partcount' in df.columns else 0
            rejected = df['total_part_reject'].sum() if 'total_part_reject' in df.columns else 0
            avg_oee = df['avg_oee'].mean() if 'avg_oee' in df.columns else 0
            total_energy = df['avg_total_energy'].sum() if 'avg_total_energy' in df.columns else 0

            quality_rate = ((produced - rejected) / produced) * 100 if produced > 0 else 0

            summary = {
                'total_records': int(len(df)),
                'total_parts_produced': int(produced),
                'total_parts_rejected': int(rejected),
                'average_oee': round(float(avg_oee), 2),
                'total_energy': round(float(total_energy), 2),
                'quality_rate': round(float(quality_rate), 2)
            }

            return summary

        except Exception as e:
            logging.error(f"Error generating file summary: {str(e)}")
            return {}
    
    def get_machine_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive summary for machine data"""
        try:
            summary = self.get_file_summary(df)
            
            # Add machine status analysis
            if 'Machine Status' in df.columns:
                status_counts = df['Machine Status'].value_counts().to_dict()
                summary['machine_status_breakdown'] = status_counts
            
            # Add maintenance analysis
            if 'Maintenance Flag' in df.columns:
                maintenance_count = df['Maintenance Flag'].sum() if df['Maintenance Flag'].dtype in ['int64', 'float64'] else 0
                summary['maintenance_events'] = maintenance_count
            
            # Add time-based analysis
            if 'date' in df.columns:
                df_with_date = df.dropna(subset=['date'])
                if not df_with_date.empty:
                    summary['date_range'] = self.get_date_range(df_with_date)
                    
                    # Monthly breakdown
                    monthly_data = df_with_date.groupby(df_with_date['date'].dt.to_period('M')).agg({
                        'Ai_partcount': 'sum',
                        'avg_oee': 'mean',
                        'avg_total_energy': 'sum',
                        'energyCost': 'sum'
                    }).to_dict('index')
                    
                    summary['monthly_breakdown'] = {str(k): v for k, v in monthly_data.items()}
            
            return summary
            
        except Exception as e:
            logging.error(f"Error generating machine summary: {str(e)}")
            return {}

    def sam_get_machine_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive summary for machine data"""
        try:
            summary = self.get_file_summary(df)
            summary = {k: (v.item() if hasattr(v, "item") else v) for k, v in summary.items()}

            # Machine status breakdown
            if 'Machine Status' in df.columns:
                status_counts = df['Machine Status'].value_counts().to_dict()
                summary['machine_status_breakdown'] = {
                    k: (v.item() if hasattr(v, "item") else v)
                    for k, v in status_counts.items()
                }

            # Maintenance events
            if 'Maintenance Flag' in df.columns:
                if df['Maintenance Flag'].dtype in ['int64', 'float64', 'bool']:
                    summary['maintenance_events'] = int(df['Maintenance Flag'].sum())
                else:
                    summary['maintenance_events'] = 0

            # Date range + monthly breakdown
            if 'date' in df.columns:
                df_with_date = df.dropna(subset=['date'])
                if not df_with_date.empty:
                    summary['date_range'] = self.get_date_range(df_with_date)

                    monthly_agg = {
                        'Ai_partcount': 'sum',
                        'avg_oee': 'mean',
                        'avg_total_energy': 'sum'
                    }
                    if 'energyCost' in df.columns:
                        monthly_agg['energyCost'] = 'sum'

                    monthly_data = df_with_date.groupby(df_with_date['date'].dt.to_period('M')).agg(monthly_agg).fillna(0)

                    monthly_dict = {
                        str(period): {
                            k: (v.item() if hasattr(v, "item") else v)
                            for k, v in row.items()
                        }
                        for period, row in monthly_data.iterrows()
                    }

                    summary['monthly_breakdown'] = monthly_dict

            return summary

        except Exception as e:
            logging.error(f"Error generating machine summary: {str(e)}")
            return {}

    def filter_by_operator(self, df, operator_name: str):
        if 'Operator Name' in df.columns:
            return df[df['Operator Name'].str.lower() == operator_name.lower()]
        return df

    def get_specific_file_data(self, machine: str, query: str) -> Dict[str, Any]:
        """Get data from specific file based on query date/month, scoped to selected machine only"""
        try:
        # Extract month and year from query
            month_year = self._extract_date_from_query(query)
            
            # if not month_year:
            # # No date found in query — load all data for machine
            #     return self.get_machine_data(machine)

            month, year = month_year
            date_start = date(int(year), month, 1).strftime("%Y-%m-%d")
            date_end = date(int(year), month, calendar.monthrange(int(year), month)[1]).strftime("%Y-%m-%d")
           
            table_client = TableServiceClient.from_connection_string(storage_conn).get_table_client(table_name)
            filter_expr = f"device_name eq '{machine}' and date ge '{date_start}' and date le '{date_end}'"
            entities = list(table_client.query_entities(filter_expr))
            
            rows = [dict(e) for e in entities]
            if not rows:
                logging.info(f"No data found for machine: {machine}")
                return {}

            df = pd.DataFrame(rows)
            df = self.sam_clean_dataframe(df)

            data = {
                'machine': machine,
                'files': {
                    'from_table': {
                        'dataframe': df,
                        'shape': df.shape,
                        'columns': df.columns.tolist(),
                        'date_range': self.get_date_range(df),
                        'summary': self.get_file_summary(df)
                    }
                },
                'combined_data': df,
                'summary': self.sam_get_machine_summary(df)
            }
            
            return data

        except Exception as e:
            logging.error(f"Error getting data for machine '{machine}': {str(e)}")
            return {}

    def _extract_date_from_query(self, query: str) -> tuple:
        """Extract (month_number, year) from query — always returns valid values"""
        try:
            query_lower = query.lower()
            now = datetime.now()

            # Relative time phrases
            if 'this month' in query_lower:
                return (now.month, str(now.year))
            elif 'last month' in query_lower:
                last_month = now - relativedelta(months=1)
                return (last_month.month, str(last_month.year))
            elif 'this year' in query_lower:
                return (1, str(now.year))  # Default to Jan
            elif 'last year' in query_lower:
                return (1, str(now.year - 1))  # Default to Jan

            # Month mapping
            months = {
                'january': 1, 'jan': 1,
                'february': 2, 'feb': 2,
                'march': 3, 'mar': 3,
                'april': 4, 'apr': 4,
                'may': 5,
                'june': 6, 'jun': 6,
                'july': 7, 'jul': 7,
                'august': 8, 'aug': 8,
                'september': 9, 'sep': 9, 'sept': 9,
                'october': 10, 'oct': 10,
                'november': 11, 'nov': 11,
                'december': 12, 'dec': 12
            }

            # Extract month
            month = None
            for key, value in months.items():
                if re.search(rf'\b{key}\b', query_lower):
                    month = value
                    break

            # Extract year
            year_match = re.search(r'20\d{2}', query_lower)
            year = year_match.group() if year_match else str(now.year)

            # If month not found, default to 1 (Jan)
            if not month:
                month = 1

            return (month, year)

        except Exception as e:
            logging.error(f"Error extracting date from query: {str(e)}")
            return (now.month, str(now.year))  # fallback
    def filter_data_by_query(self, df: pd.DataFrame, query: str) -> pd.DataFrame:
        """Filter dataframe based on natural language query"""
        try:
            # Simple filtering based on common query patterns
            query_lower = query.lower()
            filtered_df = df.copy()
            
            # Date filtering
            if 'may' in query_lower and 'Date' in df.columns:
                filtered_df = filtered_df[filtered_df['Date'].dt.month == 5]
            elif 'june' in query_lower and 'Date' in df.columns:
                filtered_df = filtered_df[filtered_df['Date'].dt.month == 6]
            # Add more month filters as needed
            
            # Year filtering
            year_match = re.search(r'20\d{2}', query)
            if year_match and 'Date' in df.columns:
                year = int(year_match.group())
                filtered_df = filtered_df[filtered_df['Date'].dt.year == year]
            
            return filtered_df
            
        except Exception as e:
            logging.error(f"Error filtering data by query: {str(e)}")
            return df
    
    #sam
    def sam_get_machine_data_from_table(self, machine: str) -> Dict[str, Any]:
        """Load and return all data for a specific machine from Azure Table Storage"""
        try:
            storage_conn = os.getenv("storage_table_connection_string")
            table_name = os.getenv("K2hourlyOEEreport_table_name")
            table_client = TableServiceClient.from_connection_string(storage_conn).get_table_client(table_name)

            filter_expr = f"device_name eq '{machine}'"
            entities = table_client.query_entities(filter=filter_expr)

            rows = [dict(e) for e in entities]
            if not rows:
                logging.info(f"No data found for machine: {machine}")
                return {}

            df = pd.DataFrame(rows)
            df = self.clean_dataframe(df)

            data = {
                'machine': machine,
                'files': {
                    'from_table': {
                        'dataframe': df,
                        'shape': df.shape,
                        'columns': df.columns.tolist(),
                        'date_range': self.get_date_range(df),
                        'summary': self.sam_get_file_summary(df)
                    }
                },
                'combined_data': df,
                'summary': self.get_machine_summary(df)
            }

            return data

        except Exception as e:
            logging.error(f"Error getting data for machine '{machine}': {str(e)}")
            return {}
