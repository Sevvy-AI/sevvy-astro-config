"""
## Currency Conversion Pipeline

This DAG performs currency conversion for trades:
1. Reads trades from base_trades table
2. Reads exchange rates from exchange_rates table
3. Calculates USD notional amounts
4. Writes results to general_ledger_trades table
5. Sends success notification to AWS SQS for anomaly detection

The pipeline processes trades for a specific business date and converts
all notional amounts to USD using the appropriate exchange rates.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
import os
import json


from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests


def send_success_webhook(context, trades_processed, total_usd_notional):
    """
    Send success webhook notification to the monitoring endpoint
    
    Args:
        context: Airflow context containing task instance and other details
        trades_processed: Number of trades successfully processed
        total_usd_notional: Total USD notional amount calculated
    """
    print("🔔 Sending success webhook notification")
    logging.info("Sending success webhook notification")
    
    try:
        # Extract context information
        task_instance = context.get('task_instance')
        dag = context.get('dag')
        execution_date = context.get('execution_date') or context.get('logical_date')
        
        # Prepare webhook payload
        webhook_payload = {
            "event_type": "task_success",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "dag_id": dag.dag_id,
            "task_id": task_instance.task_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "task_id": task_instance.task_id,
                "execution_date": execution_date.isoformat(),
                "try_number": task_instance.try_number,
                "max_tries": task_instance.max_tries,
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/runs/{context.get('dag_run').run_id}/task-instances/{task_instance.task_id}/logs",
                "trades_processed": trades_processed,
                "total_usd_notional": float(total_usd_notional)
            },
            "environment": "production"
        }
        
        print(f"📦 Webhook payload: {json.dumps(webhook_payload, indent=2)}")
        
        # Send webhook request
        response = requests.post(
            "https://655608b6e6d3.ngrok-free.app/api/webhooks/airflow",
            json=webhook_payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"✅ Webhook sent successfully: {response.status_code}")
            logging.info(f"Webhook sent successfully: {response.status_code}")
        else:
            print(f"⚠️ Webhook response: {response.status_code} - {response.text}")
            logging.warning(f"Webhook non-200 response: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ Error sending webhook: {str(e)}")
        logging.error(f"Error sending webhook: {str(e)}")
        # Don't raise exception - webhook failure shouldn't fail the pipeline



# Default arguments for the DAG
default_args = {
    'owner': 'sevvy-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG using TaskFlow API
@dag(
    'currency_conversion_pipeline',
    default_args=default_args,
    description='ETL pipeline for currency conversion of trades',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'finance', 'currency', 'production'],

)
def currency_conversion_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_base_trades(**context) -> List[Dict[str, Any]]:
        """
        Read trades from base_trades table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading trades for business date: {business_date}")
        
        # Get database connection
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        # Query to fetch trades
        query = """
            SELECT business_date, external_trade_id, currency, 
                   notional_original_currency, original_currency
            FROM public.base_trades
            WHERE business_date = %s
        """
        
        # Execute query
        results = pg_hook.get_records(query, parameters=[business_date])
        
        # Convert to list of dictionaries
        trades = []
        for row in results:
            trades.append({
                'business_date': str(row[0]),
                'external_trade_id': row[1],
                'currency': row[2],
                'notional_original_currency': float(row[3]),
                'original_currency': row[4]
            })
        
        print(f"✅ Found {len(trades)} trades for {business_date}")
        logging.info(f"Found {len(trades)} trades for {business_date}")
        
        return trades
    
    @task()
    def read_exchange_rates(**context) -> Dict[str, float]:
        """
        Read exchange rates from exchange_rates table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💱 Reading exchange rates for business date: {business_date}")
        
        # Get database connection
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        # Query to fetch exchange rates
        query = """
            SELECT from_currency, to_currency, rate
            FROM public.exchange_rates
            WHERE business_date = %s AND to_currency = 'USD'
        """
        
        # Execute query
        results = pg_hook.get_records(query, parameters=[business_date])
        
        # Convert to dictionary for easy lookup
        rates = {}
        for row in results:
            from_currency = row[0]
            rate = float(row[2])
            rates[from_currency] = rate
        
        print(f"✅ Found {len(rates)} exchange rates to USD")
        logging.info(f"Found {len(rates)} exchange rates: {rates}")
        
        return rates
    
    @task()
    def calculate_usd_notionals(trades: List[Dict[str, Any]], 
                               exchange_rates: Dict[str, float]) -> List[Dict[str, Any]]:
        """
        Calculate USD notional amounts for all trades
        """
        print(f"🧮 Calculating USD notionals for {len(trades)} trades")
        
        processed_trades = []
        total_usd = 0
        
        for trade in trades:
            original_currency = trade['original_currency']
            notional_original = trade['notional_original_currency']
            
            # Look up exchange rate
            if original_currency in exchange_rates:
                rate = exchange_rates[original_currency]
                notional_usd = notional_original / rate
                
                print(f"  Trade {trade['external_trade_id']}: "
                      f"{notional_original:,.2f} {original_currency} "
                      f"× {rate} = {notional_usd:,.2f} USD")
                
                processed_trades.append({
                    'business_date': trade['business_date'],
                    'external_trade_id': trade['external_trade_id'],
                    'notional_usd': notional_usd
                })
                
                total_usd += notional_usd
            else:
                # Handle missing exchange rate
                logging.warning(f"No exchange rate found for {original_currency}, "
                               f"setting USD notional to 0 for trade {trade['external_trade_id']}")
                
                processed_trades.append({
                    'business_date': trade['business_date'],
                    'external_trade_id': trade['external_trade_id'],
                    'notional_usd': 0
                })
        
        print(f"✅ Calculated USD notionals. Total: ${total_usd:,.2f}")
        logging.info(f"Processed {len(processed_trades)} trades with total USD: {total_usd}")
        
        return processed_trades
    
    @task()
    def write_to_general_ledger(processed_trades: List[Dict[str, Any]], **context):
        """
        Write calculated USD notionals to general_ledger_trades table
        """
        print(f"💾 Writing {len(processed_trades)} trades to general_ledger_trades")
        
        if not processed_trades:
            print("⚠️  No trades to write")
            return
        
        # Get database connection
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        # Update query
        update_query = """
            UPDATE public.general_ledger_trades
            SET notional_usd = %s
            WHERE business_date = %s AND external_trade_id = %s
        """
        
        # Execute updates in a transaction
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            trades_updated = 0
            total_usd_notional = 0
            
            for trade in processed_trades:
                cursor.execute(update_query, (
                    trade['notional_usd'],
                    trade['business_date'],
                    trade['external_trade_id']
                ))
                
                if cursor.rowcount > 0:
                    trades_updated += 1
                    total_usd_notional += trade['notional_usd']
                else:
                    logging.warning(f"No matching record found for trade {trade['external_trade_id']}")
            
            # Commit the transaction
            conn.commit()
            
            print(f"✅ Successfully updated {trades_updated} trades in general_ledger_trades")
            logging.info(f"Updated {trades_updated} trades with total USD notional: {total_usd_notional}")
            
            # Push statistics to XCom for success callback
            context['ti'].xcom_push(key='trades_processed', value=trades_updated)
            context['ti'].xcom_push(key='total_usd_notional', value=total_usd_notional)
            context['ti'].xcom_push(key='business_date', value=processed_trades[0]['business_date'] if processed_trades else None)
            
            # Send success webhook notification
            send_success_webhook(context, trades_updated, total_usd_notional)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error updating general_ledger_trades: {str(e)}")
            logging.error(f"Database update failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Define task dependencies
    trades = read_base_trades()
    rates = read_exchange_rates()
    processed = calculate_usd_notionals(trades, rates)
    write_to_general_ledger(processed)

# Instantiate the DAG
dag = currency_conversion_pipeline()