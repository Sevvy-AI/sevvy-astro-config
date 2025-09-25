"""
## ETF Holdings Pipeline

This DAG calculates daily ETF holdings values:
1. Reads ETF market data (prices) from etf_market_data table
2. Reads exchange rates from exchange_rates table
3. Reads ETF trades (shares) from etf_trades table
4. Calculates USD prices for all ETFs
5. Calculates holdings values (price × shares)
6. Writes results to etf_holdings table
7. Sends success notification webhook

The pipeline processes data for a specific business date and calculates
the total value of ETF holdings in USD.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


def send_success_webhook(context):
    """
    Send success webhook notification to the monitoring endpoint (DAG-level callback)
    
    Args:
        context: Airflow context containing DAG run and task instance details
    """
    print("🔔 Sending DAG success webhook notification")
    logging.info("Sending DAG success webhook notification")
    
    try:
        # Extract context information
        dag_run = context.get('dag_run')
        dag = context.get('dag')
        execution_date = context.get('execution_date') or context.get('logical_date')
        
        # Get processed data statistics from XCom
        task_instance = context.get('task_instance')
        ti = dag_run.get_task_instance('write_etf_holdings')
        holdings_processed = ti.xcom_pull(key='holdings_processed') or 0
        total_holdings_value = ti.xcom_pull(key='total_holdings_value') or 0
        business_date = ti.xcom_pull(key='business_date') or str(execution_date.date())
        
        # Prepare webhook payload
        webhook_payload = {
            "event_type": "dag_success",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "dag_id": dag.dag_id,
            "run_id": dag_run.run_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "execution_date": execution_date.isoformat(),
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/grid",
                "holdings_processed": holdings_processed,
                "total_holdings_value": float(total_holdings_value),
                "business_date": business_date
            },
            "environment": "production"
        }
        
        print(f"📦 DAG Success Webhook payload: {json.dumps(webhook_payload, indent=2)}")
        
        # Send webhook request
        response = requests.post(
            "https://655608b6e6d3.ngrok-free.app/api/webhooks/airflow",
            json=webhook_payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"✅ DAG Success webhook sent successfully: {response.status_code}")
            logging.info(f"DAG Success webhook sent successfully: {response.status_code}")
        else:
            print(f"⚠️ DAG Success webhook response: {response.status_code} - {response.text}")
            logging.warning(f"DAG Success webhook non-200 response: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ Error sending DAG success webhook: {str(e)}")
        logging.error(f"Error sending DAG success webhook: {str(e)}")
        # Don't raise exception - webhook failure shouldn't fail the pipeline


# Default arguments for the DAG
default_args = {
    'owner': 'sevvy-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG using TaskFlow API
@dag(
    'etf_holdings_pipeline',
    default_args=default_args,
    description='ETL pipeline for calculating ETF holdings values in USD',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'finance', 'etf', 'production'],
    on_success_callback=send_success_webhook,
)
def etf_holdings_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_etf_market_data(**context) -> List[Dict[str, Any]]:
        """
        Read ETF market data (prices) from etf_market_data table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading ETF market data for business date: {business_date}")
        
        # Get database connection
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        # Query to fetch ETF prices
        query = """
            SELECT price_date, symbol, price, currency
            FROM public.etf_market_data
            WHERE price_date = %s
            ORDER BY symbol
        """
        
        # Execute query
        results = pg_hook.get_records(query, parameters=[business_date])
        
        # Convert to list of dictionaries
        market_data = []
        for row in results:
            market_data.append({
                'price_date': str(row[0]),
                'symbol': row[1],
                'price': float(row[2]),
                'currency': row[3]
            })
        
        print(f"✅ Found {len(market_data)} ETF prices for {business_date}")
        logging.info(f"Found {len(market_data)} ETF prices for {business_date}")
        
        return market_data
    
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
    def read_etf_trades(**context) -> List[Dict[str, Any]]:
        """
        Read ETF trades (shares) from etf_trades table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📈 Reading ETF trades for business date: {business_date}")
        
        # Get database connection
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        # Query to fetch ETF trades
        query = """
            SELECT trade_id, trade_date, etf_symbol, shares
            FROM public.etf_trades
            WHERE trade_date = %s
            ORDER BY etf_symbol
        """
        
        # Execute query
        results = pg_hook.get_records(query, parameters=[business_date])
        
        # Convert to list of dictionaries
        trades = []
        for row in results:
            trades.append({
                'trade_id': row[0],
                'trade_date': str(row[1]),
                'etf_symbol': row[2],
                'shares': float(row[3])
            })
        
        print(f"✅ Found {len(trades)} ETF trades for {business_date}")
        logging.info(f"Found {len(trades)} ETF trades for {business_date}")
        
        return trades
    
    @task()
    def calculate_usd_prices(market_data: List[Dict[str, Any]], 
                            exchange_rates: Dict[str, float]) -> List[Dict[str, Any]]:
        """
        Calculate USD prices for all ETFs
        """
        print(f"🧮 Calculating USD prices for {len(market_data)} ETFs")
        
        usd_prices = []
        
        for etf in market_data:
            currency = etf['currency']
            original_price = etf['price']
            
            # Calculate USD price
            if currency == 'USD':
                usd_price = original_price
            elif currency in exchange_rates:
                rate = exchange_rates[currency]
                usd_price = original_price * rate
                print(f"  ETF {etf['symbol']}: "
                      f"{original_price:,.2f} {currency} "
                      f"× {rate} = {usd_price:,.2f} USD")
            else:
                # Handle missing exchange rate
                logging.warning(f"No exchange rate found for {currency}, "
                               f"setting USD price to 0 for ETF {etf['symbol']}")
                usd_price = 0
            
            usd_prices.append({
                'price_date': etf['price_date'],
                'symbol': etf['symbol'],
                'original_price': original_price,
                'original_currency': currency,
                'usd_price': usd_price
            })
        
        print(f"✅ Calculated USD prices for {len(usd_prices)} ETFs")
        logging.info(f"Calculated USD prices for {len(usd_prices)} ETFs")
        
        return usd_prices
    
    @task()
    def calculate_holdings(usd_prices: List[Dict[str, Any]], 
                          trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate holdings values by joining prices with trades
        """
        print(f"💰 Calculating holdings values")
        
        # Create price lookup dictionary
        price_lookup = {price['symbol']: price['usd_price'] 
                       for price in usd_prices}
        
        # Calculate holdings
        holdings = []
        total_value = 0
        
        for trade in trades:
            symbol = trade['etf_symbol']
            shares = trade['shares']
            
            if symbol in price_lookup:
                usd_price = price_lookup[symbol]
                holding_value = shares * usd_price
                
                holdings.append({
                    'business_date': trade['trade_date'],
                    'etf_symbol': symbol,
                    'shares': shares,
                    'price_usd': usd_price,
                    'holding_value_usd': holding_value,
                    'trade_id': trade['trade_id']
                })
                
                total_value += holding_value
                
                print(f"  {symbol}: {shares:,.2f} shares × ${usd_price:,.2f} = ${holding_value:,.2f}")
            else:
                logging.warning(f"No price found for ETF {symbol}, skipping trade {trade['trade_id']}")
        
        print(f"✅ Calculated {len(holdings)} holdings with total value: ${total_value:,.2f}")
        logging.info(f"Calculated {len(holdings)} holdings with total value: {total_value}")
        
        return holdings
    
    @task()
    def write_etf_holdings(holdings: List[Dict[str, Any]], **context):
        """
        Write calculated holdings to etf_holdings table
        """
        print(f"💾 Writing {len(holdings)} holdings to etf_holdings table")
        
        if not holdings:
            print("⚠️  No holdings to write")
            return
        
        # Get database connection
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        # Insert query
        insert_query = """
            INSERT INTO public.etf_holdings 
                (business_date, etf_symbol, shares, price_usd, holding_value_usd, trade_id, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (business_date, trade_id) DO UPDATE SET
                etf_symbol = EXCLUDED.etf_symbol,
                shares = EXCLUDED.shares,
                price_usd = EXCLUDED.price_usd,
                holding_value_usd = EXCLUDED.holding_value_usd,
                updated_at = NOW()
        """
        
        # Execute inserts in a transaction
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            holdings_written = 0
            total_holdings_value = 0
            
            for holding in holdings:
                cursor.execute(insert_query, (
                    holding['business_date'],
                    holding['etf_symbol'],
                    holding['shares'],
                    holding['price_usd'],
                    holding['holding_value_usd'],
                    holding['trade_id']
                ))
                
                holdings_written += 1
                total_holdings_value += holding['holding_value_usd']
            
            # Commit the transaction
            conn.commit()
            
            print(f"✅ Successfully wrote {holdings_written} holdings to etf_holdings table")
            logging.info(f"Wrote {holdings_written} holdings with total value: {total_holdings_value}")
            
            # Push statistics to XCom for success callback
            context['ti'].xcom_push(key='holdings_processed', value=holdings_written)
            context['ti'].xcom_push(key='total_holdings_value', value=total_holdings_value)
            context['ti'].xcom_push(key='business_date', value=holdings[0]['business_date'] if holdings else None)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to etf_holdings: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Define task dependencies with optimized parallel execution
    market_data = read_etf_market_data()
    rates = read_exchange_rates()
    trades = read_etf_trades()
    
    # Calculate USD prices after market data and rates are ready
    usd_prices = calculate_usd_prices(market_data, rates)
    
    # Calculate holdings after both USD prices and trades are ready
    holdings = calculate_holdings(usd_prices, trades)
    
    # Write holdings to database
    write_etf_holdings(holdings)

# Instantiate the DAG
dag = etf_holdings_pipeline()