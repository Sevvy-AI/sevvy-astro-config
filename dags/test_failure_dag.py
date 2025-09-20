from datetime import datetime, timedelta
import requests
import json
import logging
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance
from airflow.configuration import conf

def send_failure_webhook(context):
    """
    Send a webhook notification when a task fails
    
    Args:
        context: Airflow context containing task instance and other details
    """
    print("🚨 CALLBACK TRIGGERED: send_failure_webhook function called!")
    logging.error("🚨 CALLBACK TRIGGERED: send_failure_webhook function called!")
    
    try:
        # Extract relevant information from context
        task_instance = context.get('task_instance')
        dag = context.get('dag')
        task = context.get('task')
        execution_date = context.get('execution_date') or context.get('logical_date')
        
        dag_id = dag.dag_id if dag else 'unknown'
        task_id = task.task_id if task else 'unknown'
        log_url = task_instance.log_url if task_instance else 'unknown'
        
        # Get Astro/Organization information
        # These environment variables are available in Astro Cloud deployments
        organization_id = os.getenv('ASTRO_WORKSPACE_ID', 'local-dev')  # Astro Workspace ID
        deployment_id = os.getenv('ASTRO_DEPLOYMENT_ID', 'local-dev')   # Astro Deployment ID
        
        # Alternative: get from Airflow configuration if available
        if organization_id == 'local-dev':
            organization_id = conf.get('webserver', 'base_url', fallback='local-dev').split('/')[-1] if conf.get('webserver', 'base_url', fallback='') else 'local-dev'
        
        print(f"📊 Context details: dag_id={dag_id}, task_id={task_id}")
        print(f"🏢 Organization ID: {organization_id}, Deployment ID: {deployment_id}")
        logging.error(f"📊 Context details: dag_id={dag_id}, task_id={task_id}")
        logging.error(f"🏢 Organization ID: {organization_id}, Deployment ID: {deployment_id}")
        
        # Webhook URL - Your actual webhook endpoint
        webhook_url = "https://655608b6e6d3.ngrok-free.app/api/webhooks/airflow"
        
        # Prepare payload with failure details (including required dag_id and task_id at root level)
        payload = {
            "event_type": "airflow_task_failure",
            "timestamp": datetime.now().isoformat(),
            "dag_id": dag_id,  # Required field at root level
            "task_id": task_id,  # Required field at root level
            "organization_id": organization_id,  # Astro Workspace/Organization ID
            "deployment_id": deployment_id,      # Astro Deployment ID
            "dag_details": {
                "dag_id": dag_id,
                "task_id": task_id,
                "execution_date": execution_date.isoformat() if execution_date else None,
                "try_number": task_instance.try_number if task_instance else 0,
                "max_tries": task_instance.max_tries if task_instance else 0,
                "state": str(task_instance.state) if task_instance else 'unknown',
                "log_url": log_url,
            },
            "environment": "development" if deployment_id == 'local-dev' else "production",
            "error_message": "Task intentionally failed for testing purposes"
        }
        
        print(f"📡 Sending webhook to: {webhook_url}")
        logging.error(f"📡 Sending webhook to: {webhook_url}")
        print(f"📦 Payload: {json.dumps(payload, indent=2)}")
        
        # Send POST request to webhook URL
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Airflow-Webhook/1.0',
            'ngrok-skip-browser-warning': 'true'  # Skip ngrok browser warning
        }
        
        response = requests.post(
            webhook_url,
            json=payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code in [200, 201, 202]:
            print(f"✅ Webhook sent successfully to {webhook_url}")
            logging.error(f"✅ Webhook sent successfully to {webhook_url}")
            print(f"📥 Response status: {response.status_code}")
            print(f"📥 Response: {response.text}")
        else:
            print(f"❌ Webhook failed with status code: {response.status_code}")
            logging.error(f"❌ Webhook failed with status code: {response.status_code}")
            logging.error(f"❌ Response: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"🌐 Network error sending webhook: {str(e)}")
        logging.error(f"🌐 Network error sending webhook: {str(e)}")
    except Exception as e:
        print(f"💥 Unexpected error in webhook callback: {str(e)}")
        logging.error(f"💥 Unexpected error in webhook callback: {str(e)}")
        import traceback
        print(f"📋 Traceback: {traceback.format_exc()}")
        logging.error(f"📋 Traceback: {traceback.format_exc()}")

def always_fail_task():
    """
    A simple task that always fails for testing purposes
    """
    print("Task started - this task is designed to always fail")
    logging.info("Task started - this task is designed to always fail")
    print("Simulating a critical error in the data pipeline")
    logging.error("Simulating a critical error in the data pipeline")
    
    # For this example, we'll raise a generic exception
    raise Exception("This task always fails for testing webhook notifications")

# Default arguments for the DAG
default_args = {
    'owner': 'sevvy-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Set to 0 so it doesn't retry
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'test_failure_webhook',
    default_args=default_args,
    description='Test DAG with failing task and webhook notification',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'webhook', 'failure', 'monitoring'],
) as dag:
    
    # Create the failing task
    failing_task = PythonOperator(
        task_id='always_fail_task',
        python_callable=always_fail_task,
        on_failure_callback=send_failure_webhook,  # This triggers the webhook on failure
    ) 