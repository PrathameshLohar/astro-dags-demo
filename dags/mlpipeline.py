from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

## define task 1
def prepross_data():
    print("Data preprocessing...")

## define task 2
def model_training():
    print("Model training...")

## define task 3
def model_evaluation():
    print("Evaluating model...")

## define DAG
with DAG(
    'ml_pipeline',
    start_date= datetime(2024,1,1),
    schedule_interval= '@weekly'
)as dag:
    
    ## define the functions as task
    preprocess= PythonOperator(task_id="preprocess_task", python_callable=prepross_data)
    train= PythonOperator(task_id="train_task", python_callable=model_training)
    evaluate= PythonOperator(task_id="evaluate_task", python_callable=model_evaluation)

    ## set dependancies for DAG
    preprocess >> train >> evaluate




    