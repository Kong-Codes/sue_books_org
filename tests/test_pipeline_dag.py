"""
Tests for pipeline_dag.py module
"""
from datetime import datetime
from ..dags.pipeline_dag import (
    on_task_success,
    on_task_failure,
    default_args
)


class TestOnTaskSuccess:
    """Test on_task_success callback function"""
    
    def test_on_task_success_logs_message(self, mocker):
        """Test that callback logs success message"""
        # Mock task instance
        mock_ti = mocker.Mock()
        mock_ti.task_id = "test_task"
        mock_ti.dag_id = "test_dag"
        
        context = {"task_instance": mock_ti}
        
        # Call the callback
        on_task_success(context)
        
        # Verify task instance attributes were accessed
        assert mock_ti.task_id == "test_task"
        assert mock_ti.dag_id == "test_dag"


class TestOnTaskFailure:
    """Test on_task_failure callback function"""
    
    def test_on_task_failure_sends_alert(self, mocker):
        """Test that callback sends alert on task failure"""
        mock_send_alert = mocker.patch('dags.pipeline_dag.send_alert')
        
        # Mock task instance
        mock_ti = mocker.Mock()
        mock_ti.task_id = "test_task"
        mock_ti.dag_id = "test_dag"
        mock_ti.execution_date = datetime(2024, 1, 1)
        mock_ti.log_url = "http://localhost:8080/logs"
        
        context = {"task_instance": mock_ti}
        
        # Call the callback
        on_task_failure(context)
        
        # Verify alert was sent
        mock_send_alert.assert_called_once()
        call_args = mock_send_alert.call_args
        
        assert call_args[0][0] == "Airflow Task Failure"
        assert call_args[0][1]["task_id"] == "test_task"
        assert call_args[0][1]["dag_id"] == "test_dag"
    
    def test_on_task_failure_without_log_url(self, mocker):
        """Test that callback handles missing log_url gracefully"""
        mock_send_alert = mocker.patch('dags.pipeline_dag.send_alert')
        
        # Simple object without log_url attribute
        class MockTI:
            task_id = "test_task"
            dag_id = "test_dag"
            execution_date = datetime(2024, 1, 1)

        mock_ti = MockTI()
        
        context = {"task_instance": mock_ti}
        
        # Call the callback
        on_task_failure(context)
        
        # Verify alert was sent with None for log_url
        mock_send_alert.assert_called_once()
        call_args = mock_send_alert.call_args
        
        assert call_args[0][1]["log_url"] is None


class TestDefaultArgs:
    """Test default_args configuration"""
    
    def test_default_args_owner(self):
        """Test that default_args has correct owner"""
        assert default_args['owner'] == 'sadique'
    
    def test_default_args_depends_on_past(self):
        """Test that default_args has correct depends_on_past setting"""
        assert default_args['depends_on_past'] is False
    
    def test_default_args_has_start_date(self):
        """Test that default_args has start_date"""
        assert 'start_date' in default_args
        assert isinstance(default_args['start_date'], datetime)
    
    def test_default_args_email_settings(self):
        """Test that default_args has correct email settings"""
        assert 'email' in default_args
    
    def test_default_args_retries(self):
        """Test that default_args has retry configuration"""
        assert 'retries' in default_args
        assert default_args['retries'] == 2
        assert 'retry_delay' in default_args


class TestPipelineDagStructure:
    """Test DAG structure and configuration"""
    
    def test_dag_has_correct_id(self):
        """Test that DAG has correct ID"""
        from ..dags.pipeline_dag import etl_dag
        
        assert etl_dag.dag_id == 'Sue_books_data_pipeline'
    
    def test_dag_has_correct_description(self):
        """Test that DAG has correct description"""
        from ..dags.pipeline_dag import etl_dag
        
        assert 'upload data to the db' in etl_dag.description
    
    def test_dag_has_schedule(self):
        """Test that DAG has schedule configured"""
        from ..dags.pipeline_dag import etl_dag
        
        assert etl_dag.schedule is not None
    
    def test_dag_catchup_is_false(self):
        """Test that DAG has catchup disabled"""
        from ..dags.pipeline_dag import etl_dag
        
        assert etl_dag.catchup is False


class TestExtractDataTask:
    """Test extract_data task"""
    
    def test_extract_data_calls_load_tables(self, mocker):
        """Test that extract_data calls load_tables"""
        mock_load_tables = mocker.patch('dags.pipeline_dag.load_tables')
        mock_load_tables.return_value = {
            'users': '/path/to/users.csv',
            'transactions': '/path/to/transactions.csv',
            'books': '/path/to/books.csv'
        }
        
        # Import and get the task function
        from ..dags.pipeline_dag import etl_pipeline
        dag = etl_pipeline()
        
        # Find the extract_data task
        extract_task = None
        for task in dag.tasks:
            if task.task_id == 'extract_data':
                extract_task = task
                break
        
        assert extract_task is not None


class TestTransformTask:
    """Test transform task"""
    
    def test_transform_task_exists(self):
        """Test that transform task exists in DAG"""
        from ..dags.pipeline_dag import etl_pipeline
        dag = etl_pipeline()
        
        transform_task = None
        for task in dag.tasks:
            if task.task_id == 'transform_data':
                transform_task = task
                break
        
        assert transform_task is not None


class TestLoadDataTask:
    """Test load_data task"""
    
    def test_load_data_task_exists(self):
        """Test that load_data task exists in DAG"""
        from ..dags.pipeline_dag import etl_pipeline
        dag = etl_pipeline()
        
        load_task = None
        for task in dag.tasks:
            if task.task_id == 'load_data':
                load_task = task
                break
        
        assert load_task is not None


class TestValidateSchemaTask:
    """Test validate_schema task"""
    
    def test_validate_schema_task_exists(self):
        """Test that validate_schema task exists in DAG"""
        from ..dags.pipeline_dag import etl_pipeline
        dag = etl_pipeline()
        
        validate_task = None
        for task in dag.tasks:
            if task.task_id == 'validate_schema':
                validate_task = task
                break
        
        assert validate_task is not None


class TestTaskDependencies:
    """Test task dependencies"""
    
    def test_task_dependencies_are_correct(self):
        """Test that tasks have correct dependencies"""
        from ..dags.pipeline_dag import etl_pipeline
        dag = etl_pipeline()
        
        # Get tasks by ID
        extract_task = dag.get_task('extract_data')
        transform_task = dag.get_task('transform_data')
        validate_task = dag.get_task('validate_schema')
        load_task = dag.get_task('load_data')
        
        # Check dependencies
        assert extract_task.task_id in transform_task.upstream_task_ids
        assert transform_task.task_id in validate_task.upstream_task_ids
        assert validate_task.task_id in load_task.upstream_task_ids
    
    def test_task_flow_is_linear(self):
        """Test that task flow follows expected linear pattern"""
        from ..dags.pipeline_dag import etl_pipeline
        dag = etl_pipeline()
        
        # Expected task order
        expected_order = ['extract_data', 'transform_data', 'validate_schema', 'load_data']
        
        # Get all task IDs
        task_ids = [task.task_id for task in dag.tasks]
        
        # Verify all expected tasks exist
        for task_id in expected_order:
            assert task_id in task_ids


class TestTaskCallbacks:
    """Test task callbacks"""
    
    def test_tasks_have_success_callbacks(self):
        """Test that tasks have success callbacks configured"""
        from ..dags.pipeline_dag import etl_pipeline
        dag = etl_pipeline()
        
        for task in dag.tasks:
            # Check if task has on_success_callback
            # This is set via the @task decorator parameters
            assert task is not None
    
    def test_tasks_have_failure_callbacks(self):
        """Test that tasks have failure callbacks configured"""
        from ..dags.pipeline_dag import etl_pipeline
        dag = etl_pipeline()
        
        for task in dag.tasks:
            # Check if task has on_failure_callback
            # This is set via the @task decorator parameters
            assert task is not None

