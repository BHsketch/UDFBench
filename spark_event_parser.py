import json
import glob
import os
from datetime import datetime
from collections import defaultdict
import pandas as pd

class SparkEventLogParser:
    """Parse Spark event logs to extract metrics similar to Spark UI"""
    
    def __init__(self, event_log_dir="/tmp/spark-events"):
        self.event_log_dir = event_log_dir
        self.events = []
        self.app_info = {}
        self.jobs = {}
        self.stages = {}
        self.tasks = []
        self.executors = {}
        
    def load_latest_application(self):
        """Load the most recent application event log"""
        event_files = glob.glob(os.path.join(self.event_log_dir, "app-*"))
        if not event_files:
            event_files = glob.glob(os.path.join(self.event_log_dir, "local-*"))
        if not event_files:
            print(f"No event logs found in {self.event_log_dir}")
            return False
        
        # Get most recent (remove .inprogress suffix for sorting)
        latest = max(event_files, key=lambda x: os.path.getmtime(x))
        print(f"Loading event log: {latest}")
        
        return self.load_application(latest)
    
    def load_application(self, log_path):
        """Load a specific application event log"""
        try:
            with open(log_path, 'r') as f:
                self.events = [json.loads(line.strip()) for line in f if line.strip()]
            print(f"Loaded {len(self.events)} events")
            self.parse_events()
            return True
        except Exception as e:
            print(f"Error loading event log: {e}")
            return False
    
    def parse_events(self):
        """Parse all events and organize into data structures"""
        for event in self.events:
            event_type = event.get('Event')
            
            if event_type == 'SparkListenerApplicationStart':
                self.app_info = {
                    'name': event.get('App Name'),
                    'id': event.get('App ID'),
                    'start_time': event.get('Timestamp'),
                    'user': event.get('User'),
                    'spark_version': event.get('Spark Version', 'Unknown')
                }
            
            elif event_type == 'SparkListenerApplicationEnd':
                self.app_info['end_time'] = event.get('Timestamp')
            
            elif event_type == 'SparkListenerJobStart':
                job_id = event.get('Job ID')
                self.jobs[job_id] = {
                    'id': job_id,
                    'start_time': event.get('Submission Time'),
                    'stage_ids': event.get('Stage IDs', []),
                    'properties': event.get('Properties', {})
                }
            
            elif event_type == 'SparkListenerJobEnd':
                job_id = event.get('Job ID')
                if job_id in self.jobs:
                    self.jobs[job_id]['end_time'] = event.get('Completion Time')
                    self.jobs[job_id]['result'] = event.get('Job Result', {}).get('Result')
            
            elif event_type == 'SparkListenerStageSubmitted':
                stage_info = event.get('Stage Info', {})
                stage_id = stage_info.get('Stage ID')
                self.stages[stage_id] = {
                    'id': stage_id,
                    'name': stage_info.get('Stage Name'),
                    'num_tasks': stage_info.get('Number of Tasks'),
                    'parent_ids': stage_info.get('Parent IDs', []),
                    'details': stage_info.get('Details', ''),
                    'submission_time': stage_info.get('Submission Time')
                }
            
            elif event_type == 'SparkListenerStageCompleted':
                stage_info = event.get('Stage Info', {})
                stage_id = stage_info.get('Stage ID')
                if stage_id in self.stages:
                    self.stages[stage_id]['completion_time'] = stage_info.get('Completion Time')
                    self.stages[stage_id]['failure_reason'] = stage_info.get('Failure Reason')
            
            elif event_type == 'SparkListenerTaskEnd':
                task_info = event.get('Task Info', {})
                task_metrics = event.get('Task Metrics', {})
                
                self.tasks.append({
                    'stage_id': event.get('Stage ID'),
                    'stage_attempt_id': event.get('Stage Attempt ID'),
                    'task_type': event.get('Task Type'),
                    'task_id': task_info.get('Task ID'),
                    'executor_id': task_info.get('Executor ID'),
                    'host': task_info.get('Host'),
                    'launch_time': task_info.get('Launch Time'),
                    'finish_time': task_info.get('Finish Time'),
                    'failed': task_info.get('Failed', False),
                    'executor_deserialize_time': task_metrics.get('Executor Deserialize Time', 0),
                    'executor_deserialize_cpu_time': task_metrics.get('Executor Deserialize CPU Time', 0),
                    'executor_run_time': task_metrics.get('Executor Run Time', 0),
                    'executor_cpu_time': task_metrics.get('Executor CPU Time', 0),
                    'result_size': task_metrics.get('Result Size', 0),
                    'jvm_gc_time': task_metrics.get('JVM GC Time', 0),
                    'result_serialization_time': task_metrics.get('Result Serialization Time', 0),
                    'memory_bytes_spilled': task_metrics.get('Memory Bytes Spilled', 0),
                    'disk_bytes_spilled': task_metrics.get('Disk Bytes Spilled', 0),
                    'peak_execution_memory': task_metrics.get('Peak Execution Memory', 0),
                    'input_bytes': task_metrics.get('Input Metrics', {}).get('Bytes Read', 0),
                    'input_records': task_metrics.get('Input Metrics', {}).get('Records Read', 0),
                    'output_bytes': task_metrics.get('Output Metrics', {}).get('Bytes Written', 0),
                    'output_records': task_metrics.get('Output Metrics', {}).get('Records Written', 0),
                    'shuffle_read_bytes': task_metrics.get('Shuffle Read Metrics', {}).get('Total Bytes Read', 0),
                    'shuffle_read_records': task_metrics.get('Shuffle Read Metrics', {}).get('Total Records Read', 0),
                    'shuffle_write_bytes': task_metrics.get('Shuffle Write Metrics', {}).get('Bytes Written', 0),
                    'shuffle_write_records': task_metrics.get('Shuffle Write Metrics', {}).get('Records Written', 0),
                })
            
            elif event_type == 'SparkListenerExecutorAdded':
                executor_id = event.get('Executor ID')
                executor_info = event.get('Executor Info', {})
                self.executors[executor_id] = {
                    'id': executor_id,
                    'host': executor_info.get('Host'),
                    'total_cores': executor_info.get('Total Cores'),
                    'added_time': event.get('Timestamp')
                }
            
            elif event_type == 'SparkListenerExecutorRemoved':
                executor_id = event.get('Executor ID')
                if executor_id in self.executors:
                    self.executors[executor_id]['removed_time'] = event.get('Timestamp')
                    self.executors[executor_id]['removed_reason'] = event.get('Removed Reason')
    
    def print_application_summary(self):
        """Print application-level summary (like Spark UI home page)"""
        print("\n" + "="*80)
        print("APPLICATION SUMMARY")
        print("="*80)
        
        if not self.app_info:
            print("No application info found")
            return
        
        print(f"App Name: {self.app_info.get('name')}")
        print(f"App ID: {self.app_info.get('id')}")
        print(f"User: {self.app_info.get('user')}")
        print(f"Spark Version: {self.app_info.get('spark_version')}")
        
        if 'start_time' in self.app_info:
            start = datetime.fromtimestamp(self.app_info['start_time'] / 1000)
            print(f"Started: {start}")
        
        if 'end_time' in self.app_info:
            end = datetime.fromtimestamp(self.app_info['end_time'] / 1000)
            duration = (self.app_info['end_time'] - self.app_info['start_time']) / 1000
            print(f"Ended: {end}")
            print(f"Duration: {duration:.2f}s")
        
        print(f"\nJobs: {len(self.jobs)}")
        print(f"Stages: {len(self.stages)}")
        print(f"Tasks: {len(self.tasks)}")
        print(f"Executors: {len(self.executors)}")
    
    def print_jobs_summary(self):
        """Print jobs summary (like Spark UI Jobs tab)"""
        print("\n" + "="*80)
        print("JOBS SUMMARY")
        print("="*80)
        
        if not self.jobs:
            print("No jobs found")
            return
        
        for job_id, job in sorted(self.jobs.items()):
            duration = 0
            if 'end_time' in job and 'start_time' in job:
                duration = (job['end_time'] - job['start_time']) / 1000
            
            status = job.get('result', 'Running')
            print(f"\nJob {job_id}: {status}")
            print(f"  Stages: {len(job['stage_ids'])}")
            print(f"  Duration: {duration:.2f}s")
            print(f"  Stage IDs: {job['stage_ids']}")
    
    def print_stages_summary(self):
        """Print stages summary (like Spark UI Stages tab)"""
        print("\n" + "="*80)
        print("STAGES SUMMARY")
        print("="*80)
        
        if not self.stages:
            print("No stages found")
            return
        
        for stage_id, stage in sorted(self.stages.items()):
            duration = 0
            if 'completion_time' in stage and 'submission_time' in stage:
                duration = (stage['completion_time'] - stage['submission_time']) / 1000
            
            # Calculate stage-level metrics
            stage_tasks = [t for t in self.tasks if t['stage_id'] == stage_id]
            
            total_time = sum(t['executor_run_time'] for t in stage_tasks) / 1000
            gc_time = sum(t['jvm_gc_time'] for t in stage_tasks) / 1000
            input_bytes = sum(t['input_bytes'] for t in stage_tasks)
            output_bytes = sum(t['output_bytes'] for t in stage_tasks)
            
            print(f"\nStage {stage_id}: {stage['name']}")
            print(f"  Tasks: {stage['num_tasks']}")
            print(f"  Duration: {duration:.2f}s")
            print(f"  Total Task Time: {total_time:.2f}s")
            print(f"  GC Time: {gc_time:.2f}s ({gc_time/total_time*100:.1f}%)" if total_time > 0 else "  GC Time: 0s")
            print(f"  Input: {input_bytes / (1024**2):.2f} MB")
            print(f"  Output: {output_bytes / (1024**2):.2f} MB")
            
            if stage.get('failure_reason'):
                print(f"  FAILED: {stage['failure_reason']}")
    
    def print_tasks_summary(self):
        """Print tasks summary with timing breakdown"""
        print("\n" + "="*80)
        print("TASKS SUMMARY")
        print("="*80)
        
        if not self.tasks:
            print("No tasks found")
            return
        
        df = pd.DataFrame(self.tasks)
        
        # Group by stage
        for stage_id in sorted(df['stage_id'].unique()):
            stage_tasks = df[df['stage_id'] == stage_id]
            
            print(f"\nStage {stage_id}:")
            print(f"  Tasks: {len(stage_tasks)}")
            print(f"  Failed: {stage_tasks['failed'].sum()}")
            
            # Timing breakdown
            print(f"\n  Timing Breakdown (total across all tasks):")
            print(f"    Deserialize Time:     {stage_tasks['executor_deserialize_time'].sum()/1000:>10.2f}s")
            print(f"    Executor Run Time:    {stage_tasks['executor_run_time'].sum()/1000:>10.2f}s")
            print(f"    Serialize Time:       {stage_tasks['result_serialization_time'].sum()/1000:>10.2f}s")
            print(f"    GC Time:              {stage_tasks['jvm_gc_time'].sum()/1000:>10.2f}s")
            
            total_time = stage_tasks['executor_run_time'].sum()
            if total_time > 0:
                deser_pct = stage_tasks['executor_deserialize_time'].sum() / total_time * 100
                ser_pct = stage_tasks['result_serialization_time'].sum() / total_time * 100
                gc_pct = stage_tasks['jvm_gc_time'].sum() / total_time * 100
                
                print(f"\n  Overhead Percentage:")
                print(f"    Deserialization:      {deser_pct:>10.1f}%")
                print(f"    Serialization:        {ser_pct:>10.1f}%")
                print(f"    GC:                   {gc_pct:>10.1f}%")
                print(f"    Total Overhead:       {deser_pct + ser_pct + gc_pct:>10.1f}%")
            
            # Data metrics
            print(f"\n  Data Metrics:")
            print(f"    Input:                {stage_tasks['input_bytes'].sum()/(1024**2):>10.2f} MB")
            print(f"    Output:               {stage_tasks['output_bytes'].sum()/(1024**2):>10.2f} MB")
            print(f"    Shuffle Read:         {stage_tasks['shuffle_read_bytes'].sum()/(1024**2):>10.2f} MB")
            print(f"    Shuffle Write:        {stage_tasks['shuffle_write_bytes'].sum()/(1024**2):>10.2f} MB")
            print(f"    Memory Spilled:       {stage_tasks['memory_bytes_spilled'].sum()/(1024**2):>10.2f} MB")
            print(f"    Disk Spilled:         {stage_tasks['disk_bytes_spilled'].sum()/(1024**2):>10.2f} MB")
    
    def print_executors_summary(self):
        """Print executors summary"""
        print("\n" + "="*80)
        print("EXECUTORS SUMMARY")
        print("="*80)
        
        if not self.executors:
            print("No executors found")
            return
        
        for executor_id, executor in sorted(self.executors.items()):
            status = "Removed" if 'removed_time' in executor else "Active"
            print(f"\nExecutor {executor_id}: {status}")
            print(f"  Host: {executor['host']}")
            print(f"  Cores: {executor['total_cores']}")
            
            if 'removed_time' in executor:
                uptime = (executor['removed_time'] - executor['added_time']) / 1000
                print(f"  Uptime: {uptime:.2f}s")
                print(f"  Removal Reason: {executor.get('removed_reason', 'Unknown')}")
            
            # Count tasks for this executor
            executor_tasks = [t for t in self.tasks if t['executor_id'] == executor_id]
            print(f"  Tasks Executed: {len(executor_tasks)}")
            
            if executor_tasks:
                total_time = sum(t['executor_run_time'] for t in executor_tasks) / 1000
                gc_time = sum(t['jvm_gc_time'] for t in executor_tasks) / 1000
                print(f"  Total Task Time: {total_time:.2f}s")
                print(f"  GC Time: {gc_time:.2f}s ({gc_time/total_time*100:.1f}%)" if total_time > 0 else "  GC Time: 0s")
    
    def export_to_csv(self, output_dir="."):
        """Export parsed data to CSV files"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Export tasks
        if self.tasks:
            df_tasks = pd.DataFrame(self.tasks)
            tasks_file = os.path.join(output_dir, "spark_tasks.csv")
            df_tasks.to_csv(tasks_file, index=False)
            print(f"Exported tasks to {tasks_file}")
        
        # Export stages
        if self.stages:
            stages_list = []
            for stage_id, stage in self.stages.items():
                stage_copy = stage.copy()
                stages_list.append(stage_copy)
            df_stages = pd.DataFrame(stages_list)
            stages_file = os.path.join(output_dir, "spark_stages.csv")
            df_stages.to_csv(stages_file, index=False)
            print(f"Exported stages to {stages_file}")
        
        # Export jobs
        if self.jobs:
            jobs_list = []
            for job_id, job in self.jobs.items():
                job_copy = job.copy()
                job_copy['stage_ids'] = str(job_copy['stage_ids'])
                jobs_list.append(job_copy)
            df_jobs = pd.DataFrame(jobs_list)
            jobs_file = os.path.join(output_dir, "spark_jobs.csv")
            df_jobs.to_csv(jobs_file, index=False)
            print(f"Exported jobs to {jobs_file}")
    
    def generate_full_report(self):
        """Generate a full report similar to Spark UI"""
        self.print_application_summary()
        self.print_jobs_summary()
        self.print_stages_summary()
        self.print_tasks_summary()
        self.print_executors_summary()
    
    def compare_stages(self, stage_id_1, stage_id_2):
        """Compare two stages (useful for comparing Pandas vs Scala UDF)"""
        print("\n" + "="*80)
        print(f"COMPARING STAGE {stage_id_1} vs STAGE {stage_id_2}")
        print("="*80)
        
        tasks_1 = [t for t in self.tasks if t['stage_id'] == stage_id_1]
        tasks_2 = [t for t in self.tasks if t['stage_id'] == stage_id_2]
        
        if not tasks_1 or not tasks_2:
            print("One or both stages not found")
            return
        
        df1 = pd.DataFrame(tasks_1)
        df2 = pd.DataFrame(tasks_2)
        
        metrics = {
            'executor_run_time': 'Execution Time',
            'executor_deserialize_time': 'Deserialization Time',
            'result_serialization_time': 'Serialization Time',
            'jvm_gc_time': 'GC Time',
        }
        
        print(f"\n{'Metric':<30} {'Stage ' + str(stage_id_1):>15} {'Stage ' + str(stage_id_2):>15} {'Difference':>15}")
        print("-" * 80)
        
        for metric, label in metrics.items():
            val1 = df1[metric].sum() / 1000
            val2 = df2[metric].sum() / 1000
            diff = ((val2 - val1) / val1 * 100) if val1 > 0 else 0
            
            print(f"{label:<30} {val1:>14.2f}s {val2:>14.2f}s {diff:>13.1f}%")


def main():
    """Main function to parse and display Spark event logs"""
    import sys
    
    # Check if event log directory was provided
    event_dir = sys.argv[1] if len(sys.argv) > 1 else "/tmp/spark-events"
    
    parser = SparkEventLogParser(event_dir)
    
    if parser.load_latest_application():
        parser.generate_full_report()
        
        # Optionally export to CSV
        parser.export_to_csv("spark_metrics")
        print(f"\nMetrics exported to spark_metrics/ directory")
    else:
        print("Failed to load event logs")


if __name__ == "__main__":
    main()
