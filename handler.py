import json
import datetime
import boto3

cloud_watch = boto3.client('cloudwatch', region_name='us-east-1')
dynamodb = boto3.client('dynamodb')
 
list_metrics = cloud_watch.list_metrics()

def run(event, context):
    start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=60)
    end_time = datetime.datetime.utcnow()
    period = 300

    consumed_read_cap_parameters = {
        'namespace': 'AWS/DynamoDB',
        'metric_name': 'ConsumedReadCapacityUnits',
        'dimension_name': 'TableName',
        'dimension_value': 'CapacityUnitsTest',
        'start_time': start_time,
        'end_time': end_time,
        'period': period,
        'statistics': ['Average'],
        'unit': 'Count'
    }

    metric_consumed_read_cap = get_metrics(consumed_read_cap_parameters)
    output_metrics(consumed_read_cap_parameters, metric_consumed_read_cap)


    consumed_write_cap_parameters = {
        'namespace': 'AWS/DynamoDB',
        'metric_name': 'ConsumedWriteCapacityUnits',
        'dimension_name': 'TableName',
        'dimension_value': 'CapacityUnitsTest',
        'start_time': start_time,
        'end_time': end_time,
        'period': period,
        'statistics': ['Average'],
        'unit': 'Count'
    }

    metric_consumed_write_cap = get_metrics(consumed_write_cap_parameters)
    output_metrics(consumed_write_cap_parameters, metric_consumed_write_cap)

    provisioned_read_cap_parameters = {
        'namespace': 'AWS/DynamoDB',
        'metric_name': 'ProvisionedReadCapacityUnits',
        'dimension_name': 'TableName',
        'dimension_value': 'CapacityUnitsTest',
        'start_time': start_time,
        'end_time': end_time,
        'period': period,
        'statistics': ['Average'],
        'unit': 'Count'
    }

    metric_provisioned_read_cap = get_metrics(provisioned_read_cap_parameters)
    output_metrics(provisioned_read_cap_parameters, metric_provisioned_read_cap)

    provisioned_write_cap_parameters = {
        'namespace': 'AWS/DynamoDB',
        'metric_name': 'ProvisionedWriteCapacityUnits',
        'dimension_name': 'TableName',
        'dimension_value': 'CapacityUnitsTest',
        'start_time': start_time,
        'end_time': end_time,
        'period': period,
        'statistics': ['Average'],
        'unit': 'Count'
    }

    metric_provisioned_write_cap = get_metrics(provisioned_write_cap_parameters)
    output_metrics(provisioned_write_cap_parameters, metric_provisioned_write_cap)

    return "Success"

def get_metrics(parameters):

    metrics = cloud_watch.get_metric_statistics(
                            Namespace=parameters['namespace'],
                            MetricName=parameters['metric_name'],
                            Dimensions=[
                                {
                                    'Name': parameters['dimension_name'],
                                    'Value': parameters['dimension_value']
                                }
                            ],
        StartTime=parameters['start_time'],
        EndTime=parameters['end_time'],
        Period=parameters['period'],
        Statistics=parameters['statistics'],
        Unit=parameters['unit'])
    
    return metrics

def output_metrics(parameters, target_metric):
    sort_datapoints = sorted(target_metric['Datapoints'], key=lambda x: x['Timestamp'])   
    print(parameters['metric_name'] + " result:") 
    for data in sort_datapoints:
        print(str(data['Timestamp']) + "\t" + str(data['Average']))
