import os
import json
import boto3
import time


def lambda_handler(event, context):
    """
    Health check endpoint for Route 53 health monitoring
    Returns 200 OK with proper CORS headers
    """
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
        },
        'body': json.dumps({
            'status': 'healthy',
            'timestamp': time.time(),
            'region': os.environ.get('AWS_REGION', 'unknown')
        })
    }