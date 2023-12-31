import boto3
import json

print('Loading function')
dynamo = boto3.client('dynamodb')
comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')


def set_default(obj):
    if isinstance(obj, set):
        return dict(obj)
    raise TypeError
    

def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }


def lambda_handler(event, context):
    '''Demonstrates a simple HTTP endpoint using API Gateway. You have full
    access to the request and response payload, including headers and
    status code.

    To scan a DynamoDB table, make a GET request with the TableName as a
    query string parameter. To put, update, or delete an item, make a POST,
    PUT, or DELETE request respectively, passing in the payload to the
    DynamoDB API as a JSON body.
    '''
    
    # print("body: " + json.dumps(event['body']))

    params = json.loads(event['body'])
    sentiment_data = None

    if "Comment" in params:
        sentiment_data = comprehend.detect_sentiment(Text=params['Comment'], LanguageCode='en')
        print(sentiment_data)

    operations = {
        'DELETE': lambda dynamo, x: dynamo.delete_item(**x),
        'GET': lambda dynamo, x: dynamo.scan(**x),
        'POST': lambda dynamo, x: dynamo.put_item(**x),
        'PUT': lambda dynamo, x: dynamo.update_item(**x),
    }

    operation = event['httpMethod']
    if operation in operations:
        if operation == 'GET':
            payload = event['queryStringParameters']
        elif operation == 'PUT':
            
            dump = json.dumps({
                'ExpressionAttributeNames':{
                    '#F': 'FirstName',
                    '#L': 'LastName',
                    '#C': 'Comment',
                    '#S': 'Sentiment'
                },
                'ExpressionAttributeValues':{
                    ':f': {
                        'S': params['FirstName'],
                    },
                    ':l': {
                        'S': params['LastName'],
                    },
                    ':c': {
                        'S': params['Comment'] if 'Comment' in params else '',
                    },
                    ':s': {
                        'S': sentiment_data['Sentiment'] if sentiment_data is not None else '',
                    }
                },
                'TableName':'demo-day-table',
                'Key':{
                    'Username': {
                        'S': params['Username']
                    }
                },
                'UpdateExpression':'SET #F = :f, #L = :l, #C = :c, #S = :s'
            })
            print(dump)
            payload = json.loads(dump)
        else:
            payload = json.loads(event['body'])
        return respond(None, operations[operation](dynamo, payload))
    else:
        return respond(ValueError('Unsupported method "{}"'.format(operation)))
