#!/usr/bin/env python

import boto3
from boto3.dynamodb.conditions import Attr
import requests

#######################################
### Global Vars #######################
#######################################

ses         = boto3.client('ses')
dynamodb    = boto3.resource('dynamodb')
table       = dynamodb.Table('monitoring')

#######################################
### Main Function #####################
#######################################

def main():
    list = scan_table()
    result = parse_data(list)

    print(result),
    return result


#######################################
### Program Specific Functions ########
#######################################

def scan_table():
    print("Performing DynamoDB Monitoring Table Scan...")

    response = table.scan(
        FilterExpression=Attr('site_name').begins_with('http')
    )
    result = response['Items']

    return result

def parse_data(list):
    result = ''

    for index in range(len(list)):
        url = list[index]['site_name']
        print("Parsing site {} ".format(url)),

        code, error, message = site_heartbeat(url)
        print(code)

        if message:
            print(message)

        incident = list[index]['incident']

        status_logic(code, url, error, message, incident)
        result = result + str(code) + "\t" + url + "\n"

    return result


def site_heartbeat(site_name):
    error   = False
    message = ''
    try:
        r   = requests.get(site_name, timeout=10)
    except requests.exceptions.Timeout:
        error   = True
        message = "Timeout limit reached"
    except requests.exceptions.ConnectionError:
        error   = True
        message = "Connection Refused"

    if error:
        status = -1
    else:
        status = r.status_code

    return status, error, message


def status_logic(code, name, error, message, incident_activity):
    if error == True and not incident_activity:
        print("Recording incident in database...")
        incident("site_name", name, True)

        print("Pushing notification for {} - status {}...".format(name, str(code)))
        info    = name + "\n" + \
                "Recieved the following error:" + "\n" + \
                message
        alert_type  = 'Monitoring Alert - ' + name
        alert(alert_type, info)

    elif code != 200 and not incident_activity:
        print("Recording incident in database...")
        incident("site_name", name, True)

        print("Pushing notification for {} - status {}...".format(name, code))
        info    = name + "\n" + \
                "Recieved the following error:" + "\n" + \
                str(code)
        alert_type  = 'Monitoring Alert - ' + name
        alert(alert_type, info)

    elif code == 200 and incident_activity:
        print("Resolving incident in database...")
        incident("site_name", name, False)
        print("Pushing notification for {} - status {}...".format(name, code))
        info    = name + "\n" + \
                "Incident Clear"
        alert_type  = 'Monitoring Alert Clear - ' + name
        alert(alert_type, info)


def incident(match, value, active):
    table.update_item(
        Key={
            match: value
        },
        UpdateExpression='SET incident = :val1',
        ExpressionAttributeValues={
            ':val1': active
        }
    )


def alert(subject, body):
    response    = ses.send_email(
        Source='no-reply@scriptmyjob.com',
        Destination={
            'BccAddresses': [
                'robert@scriptmyjob.com'
            ]
        },
        Message={
            'Subject': {
                'Data': subject,
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': body,
                    'Charset': 'UTF-8'
                }
            }
        }
    )

    return response

#######################################
### Execution #########################
#######################################

if __name__ == "__main__":
    main()


def execute_me_lambda(event, context):
    out = main()
    return out
