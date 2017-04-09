
from django.shortcuts import render
from django.utils import timezone
from django.http import HttpResponse, JsonResponse
from django.template.loader import get_template
from django.template import Context
from django.template import RequestContext
from datetime import datetime
from django.views.decorators.csrf import csrf_exempt

import json
import time
import os
import re
import requests
from elasticsearch import Elasticsearch
import certifi

es_endpoint = 'search-tweetanalysis-a3bxpbmcwvfst6oamsavyzaw2q.us-east-1.es.amazonaws.com'
search_address = 'https://search-tweetanalysis-a3bxpbmcwvfst6oamsavyzaw2q.us-east-1.es.amazonaws.com/tweetanalysis/tweets/_search'


def tweeterAPI(request):
    return render(request, 'map.html')

@csrf_exempt
def sns(request):
    message_type_header = 'HTTP_X_AMZ_SNS_MESSAGE_TYPE'
    #message_type_header = 'x-amz-sns-message-type'
    #message_type_header = 'CONTENT_TYPE'
    if message_type_header in request.META:
        payload = json.loads(request.body.decode('utf-8'))
        message_type = request.META[message_type_header]

        if message_type == 'SubscriptionConfirmation':
            subscribe_url = payload.get('SubscribeURL')
            print subscribe_url
            res = requests.get(subscribe_url)
            if res.status_code != 200:
                print "Failed to verify SNS Subscription"
            else:
                print "Subcription Successful"

        if message_type == 'Notification':
            message = json.loads(payload['Message'])
            message_id = payload['MessageId']
            #message = payload['Message']
            es_index(message,message_id)
            print message,message_id
    return HttpResponse('OK',content_type='application/json')


def search(request):
    keyword = request.POST['x']
    response = es_search(keyword)
    res = response['hits']['hits']
    res = json.dumps(res)
    return HttpResponse(res,content_type='application/json')


def geosearch(request):
    location = request.POST['location'].split(",")
    location[0] = float(location[0])
    location[1] = float(location[1])
    distance = request.POST['distance']
    print 'location: %s, radius: %s' % (location, distance)

    response = es_geosearch(location, distance, 2000) # search at most 2000 tweets
    print 'geosearch response: %s' % response
    response = response['hits']['hits']
    response = json.dumps(response)
    return HttpResponse(response, content_type='application/json')

#Index tweet in ElasticSearch
def es_index(message,msg_id):
    es = Elasticsearch(hosts=[es_endpoint], port=443, use_ssl=True, verify_certs=True, ca_certs=certifi.where())
    response = es.index(index='tweetanalysis', doc_type='tweets', id=msg_id, body=message)
    return


#Send Post Request to ElasticSearch
def es_search(keyword):
    data = {"size": 2000,"query": {"query_string": { "query": keyword }}}
    response = requests.post(search_address, data=json.dumps(data))
    #print response
    return response.json()

#Geo index is set to "geo_point" variable
def es_geosearch(location, distance, size):
    data = {
            "size": size,
            "query": {
                "bool": {
                    "must": {
                        "match_all": {}
                    },
                    "filter": {
                        "geo_distance": {
                            "distance": '%skm' % (distance),
                            "geo": location
                        }
                    }
                }
            }
    }
    response = requests.post(search_address, data=json.dumps(data))
    #print response
    return response.json()

