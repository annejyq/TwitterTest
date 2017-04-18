import requests

class Analysis:
	url = ""
	querystring = {"version":"2017-02-27"}
	content = ""
	def __init__(self,text,api_link):
		self.content = text
		self.url = api_link

	def request_api(self):
		payload = "{\n  \"text\": \""+ self.content +"\",\n  \"features\": {\n      \"sentiment\": {\n      }\n   }\n}"
		headers = {
			'content-type': "application/json",
			'authorization': "Basic N2RkMDEyMjAtNTRjYy00YTEyLWE2MzItZWRkY2JmMGFjYmE1OlAzRmVpM3RFeUxqTw==",
			'cache-control': "no-cache",
			'postman-token': "f1b40eed-33be-c078-1790-0e870463c8f5"
			}
		response = requests.request("POST", self.url, data=payload, headers=headers, params=self.querystring)
		return response.text