from django.shortcuts import render
from django.http import HttpResponse
# Create your views here.
import datetime,json
import requests,path
import pandas as pd
import pygsheets,redis
from config import config




def makestr(df,column):
    df[column] = df[column].astype(str)
    
# r = redis.Redis('{}'.format(config.redis_host), port = 6379, db = 0)
encoding = 'utf-8'
date1 = str(datetime.datetime.now().date() - datetime.timedelta(days=15))
date2 = str(datetime.datetime.now().date())
gc = pygsheets.authorize(client_secret='./dispositions/client_secret.json')


def welcome(request):
    if request.method == 'GET':
        return HttpResponse("NEW API", content_type ='application/json')



def get_call_hist(request, phone_no):
    if request.method == 'GET':
        try:
            corporate_url = "https://{}:{}@{}/v1/Accounts/medibuddy3/Calls.json?DateCreated=gte:{}+00:00:01%3Blte:{}+23:59:59&details=true&PageSize=100&From=%2B91{}".format(config.corporate_api_key,config.corporate_token,config.sub_domain,date1,date2,phone_no)
            corporate_resp = requests.get(corporate_url)
            corporate_resp_json = corporate_resp.json()
            retail_url = "https://{}:{}@{}/v1/Accounts/docsapp1/Calls.json?DateCreated=gte:{}+00:00:01%3Blte:{}+23:59:59&details=true&PageSize=100&From=%2B91{}".format(config.retail_api_key,config.retail_api_token,config.sub_domain,date1,date2,phone_no)
            retail_resp = requests.get(retail_url)
            retail_resp_json = retail_resp.json()
            final_json = {'corporate': corporate_resp_json,
                        'retail' : retail_resp_json}
            
            calls_final = []
            if final_json['corporate']['Metadata']!=None:
                for i in range(len(final_json['corporate']['Calls'])):
                    final_dict = {}
                    final_dict['From'] =  final_json['corporate']['Calls'][i]['From']
                    final_dict['ExoPhone'] = final_json['corporate']['Calls'][i]['PhoneNumber']
                    final_dict['calledAt'] = final_json['corporate']['Calls'][i]['StartTime']
                    final_dict['ConversationDuration'] = final_json['corporate']['Calls'][i]['Details']['ConversationDuration']
                    final_dict['Leg2Status'] = final_json['corporate']['Calls'][i]['Details']['Leg2Status']
                    calls_final.append(final_dict)   
            if final_json['retail']['Metadata']!=None:
                for i in range(final_json['retail']['Metadata']['Total']):
                    final_dict = {}
                    final_dict['From'] =  final_json['retail']['Calls'][i]['From']
                    final_dict['ExoPhone'] = final_json['retail']['Calls'][i]['PhoneNumber']
                    final_dict['calledAt'] = final_json['retail']['Calls'][i]['StartTime']
                    final_dict['ConversationDuration'] = final_json['retail']['Calls'][i]['Details']['ConversationDuration']
                    final_dict['Leg2Status'] = final_json['retail']['Calls'][i]['Details']['Leg2Status']
                    calls_final.append(final_dict)
            response_json = json.dumps(calls_final)
        except:
            response_json = json.dumps([{'Phone_No': phone_no,
            'Data' : 'No Found!'}])
    return HttpResponse(response_json, content_type = 'application/json')


def load_corporate_dict():
    sh_corporate = gc.open_by_url("{}".format(config.corporate_sheet))
    campaign_corporate = 'Corporate_Calls'
    worksheet_corporate = sh_corporate.worksheet('title',campaign_corporate)
    corporate_length = worksheet_corporate.rows
    start_row = 'A{}'.format(corporate_length-10000)
    last_row = 'J{}'.format(corporate_length)
    grange = pygsheets.GridRange(worksheet=worksheet_corporate, start=start_row, end=last_row,propertiesjson = None)
    dem = worksheet_corporate.get_values(grange = grange,returnas='matrix')
    responses = pd.DataFrame(dem)
    
    
    col = ['Timestamp', 'Called No','1887683', 'Corporate Name' ,'If Other then Above Corporates','Medical Type', 'Query', 'Resolution', 'Agent Name' ,'Date']
    responses.columns = col
    responses = responses.rename({'Called No':'Phone Number of the Caller',
                       'Medical Type' : 'Disposition Class',
                        'Query' : 'Voice of Customer - Remarks as to why customer called us'},axis = 1)
    responses[['Email','disposition_text']] = ''
    responses = responses[['Timestamp','Email','Phone Number of the Caller','Disposition Class','disposition_text','Voice of Customer - Remarks as to why customer called us','Resolution','Corporate Name']]
    
    campaign_pharmacy = 'Pharmacy_Calls'
    worksheet_pharmacy = sh_corporate.worksheet('title',campaign_pharmacy)
    pharma_length = worksheet_pharmacy.rows
    start_row_pharma = 'A{}'.format(pharma_length-10000)
    end_row_pharma = 'L{}'.format(pharma_length)
    grange_pharma = pygsheets.GridRange(worksheet=worksheet_pharmacy, start=start_row_pharma, end=end_row_pharma,propertiesjson = None)
    dem_pharma = worksheet_pharmacy.get_values(grange = grange_pharma,returnas='matrix')
    responses1 = pd.DataFrame(dem_pharma)
    cols_pharma = ['Timestamp'	,'Called No','Employee ID','Order Id','Corporate Name','If Other then Above Corporates','Category','Query','Resolution','Agent Name','Date','Is there a need to email vendor ?']
    responses1.columns = cols_pharma
    responses1 = responses1.rename({'Called No':'Phone Number of the Caller',
                                    'Category' : 'Disposition Class',
                                   'Query' : 'Voice of Customer - Remarks as to why customer called us'},axis =1)

    responses1[['Email','disposition_text']] = ''
    responses1 = responses1[['Timestamp','Email','Phone Number of the Caller','Disposition Class','disposition_text','Voice of Customer - Remarks as to why customer called us','Resolution','Corporate Name']]
    responses_corporate_dict = responses.append(responses1,ignore_index=True)
    responses_corporate_dict = responses_corporate_dict.to_dict(orient='records')
    r.hmset("responses_corporate_dict", {"Responses":json.dumps(responses_corporate_dict)})
    r.expire("responses_corporate_dict", 1500)





def load_retail_dict():
    sh=gc.open_by_url("{}".format(config.retail_sheet))
    campaign = 'Sheet1'
    master_call_dict = pd.DataFrame()
    worksheet = sh.worksheet('title',campaign)
    tmp = worksheet.get_all_records()
    master_call_dict = master_call_dict.append(tmp)
    master_call_dict = master_call_dict.to_dict(orient = 'records')
    r.hmset("responses_retail_dict", {"Responses":json.dumps(master_call_dict)})
    r.expire("responses_retail_dict", 1500)



    


def get_call_dispositions(request, phone_no):
    if request.method == 'GET':
        try:
            if phone_no!=None:
                responses_retail_dict =  r.hgetall("responses_retail_dict")  
                if len(responses_retail_dict) == 0:
                    load_retail_dict()
                    responses_retail_dict =  r.hgetall("responses_retail_dict")                          
                responses_retail = pd.DataFrame(json.loads(responses_retail_dict[b'Responses']))
                makestr(responses_retail, 'Timestamp')
                makestr(responses_retail,'Disposition Class')
                makestr(responses_retail, 'Phone Number of the Caller')
                responses_retail = responses_retail[responses_retail['Phone Number of the Caller'] == phone_no]
                responses_retail[['Resolution']] = '' 
                responses_retail['Corporate Name'] = 'Retail'
                responses_retail['disposition_text'] = responses_retail[['Labs', 'Medicine', 'Non-App User', 'Non-Paid user', 'Others', 'Package','Paid User', 'Payments', 'Tech Issue']].max(axis=1)
                responses_retail = responses_retail[['Timestamp','Email','Phone Number of the Caller','Disposition Class','disposition_text','Voice of Customer - Remarks as to why customer called us','Resolution','Corporate Name']]
                # responses_retail = responses_retail.sort_values('Timestamp', ascending = False)
                responses_retail = responses_retail.to_dict(orient='records')
        except Exception as e:
            responses_retail = [e]
    
        try:
            if phone_no!=None:
                responses_corporate_dict = r.hgetall("responses_corporate_dict")  
                if len(responses_corporate_dict) == 0:
                    load_corporate_dict()
                    responses_corporate_dict = r.hgetall("responses_corporate_dict")
                responses_corporate = pd.DataFrame(json.loads(responses_corporate_dict[b'Responses']))
                makestr(responses_corporate, 'Phone Number of the Caller')
                responses_corporate = responses_corporate[responses_corporate['Phone Number of the Caller'] == phone_no]
                responses_corporate = responses_corporate[['Timestamp','Email','Phone Number of the Caller','Disposition Class','disposition_text','Voice of Customer - Remarks as to why customer called us','Resolution','Corporate Name']]
                # responses_corporate = responses_corporate.sort_values('Timestamp', ascending = False)
                responses_corporate = responses_corporate.to_dict(orient='records')
        except Exception as e:
            responses_corporate =[e]
        
        response = responses_retail + responses_corporate
        # response = json.dumps(response)
        return HttpResponse(response, content_type = 'application/json')

        
            

