import os
import json
import pandas as pd
import oauth2client.client
from oauth2client.client import OAuth2WebServerFlow
import requests_oauthlib
import httplib2
import apiclient
import webbrowser
from requests_oauthlib import OAuth2Session
from oauth2client import file


class GoogleAPI(object):

    """Base Class for Google APIs that seem to be consistent when looking at Search Console and core reporting API
    args:
    api_name: name of the google api as reported by the google specs (ie:webmasters etc...)
    api_version: needed to get to the API, read current docs for the API you will use
    dat_filename:.dat file where your credential string will/should be stored in. See Auth workflow for more informations
    scope: scope of the API telling what you can/can't access. So far all implemented methods are for readonly access
    client_secret_json_file is the fullpath (localepath/filename) where your credentials are stored. You can get them in the google API console
    redirect uri is where you get redirected if your token is missing/invalid.

    methods:
    to get / set attribute use the methods get_{attribute_name}()/set_{attribute_name}()
    get_service:
    create a service object used by all GA apis,
    rule of thumb with filepath : always use full path stuff with os.path.join & os.getcwd()
    To be explicit / and be crossplatform rather than hardcoded strings if you can avoid it

    """

    def __init__(self, api_name, api_version, dat_filename, scope, client_secret_json_file, redirect_uri):
        self.api_name = api_name
        self.api_version = api_version
        self.dat_filename = dat_filename
        self.scope = scope
        self.json_file = client_secret_json_file
        self.redirect_uri = redirect_uri
        self.service = self.get_service()

    def set_api_name(self, api_name):
        self.api_name = api_name
        return None

    def set_dat_filename(self, dat_filepath):
        self.dat_filename = dat_filepath
        return None

    def set_scope(self, scope):
        self.scope = scope
        return None

    def set_jsonfilepath(self, jsonfilepath):
        self.json_file = jsonfilepath
        return None

    def set_redirect_uri(self, redirect_uri):
        self.redirect_uri = redirect_uri
        return None

    def get_api_name(self,):
        return self.api_name

    def get_credentials(self):
        """
        param:dat_filename is the name of the .dat file in which you have the installed Oauth credential. \n
        It's either an existing .dat file or the name of the file once you're done with the authentication workflow.
        """
        with open(self.json_file, 'r') as client_secret:
            credentials = json.loads(client_secret.read())
        installed_credentials = credentials["installed"]

        client_id = installed_credentials["client_id"]
        client_secret = installed_credentials["client_secret"]
        redirect_uri = self.redirect_uri
        auth_url = installed_credentials["auth_uri"]
        token_url = installed_credentials["token_uri"]
        """Gets google api credentials, or generates new credentials
        if they don't exist or are invalid."""

        # this should be where your json file is
        pathToFile = self.json_file
        print(pathToFile)

        # first step of flow process
        # redirect uri can be a custom url for your app
        #'urn:ietf:wg:oauth:2.0:oob' is localhost
        flow = oauth2client.client.flow_from_clientsecrets(pathToFile, self.scope, redirect_uri=self.redirect_uri)
        # check to see if you have something already

        storage = file.Storage(self.dat_filename)
        # if its there then get it
        credentials = storage.get()

        if not credentials or credentials.invalid:
            # get authorization url
            auth_url = flow.step1_get_authorize_url()
            # open the url to get a code since its the first time
            # this will default to the redirect_uri you set earlier
            webbrowser.open(auth_url)

            codeStr = input("your code string here")

            credentials = flow.step2_exchange(codeStr)

            # save the code to the dat
            storage = oauth2client.file.Storage(self.dat_filename)
            storage.put(credentials)
            return credentials

        else:
            return credentials

    def get_service(self):
        credentials = self.get_credentials()
        http = httplib2.Http()
        http = credentials.authorize(http)
        service = apiclient.discovery.build(self.api_name,
                                            self.api_version,
                                            http=http)
        print(f"getting {self.api_name} service object")
        return service


class Analytics(GoogleAPI):
    """full core reporting documentation is here :\n
    https://developers.google.com/analytics/devguides/reporting/core/v4/
    """

    def get_report(self, query):
        """
        Since the requests can be tedious to put together instead it is requesting core elements.
        Not excluding to refactor the way things are handled later on though

        """
        analytics = self.service

        for n in range(0, 5):
            try:
                return analytics.reports().batchGet(
                    body={
                        'reportRequests': [
                            query
                        ]
                    }
                ).execute()
            except (HttpError) as error:
                if error.resp.reason in ['HttpError 429', 'userRateLimitExceeded', 'quotaExceeded',
                                         'internalServerError', 'backendError', 'Too Many Requests']:
                    time.sleep((2 ** n) + random.random())
                    print(error.resp.reason)
                else:
                    print("Will need to try some other time, critical failure !")
                    print(error.resp.reason)
                    break


class SearchConsole(GoogleAPI):

    """
    https://developers.google.com/webmaster-tools/
    only use request_to_df(self, property_uri, request) with the proper query template as described in the API
    to return a Dataframe that can be easily handled afterward for writing to Excel file/csv for instance
    only implemented methods are readonly scope for the API.

    """

    def account_properties(self, to_df=True):
        """
        get all the sites you have access to as well as the level of permission you have
        param:service is the service object for all Google APIs, requested when you initialise your Google API object
        return: raw response or response in DF

        """
        properties = self.service.sites().list().execute()

        if to_df:
            return pd.DataFrame(properties['siteEntry'])
        else:
            return properties['siteEntry']

    def search_analytics_data(self, property_uri, request):
        """Executes a searchAnalytics.query request.
        Args:
          service: The webmasters service to use when executing the query.
          property_uri: The site or app URI to request data for.
          request: The request to be executed.
        Returns:
          An array of response rows.
        """

        return self.service.searchanalytics().query(
            siteUrl=property_uri, body=request).execute()

    def search_analytics_to_df(self, property_uri, request):
        """
        You only have to use
        param:request : original request for the search console
        return:dataframe if the response is not empty

        """
        response = self.searchanalytics_data(property_uri, request)

        if 'rows' not in response:
            print('Empty response')
            return None
        else:

            data = response['rows']
            data_df = pd.DataFrame(data)

            df_dimensions = pd.DataFrame(data_df['keys'].values.tolist(),
                                         columns=request['dimensions'])

            response_df = pd.concat([df_dimensions,
                                     data_df],
                                    axis=1)

            response_df.drop("keys", axis=1, inplace=True)

            return response_df


if __name__ == '__main__':

    site = "https://www.myprotein.com/"

    start_date = "2019-06-01"
    end_date = "2019-09-09"

    # Get top 10 queries for the date range, sorted by click count, descending.
    search_console_request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': ['query'],
        'rowLimit': 10000
    }

    core_reporting_request = {
        "viewId": "38156032",
        "dateRanges": [{"startDate": "2018-02-02", "endDate": "2018-08-05"}],
        "metrics": [{"expression": "ga:sessions", "alias": "sessions"},
                    {"expression": "ga:transactionRevenue", "alias": "revenue"}],
        "dimensions": [{"name": "ga:landingPagePath"},
                       {"name": "ga:segment"}],
        "segments": [{"segmentId": "gaid::-5"}],
        "pageSize": 100,
        "samplingLevel": "LARGE"
    }

    """
    search_console = SearchConsole('webmasters',
                                   'v3',
                                   'MyCreds.dat',
                                   'https://www.googleapis.com/auth/webmasters.readonly',
                                   'client_secret.json',
                                   "urn:ietf:wg:oauth:2.0:oob")

    all_properties = search_console.account_properties(to_df=True)
    """

    core_reporting_analytics = Analytics('analyticsreporting',
                                         'v4',
                                         'ga_creds.dat',
                                         'https://www.googleapis.com/auth/analytics.readonly',
                                         'client_secret.json',
                                         "urn:ietf:wg:oauth:2.0:oob")

    print(core_reporting_analytics.get_report(core_reporting_request))

    # search_console_response_df = search_console.search_analytics_to_df(site, search_console_request)
