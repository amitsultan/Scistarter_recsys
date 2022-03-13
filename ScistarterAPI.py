import requests
import yaml
import json
import pandas as pd
import os.path
import numpy as np
from AWSHandler import IPHandler
from geopy.distance import geodesic


def get_coordinates(coords_string):
    try:
        coords_string = coords_string.replace("'", "\"")
        coords = json.loads(coords_string)['coordinates']
        return tuple([coords[1], coords[0]])
    except Exception as e:
        return None


class ScistarterAPI:
    """Zooniverse API class handels API requests and collection of opportunities data."""
    
    def __init__(self):
        """Initializing the API handler. saves the API base URL and load the configuration file.
        
        configuration file:
            Should be called "scistarter_cfg.yml" and contain two components:
                endpoints: contains the relevant endpoints to communicate with Scistarter API
                    opportunities_list: should be the opportunity end point where blank returns all
                     opportunities and with an UID provided returns specific opportunity data.
                dictionary_keys: This holds the response json keys for genetric use, current one is matches
                 and it holds the opportunities array.
        Args:
        """
        with open("scistarter_cfg.yml", "r") as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        self.BASE_URL = cfg['Scistarter']['base_url'] 
        self.endpoints = cfg['endpoints']  # This hold a dictionary of the relevant endpoints for scistarter API
        self.scistarter_dict_keys = cfg['dictionary_keys']  # This holds the response json keys for genetric use
        self.opportunities = None
        self.opportunities_df = None
        self.ip_handler = IPHandler()
    
    def send_request(self, url, success_msg=None, failed_msg=None):
        """ Generic method for sending API requests.

        Args:
            url(string): URL to send a specific request. success_msg=None, failed_msg=None
            success_msg (string):  Optional - print a success message upon reciving status_code 200.
            failed_msg (string):   Optional - print a failed message upon reciving status_code other than 200.

        Returns:
            object: Response in JSON format.
        """
        response = requests.get(url)
        # decide how to handle a server that's misbehaving to this extent
        if response.status_code == 200:
            if success_msg is not None:
                print(success_msg)
            try:
                return response.json()
            except ValueError:
                return None
        else:
            if failed_msg is not None:
                print(failed_msg)
            return None
    
    def get_opportunities(self):  
        """ Retrieve list of all opportunities found with partial information and saves them in the ScistarterAPI object.
        Args:
        Returns:
            object: List of opportunities.
        """
        
        url = f'{self.BASE_URL}{self.endpoints["opportunities_list"]}'
        try:
            data = self.send_request(url)
        except Exception as e:
            print(f'Failed to send request with error: {e}')
            return None
        if data is not None:
            self.opportunities = data[self.scistarter_dict_keys['opportunities_json_key']]
            print(f'Loaded: {len(self.opportunities)} opportunities')
            return self.opportunities
        else:  # Couldn't fetch opportunities
            return None
    
    # Get information about a given opportunity, if fields not None get only those fields
    def get_opportunity_info(self, uid, fields=None):
        """ Retrieve a specific opportunity full information.
        Args:
            uid(string): UID - opportunity unique identifier.
            fields(list): Optional - List of fields names to filter by out of all the opportunity information.
        Returns:
            object: List of opportunities.
        """

        url = f'{self.BASE_URL}{self.endpoints["opportunities_list"]}{uid}'
        data = self.send_request(url)
        if fields is not None and data is not None:
            data = {k: v for k, v in data.items() if k in fields}
        return data


    # Method will load a csv file into a dataframe object.
    # if file not exist, it will create the dataframe and save it in the given path
    # Lazy_load - if True, only reads the csv file, if False it will update each row to
    #               by calling scistarter API (very expensive)
    # If file doesn't exist -> Lazy_load will be False as there is no file to read.
    def load_opportunities_df(self, path, fields=None, lazy_load=True):
        """ Method loads a dataframe if available and if not, it will execute the relevant
            API's calls to create the dataframe and save it at the given path.
            By default this method is lazy meaning it prefer to avoid making all the API calls
            each time, it will only call for new items.

        Args:
            path(string): UID - Path to load or save the DataFrame once created.
            fields(list): Optional - List of fields names to filter by out of all the opportunity information.
            lazy_load(bool): Default is True. 
                        If True - It will read the dataframe from the given path and will only add new opportunities
                                  and will not check to see if previous opportunities changed.
                        If False - It will create the DataFrame from screatch and will save it at the given path
                                   this behaviour is very expensive thus try to avoid it.
        Returns:
            object: List of opportunities.
        """
        if fields is None:
            fields = ['location_point',
                      'start_datetimes',
                      'has_end',
                      'end_datetimes']

        if self.opportunities is None:  # Make sure opportunities general info is loaded
            response = self.get_opportunities()
            if response is None:
                raise Exception("Couldn't load opportunities, please make sure the base URL and the endpoints are correct!")
        
        if not os.path.isfile(path):
            lazy_load = False  # Must create DataFrame from scratch
            
        if lazy_load:
            self.opportunities_df = pd.read_csv(path)
            updated_uids = np.array([opp['uid'] for opp in self.opportunities])
            missing_uids = np.setdiff1d(updated_uids, self.opportunities_df['uid'])  # Checking if our file contains all uids
            if len(missing_uids) > 0:
                missing_uids_info = []
                missing_uids_partial_df = pd.DataFrame(self.opportunities)
                missing_uids_partial_df = missing_uids_partial_df[missing_uids_partial_df.uid.isin(missing_uids)]
                for missing_uid in missing_uids:
                    missing_uids_info.append(self.get_opportunity_info(missing_uid, fields))
                uids_missing_info_df = pd.DataFrame(missing_uids_info)
                missing_df = pd.concat([missing_uids_partial_df.reset_index(drop=True), uids_missing_info_df.reset_index(drop=True)], axis=1)
                try:
                    self.opportunities_df = pd.concat([self.opportunities_df, missing_df])
                except Exception as e:
                    print('Failed to merge DataFrame, this can be a cause of differentiate in DataFrame fields')
        else:
            self.opportunities_df = pd.DataFrame(self.opportunities)
            additional_info = self.opportunities_df['uid'].swifter.apply(lambda uid: self.get_opportunity_info(uid, fields))
            additional_info_df = pd.DataFrame(additional_info.tolist())
            self.opportunities_df = pd.concat([self.opportunities_df, additional_info_df], axis=1)
        self.opportunities_df['cords'] = self.opportunities_df['location_point'].apply(lambda point: get_coordinates(point))
        self.opportunities_df.to_csv(path, index=False)

    def recommend_user(self, ip_addr, N, max_distance=None):
        """ Method Recommend opportunities for a user based on their IP location.

        Args:
            ip_addr(string): IP Address of the user.
            N(int): Number of recommendations
            max_distance(int): Max distance for the recommendations, will not recommend anything
                                further than max_distance. if None, will return the closest-N.
        Returns:
            object: List of most closest opportunities ID's, opportunities must not be "Ended".
        """
        uid_recommendations = []
        if self.opportunities_df is None:
            print('No opportunities DataFrame was found, loading opportunities please wait...')
            self.load_opportunities_df("opportunities.csv")
            print('Opportunities loaded, created a file name opportunities.csv')
        ip_info = self.ip_handler.lookup_ip(ip_addr)
        if ip_info is None:
            return uid_recommendations
        location = ip_info['loc'].split(',')
        location_point = (location[0], location[1])
        still_active_opportunities = self.opportunities_df[self.opportunities_df['has_end'].eq(False)]
        possible_opportunities = still_active_opportunities['cords'].\
            apply(lambda cord: geodesic(cord, location_point).km)
        if max_distance is not None:
            possible_opportunities = possible_opportunities[possible_opportunities <= max_distance]
        possible_opportunities.sort_values(inplace=True, ascending=True)
        possible_opportunities = possible_opportunities[:N]
        uid_recommendations = self.opportunities_df.iloc[possible_opportunities.index]['uid'].values
        return uid_recommendations
