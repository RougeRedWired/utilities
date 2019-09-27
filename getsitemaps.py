import requests
import pandas as pd
from lxml import etree

"""
#to use this as a custom library you should have this at the top of your scripts :
import sys
sys.path.insert(0, "C:\\python_projects\\custom_libraries")
"""


def get_sitemaps(list_of_sites=[],
                 slugs=["post-sitemap.xml", "post_sitemap_1.xml", "post_sitemap_2.xml", "post_sitemap_3.xml", "post-sitemap1.xml", "post-sitemap2.xml", "post-sitemap3.xml"],):
    """
    Pass a list of sites you want to check the sitemap for,
    :params: list of sites like this : https://www.example.com
    :return: concatenated dataframe
    """

    def get_page(url):
        return requests.get(url)

    def write_xml_response_to_file(response, filename="xml_sitemap.xml"):
        """
        writes to file and return filename so that it can be passed to processing function
        """
        with open(filename, "wb") as output:
            output.write(response.content)
        return filename

    def parse_sitemap(xml_file_path):
        """
        parse sitemap to get urls, and return it with last modified nicely formatted into a dataframe.
        ready to be put as a csv
        """
        parser = etree.XMLParser(ns_clean=True)
        tree = etree.parse(xml_file_path)
        root = tree.getroot()
        data = [[url.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")[0].text for url in root], [url.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod")[0].text for url in root]]
        data_df = pd.DataFrame(data).T
        data_df.columns = ["url", "last_modified"]
        return data_df

    def build_sitemap_urls(site_list=[], slug_list=[]):
        """slug is all the stuff common to a pattern, adapt according to case"""
        all_urls = ["".join([site, slug]) for site in site_list for slug in slug_list]
        return all_urls

    list_of_sites = list_of_sites
    # Could be replaced by a file if needed
    slugs = list(set(slugs))
    sitemap_dfs = []
    for url in build_sitemap_urls(list_of_sites, slugs):

        response = get_page(url)
        if response.status_code != 404:
            print(url)
            print(response.status_code)
            write_xml_response_to_file(response)
            # write xml create xml file and return the name to be passed in the parser function
            urls_in_sitemap = parse_sitemap(write_xml_response_to_file(response))
            sitemap_dfs.append(urls_in_sitemap)
        else:
            print(response.status_code)
            pass
    return pd.concat(sitemap_dfs, ignore_index=True)
