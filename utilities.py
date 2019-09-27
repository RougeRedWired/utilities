import os
import re
import lxml
import codecs
from lxml import etree
import lxml.html as lh
import xml.etree.ElementTree
import time
import pandas as pd
import requests
import uuid
import tldextract
import logging
import plotly.express as px
from urllib.parse import urlparse


__doc__ = """
This section should be where all diverse utilities are located, for all types of purposes (think netloc, dictionary of all websites, blogs etc...)
#to use this as a custom library you should have this at the top of your scripts :
import sys
sys.path.insert(0, "C:\\python_projects\\custom_libraries")
"""


def encode_and_bind(original_dataframe, feature_to_encode):
    """
    One hot encode features in a dataframe then drop the encoded feature.
    feature to encode is a string label

    """
    dummies = pd.get_dummies(original_dataframe[[feature_to_encode]])
    res = pd.concat([original_dataframe, dummies], axis=1)
    res = res.drop([feature_to_encode], axis=1)

    return(res)


def view_distrib(df=None,
                 column=""):
    """
    Takes a column return a saved graph to show rough distribution before proceeding to further analysis


    """

    df_test = pd.DataFrame(df[column].value_counts())
    df_test["rank"] = df_test.index

    print(df_test)

    fig = px.bar(df_test,
                 x="rank",
                 y=column
                 )
    fig.write_image(f"{column}_distribution.jpeg")

    return None


def return_exchange_rates(
        base="EUR",):
    """
    Call Exchange rate where :
    base : 3 letters internationally compliant format :
    https://en.wikipedia.org/wiki/ISO_4217

    """
    base_target_url = f"https://api.exchangerate-api.com/v4/latest/{base}"

    response = requests.get(base_target_url).json()

    return response["rates"]


def get_filetype_cwd(extension=""):
    """
    Get all files in the current working directory.
    extension:type:str=file extension you're looking for
    return:list
    """
    path = os.getcwd()
    return [file for file in os.listdir(path) if file.endswith(extension)]


def weird_chars():
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())


def create_folder(directory):
    """
    create folder with absolute/relative path.
    directory is a string like path.
    return:directory if successfully created
    else breaks
    """
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory
    except OSError:
        logger.info('Error: Creating directory. ' + directory)


def netloc(url):
    """
    extract the netloc from url
    """
    try:
        ext = tldextract.extract(url)
    except TypeError:
        print(url)
        return "type error"

    try:
        # This is because in some cases there is not subdomains.
        # in our case better to be more accurate
        final_subdomain = '.'.join(ext).strip(".")
        return final_subdomain
    except:
        return "error"


def extract_skus(df, columns):
    """
    Extract SKUS,
    :param df: dataframe where the SKU will be extracted from
    :param columns: columns where the SKU is.
    :return: dataframe with f"{column}_sku for each column

    """
    if type(columns) == str:
        columns = [columns]

    new_df = pd.DataFrame()

    print(columns)
    for column in columns:
        new_df[column] = df.loc[:, column].astype(str)
        new_column_name = "_".join([column, "sku"])
        print(new_df.columns.values)
        new_df[new_column_name] = new_df[column].str.extract("(\d{8})")
        print(new_df[new_column_name].head())

    return new_df


def get_hreflang_attribs(url="https://fr.myprotein.com"):
    """
    find equivalent page for the given locale
    (fr.myprotein after requesting us.myprotein.com

    """
    html_requests = requests.get(url)
    html_object = lh.fromstring(html_requests.text)
    found_locales = html_object.xpath("//link[@hreflang]/@href")
    found_language_codes = html_object.xpath("//link[@hreflang]/@hreflang")

    if len(found_locales) == 0:
        return html_requests.status_code
    else:
        return zip(found_language_codes, found_locales)


def download_section(locale_codes, headers=""):
    """
    download all section id from the site importer
    takes as an input : authorised username, user-agent headers
    example of headers = {"Authorization":login,
               "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
               }
    """
    for code in locale_codes:

        url = "".join(["https://tools.io.thehut.local/api/site-importer/sites/", code, "/sections/download"])
        response = requests.get(url, headers=headers, verify=False)
        print(response.url)
        output_filename = "".join([code, "-", "section_id.csv"])
        with open(output_filename, "wb") as response_csv:
            response_csv.write(response.content)
        print("created", output_filename)
        time.sleep(1)
    print("All good")
    return None


def get_live_list_pages(section_id_df, site_list):
    """
    parse a section id_file and download

    """
    live_lists = section_id_df[(section_id_df["enabled"] == True) &
                               (section_id_df["browserType"] == 0) &
                               (section_id_df["fullPath"].str.contains("myprotein-rebrand"))]

    site_mapping = site_list
    language_code = section_id_file.split("-")[1]
    print(language_code)
    locale = site_mapping.loc[site_mapping["lc"] == language_code, "site"]
    locale = list(locale)[0]
    live_lists["url"] = "https://" + live_lists["fullPath"].copy().str.replace("myprotein-rebrand", locale) + ".list"
    output_name = "_".join(["urls", section_id_file])

    live_lists["url"].to_csv(output_name, index=False)
    return output_name


def count_urls_by_type(df, column="Address"):
    """breaks down all main types of urls"""
    def remove_parameters(url):
        sep = ("?")
        return url.split(sep, 1)[0]

    df["Address_no_parameters"] = df[column].apply(remove_parameters)
    df.drop_duplicates(subset="Address_no_parameters", inplace=True)

    all_lists = df[(df["Address_no_parameters"].str.contains(".list"))]
    all_products = df[(df["Address_no_parameters"].str.contains(".html"))]
    all_reviews = df[(df["Address_no_parameters"].str.contains(".reviews"))]
    all_blogs = df[(df["Address_no_parameters"].str.contains("/thezone/")) |
                   (df["Address_no_parameters"].str.contains("/blog/"))]
    all_account = df[(df["Address_no_parameters"].str.contains(".account"))]
    all_trade = df[(df["Address_no_parameters"].str.contains(".trade"))]
    all_tesseract = df[(df["Address_no_parameters"].str.contains(".tesseract"))]
    all_uploads_cdn = df[(df["Address_no_parameters"].str.contains(".uploads-cdn.thgblogs.com"))]

    no_parameters_patt = "|".join(["/thezone/",
                                   "/blog/",
                                   "\.list",
                                   "\.html",
                                   "\.reviews",
                                   "\.account",
                                   "\.trade",
                                   "\.tesseract",
                                   "\.uploads-cdn.thgblogs.com"
                                   ])

    all_others = df[(df["Address_no_parameters"].str.contains(no_parameters_patt) == False)
                    ]
    non_indexable = df[(df["Address_no_parameters"].str.contains(".html"))]

    print(all_others.head())
    counts = {".list": len(all_lists),
              ".html": len(all_products),
              ".reviews": len(all_reviews),
              "blog": len(all_blogs),
              "other": len(all_others),
              "account": len(all_account),
              "trade": len(all_trade),
              "tesseract": len(all_tesseract),
              "cdn_url": len(all_uploads_cdn),
              "total": len(df)
              }
    print(counts)

    return counts


def check_presence(function, output_filename, files):
    if output_filename not in files:
        return function
    else:
        print(output_filename, "already exists, passing")
        pass


def merge_csv_files(file_list=[], destination_file=str(uuid.uuid4())[:8] + "destination.csv"):
    """
    Merge csv files together.
    To use when you're dealing with huge csv.
    file_list:list of file. absolute path recommended
    destination_file:str default: str(uuid.uuid4())[:8] + "destination.csv"
    """
    for file in file_list:
        for chunk in pd.read_csv(file, chunksize=100000):
            chunk.to_csv(destination_file, mode="a", index=False)
    logger.info("finished")
    return None


def get_page(url):
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
    header = {"user-agent": user_agent}

    response = requests.get(url)
    return response
