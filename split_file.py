import pandas as pd


def split_file(df=None, step=1, output_name=""):
    """
    param:df = dataframe to split DataFrame object
    param:step= number of fragments, default 1
    output name:string


    """

    len_df = len(data_set_links)
    print(f" is the original length of the table {len(data_set_links)}")

    fragment_length = int(len_df / step)
    print(f"{fragment_length} is the length of the fragment")

    for i in range(1, step + 1):

        print(i)
        writer = pd.ExcelWriter(f"{i}_{output_name}.xlsx", options={'strings_to_urls': False})

        batch_link_data = df.iloc[(i - 1) * fragment_length:i * fragment_length]
        print(len(batch_link_data))
        assert len(batch_link_data) < (fragment_length + 1)
        batch_link_data.to_excel(writer, index=False)
        writer.save()
    print("Done")
    return None


def read_datafile(filename):
    if filename.endswith(".xlsx"):
        return pd.read_excel(filename)
    elif filename.endswith(".csv"):
        return pd.read_csv(filename)
    else:
        raise TypeError("Only xlsx or csv are allowed")


if __name__ == '__main__':
    """
    put your filename between the double quote in front of filename
    step will be the number of fragments you are going to have.
    pick your number
    output_name : the name you want for your fragmented file. It will include the fragment number as well
    don't touch the function above unless you know what you are doing


    """
    # can be either a csv or a xlsx
    filename = ""

    step = 1
    output_name = ""

    file_df = read_datafile(filename)

    split_file(data_set_links=file_df,
               step=step,
               output_name=output_name)
