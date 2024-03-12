from os import path
import re
import pandas as pd
import pendulum


def _get_project_dir_folder():
    return path.dirname(path.dirname(path.dirname(path.dirname(__file__))))


def _from_source(downloaded_folder_path, files):
    print("start: _from_source()")
    df_list = []
    for fle in files:
        df_from_file = pd.read_csv(path.join(downloaded_folder_path, fle))
        print(f"filename: {fle} df.shape={df_from_file.shape}")
        df_list.append(pd.read_csv(path.join(downloaded_folder_path, fle)))

    concat_df = pd.concat(df_list)
    df = concat_df.copy()
    l = df.columns.tolist()

    # Sale year / month
    df["x0a_sale_year"] = df["month"].apply(lambda x: x.split("-")[0])
    df["x0a_sale_year"] = df["x0a_sale_year"].astype(int)
    df["x0b_sale_month"] = df["month"].apply(lambda x: x.split("-")[1])
    df["x0b_sale_month"] = df["x0b_sale_month"].astype(int)

    # Address and Town
    def _address(x):

        blk, st = x.get("block"), x.get("street_name")
        return f"{blk} {st}"

    df["x1a_address"] = df[["block", "street_name"]].apply(_address, axis=1)
    df["x1b_block"] = df["block"].apply(lambda x: f"B-{x}")
    df["x1c_road"] = df["street_name"]
    df["x1d_town"] = df["town"]

    # Storey
    df["x3a_storey_min"] = df["storey_range"].apply(lambda x: x.split()[0])
    df["x3a_storey_min"] = df["x3a_storey_min"].astype(int)
    df["x3b_storey_max"] = df["storey_range"].apply(lambda x: x.split()[2])
    df["x3b_storey_max"] = df["x3b_storey_max"].astype(int)

    # Lease
    df["x4b_lease_commence_year"] = df["lease_commence_date"]
    df["x4b_lease_commence_year"] = df["x4b_lease_commence_year"].astype(int)

    # Area and Flat Type
    df["x5a_area_sqft"] = df["floor_area_sqm"] * 10.7639
    df["x5a_area_sqft"] = df["x5a_area_sqft"].astype(int)
    df["x5b_area_sqm"] = df["floor_area_sqm"].astype(int)
    df["x5c_flat_type"] = df["flat_type"]

    # Price and PSF
    df["y1a_price"] = df["resale_price"]
    df["y1a_price"] = df["y1a_price"].astype(int)
    df["y1b_psf"] = df["y1a_price"] / df["x5a_area_sqft"]
    df["y1b_psf"] = (df["y1b_psf"] * 100).astype(int) / 100

    df.drop(l, axis=1, inplace=True)
    print(f"out_df.shape={df.shape}")
    # print(df)
    print("end: _from_source()")
    return df


def _from_ingested(df):

    def road(x):
        TOKENS_SUBSTITUTE = {
            "AVE": "AVENUE",
            "BT": "BUKIT",
            "C'WEALTH": "COMMONWEALTH",
            "CL": "CLOSE",
            "CRES": "CRESCENT",
            "CTRL": "CENTRAL",
            "DR": "DRIVE",
            "GDNS": "GARDENS",
            "JLN": "JALAN",
            "KG": "KAMPUNG",
            "LOR": "LORONG",
            "LOR": "LORONG",
            "NTH": "NORTH",
            "PK": "PARK",
            "PL": "PLACE",
            "RD": "ROAD",
            "ST": "STREET",
            "ST.": "SAINT",
            "STH": "SOUTH",
            "TER": "TERRACE",
            "TG": "TANJONG",
            "UPP": "UPPER",
        }
        x_list = x.split()
        x_list_out = []
        for t in x_list:
            x_list_out.append(TOKENS_SUBSTITUTE.get(t, t))
        return " ".join(x_list_out)

    print("start: _from_ingested()")
    out_df = df.copy()

    # Transformations
    ### ### ### ### ###
    # Address
    def address(x):
        return x.get("x1b_block")[2:] + " " + x.get("x1c_road")

    out_df["address"] = out_df[["x1b_block", "x1c_road"]].apply(address, axis=1)
    out_df["address"] = out_df["address"].apply(road)
    # Road
    out_df["road"] = out_df["x1c_road"].apply(road)
    # Town
    out_df["town"] = out_df["x1d_town"]
    out_df.rename(
        columns={
            "x0a_sale_year": "year_of_sale",
            "x0b_sale_month": "month_of_sale",
            "x3a_storey_min": "min_storey",
            "x3b_storey_max": "max_storey",
            "x4b_lease_commence_year": "built_year",
            "x5a_area_sqft": "sqft",
            "x5b_area_sqm": "sqm",
            "x5c_flat_type": "flat_type",
            "y1a_price": "price",
            "y1b_psf": "psf",
        },
        inplace=True,
    )
    # Select columns
    ### ### ### ### ###
    out_df = out_df[
        [
            "year_of_sale",
            "month_of_sale",
            "min_storey",
            "max_storey",
            "built_year",
            "sqft",
            "sqm",
            "flat_type",
            "price",
            "psf",
            "address",
            "road",
            "town",
        ]
    ]
    print(f"out_df.shape={out_df.shape}")
    print("end: _from_ingested()")
    return out_df


def _from_preprocessed(df, bands_absfp):
    print("start: _from_preprocessed()")
    dfband = pd.read_csv(bands_absfp)
    dfhdb = df.copy()

    # Transform
    dfhdb.rename(
        columns={
            "min_storey": "minimum_floor",
            "max_storey": "maximum_floor",
        },
        inplace=True,
    )
    now = pendulum.now()
    thisyear = now.year
    dfhdb["lease_remaining"] = 99 - thisyear + dfhdb["built_year"]

    # Select Columns
    dfhdb = dfhdb[
        [
            "address",
            "lease_remaining",
            "minimum_floor",
            "maximum_floor",
            "built_year",
            "sqft",
            "sqm",
            "flat_type",
            "road",
            "town",
            "year_of_sale",
            "month_of_sale",
            "price",
            "psf",
        ]
    ].copy()

    # merge with band
    dfhdb = dfhdb.copy().merge(dfband, on="lease_remaining", how="left")
    print(f"out_df.shape={dfhdb.shape}")
    print("end: _from_preprocessed()")
    return dfhdb


def main(files, bands_fp, out_filename):
    print("Start: main()")
    ASSETS_FP = path.join(_get_project_dir_folder(), "assets")

    downloaded_folder_path = path.join(ASSETS_FP, "datasets", "downloaded")
    external_folder_path = path.join(ASSETS_FP, "datasets", "external")
    datamart_folder_path = path.join(ASSETS_FP, "datasets", "datamart")

    out_df = _from_source(downloaded_folder_path, files)

    out_df = _from_ingested(out_df)

    bands_absfp = path.join(external_folder_path, bands_fp)
    out_df = _from_preprocessed(out_df, bands_absfp)

    out_fp = path.join(datamart_folder_path, out_filename)
    out_df.to_csv(path.join(datamart_folder_path, out_filename), index=False)
    print(f"Wrote to {out_fp}")
    print("Done: main()")


if __name__ == "__main__":
    files = [
        "ResaleFlatPricesBasedonApprovalDate19901999.csv",
        "ResaleFlatPricesBasedonRegistrationDateFromMar2012toDec2014.csv",
        "ResaleFlatPricesBasedonApprovalDate2000Feb2012.csv",
        "ResaleflatpricesbasedonregistrationdatefromJan2017onwards.csv",
        "ResaleFlatPricesBasedonRegistrationDateFromJan2015toDec2016.csv",
    ]
    bands_fp = "bands.csv"
    out_filename = "datamart.csv"
    main(files, bands_fp, out_filename)
