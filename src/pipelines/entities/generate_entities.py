from os import path
import re
import pandas as pd


def _get_project_dir_folder():
    return path.dirname(path.dirname(path.dirname(path.dirname(__file__))))


def _generate_entities(dfhdb):
    print("start: _generate_entities()")
    # Flat (unit)
    dfflat = dfhdb[
        [
            "address",
            "minimum_floor",
            "maximum_floor",
            "sqft",
            "flat_type",
        ]
    ].copy()
    dfflat.drop_duplicates(inplace=True)
    dfflat.sort_values("address", inplace=True)
    # Block
    dfblockfloor = dfhdb[
        [
            "address",
            "maximum_floor",
        ]
    ].copy()
    dfblockfloor.drop_duplicates(inplace=True)
    dfblock = dfhdb[
        [
            "address",
            "road",
            "town",
            "built_year",
            "lease_remaining",
            "band_name",
        ]
    ].copy()
    dfblock.drop_duplicates(inplace=True)
    dfblock = dfblock.merge(dfblockfloor, on="address")

    def _mapname(x):
        ad, bn = x.get("address"), x.get("band_name")
        return f"{ad} [{bn}]"

    dfblock["map_name"] = dfblock[["address", "band_name"]].apply(_mapname, axis=1)
    dfblock.sort_values("address", inplace=True)
    # Transaction
    dftxn = dfhdb[
        [
            "address",
            "road",
            "minimum_floor",
            "maximum_floor",
            "sqft",
            "flat_type",
            "year_of_sale",
            "month_of_sale",
            "price",
            "psf",
            "town",
            "built_year",
            "band_name",
        ]
    ].copy()
    dftxn.rename(
        columns={
            "band_name": "lease_band_name",
        },
        inplace=True,
    )
    dftxn["price_band_name"] = dftxn["price"] / 25000
    dftxn["price_band_name"] = dftxn["price_band_name"].round() * 25000
    dftxn["price_band_name"] = dftxn["price_band_name"].astype(int)

    dftxn["psf_band_name"] = dftxn["psf"] / 5
    dftxn["psf_band_name"] = dftxn["psf_band_name"].round() * 5
    dftxn["psf_band_name"] = dftxn["psf_band_name"].astype(int)
    print("end: _generate_entities()")
    return dftxn, dfflat, dfblock


def main(datamart_fp, txnentity_fp, flatentity_fp, blockentity_fp):
    print("Start: main()")
    ASSETS_FP = path.join(_get_project_dir_folder(), "assets")

    datamart_folder_path = path.join(ASSETS_FP, "datasets", "datamart")
    entities_folder_path = path.join(ASSETS_FP, "datasets", "entities")

    # Read
    hdb_filepath = path.join(datamart_folder_path, datamart_fp)
    dfhdb = pd.read_csv(hdb_filepath)
    print(f"dfhdb.shape={dfhdb.shape}")
    txn_df, flat_df, block_df = _generate_entities(dfhdb)

    txns_filepath = path.join(entities_folder_path, txnentity_fp)
    flats_filepath = path.join(entities_folder_path, flatentity_fp)
    blocks_filepath = path.join(entities_folder_path, blockentity_fp)

    for d, f in zip(
        [txn_df, flat_df, block_df], [txns_filepath, flats_filepath, blocks_filepath]
    ):
        d.to_csv(f, index=False)
        print(f"Wrote to {f}. df.shape={d.shape}")
    print("Done: main()")


if __name__ == "__main__":
    datamart_fp = "datamart.csv"
    txnentity_fp = "entity-transaction.csv"
    flatentity_fp = "entity-flat.csv"
    blockentity_fp = "entity-block.csv"
    main(datamart_fp, txnentity_fp, flatentity_fp, blockentity_fp)
