import pandas as pd


def remove_nan(df: pd.core.frame.DataFrame):
    """ """
    df = df.dropna(subset=["lat", "long", "sog", "cog"])
    return df


def round_data(df: pd.core.frame.DataFrame, round_coord: int = 6, round_sog: int = 1, round_cog: int = 0):
    df.lat = df.lat.round(round_coord)
    df.long = df.long.round(round_coord)
    df.sog = df.sog.round(round_sog)
    df.cog = df.cog.round(round_cog)

    return df


def filter_lat(df: pd.core.frame.DataFrame, lat_min: float = -180, lat_max: float = 180):
    df = df[df.lat > lat_min]
    df = df[df.lat < lat_max]

    return df


def filter_long(df: pd.core.frame.DataFrame, long_min: float = -180, long_max: float = 180):
    df = df[df.long > long_min]
    df = df[df.long < long_max]

    return df


def filter_sog(df: pd.core.frame.DataFrame, sog_min: float = 0, sog_max: float = 210):
    df = df[df.sog > sog_min]
    df = df[df.sog < sog_max]

    return df


def filter_cog(df: pd.core.frame.DataFrame, cog_min: float = 0, cog_max: float = 360):
    df = df[df.cog > cog_min]
    df = df[df.cog < cog_max]

    return df