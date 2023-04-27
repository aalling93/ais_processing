import pandas as pd
import numpy as np
import collections






def get_datasets(
    df: pd.core.frame.DataFrame, lookback_offset: int = 1, lookback: int = 5, target_observations: int = 1
):

    sample = (
        df.groupby("ids")
        .apply(
            datasets_training,
            lookback_offset=lookback_offset,
            lookback=lookback,
            target_observations=target_observations,
        )
        .apply(list)
    )

    samples = np.array([sam[0] for sam in sample],dtype=object)
    targets = np.array([sam[1] for sam in sample],dtype=object)
    features = sample[0][2]



    return samples, targets, features


def datasets_training(
    df: pd.core.frame.DataFrame, lookback_offset: int = 1, lookback: int = 5, target_observations: int = 1
):
    samples = []
    targets = []
    for rows in range(
        lookback_offset, df.shape[0] - lookback - target_observations + 1
    ):
        samples.append(df.iloc[rows : rows + lookback, :].to_numpy())
        targets.append(
            df.iloc[
                rows + lookback : rows + lookback + target_observations, :
            ].to_numpy()
        )

    #
    return np.array(samples), np.array(targets), np.array(df.columns.values)





def raw2clean(
    df_raw: pd.core.frame.DataFrame, t_messages=100, t_pausetime=1000, f_resampling=10, t_maxlengt=300, verbose=0
):
    """

    t_messages = 100 # threshold for number of messages
    t_pausetime = 1000 #threshold for pause time
    f_resampling = 10 #resampling frequence
    t_minlengt = 10 # minimum length of sequences.
    t_maxlengt = 300 # maximum length of sequences. sequence time = f_resampling*samples.


    """
    cargo_df = df_for_shiptype(df_raw, t_messages)
    if verbose > 0:
        print(len(cargo_df))
    del df_raw

    cargo_df_split = df_split_ais(cargo_df, t_pausetime)
    if verbose > 0:
        print(len(cargo_df_split))
    del cargo_df

    cargo_df_split_resampl = df_resampling(cargo_df_split, f_resampling)
    if verbose > 0:
        print(len(cargo_df_split_resampl))
    del cargo_df_split

    cargo_df_split_resampl_maxlength = df_splt_smaller_seq(
        cargo_df_split_resampl, t_maxlengt
    )
    if verbose > 0:
        print(len(cargo_df_split_resampl_maxlength))
    del cargo_df_split_resampl

    cargo_df_split_resampl_maxlength_corrected = df_correction(
        cargo_df_split_resampl_maxlength
    )
    if verbose > 0:
        print(len(cargo_df_split_resampl_maxlength_corrected))
    del cargo_df_split_resampl_maxlength

    testing_set = df_to_numpy_training(cargo_df_split_resampl_maxlength_corrected)

    if verbose > 0:
        print(testing_set.shape)

    return testing_set, cargo_df_split_resampl_maxlength_corrected


def make_dataset_single(
    df: pd.core.frame.DataFrame, lookback: int = 5, lookback_offset: int = 0, target_observations: int = 0
):
    samples = []
    targets = []
    for rows in range(
        lookback_offset, df.shape[0] - lookback - target_observations + 1
    ):
        samples.append(df.iloc[rows : rows + lookback, :].to_numpy())
        targets.append(
            df.iloc[
                rows + lookback : rows + lookback + target_observations, :
            ].to_numpy()
        )
    return samples, targets


def add_datasets_all(df: pd.core.frame.DataFrame):
    try:
        samples, targets = df.groupby("ids").apply(make_dataset_single)
        return samples, targets
    except:
        pass


def get_index(
    samples_description: list,
    parms=[
        "lat",
        "lon",
        "sog",
        "cog",
        "Total_distance",
        "Running_Distance",
        "delta_lat",
        "delta_lon",
    ],
):
    features = samples_description.isin(parms)
    features = pd.Series(features)
    features = features[features].index

    return features
