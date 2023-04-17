import pandas as pd
from geopy import distance


def calc_end_to_end_distance(coordinates: pd.Series) -> float:
    coords_start = list(coordinates.iloc[0].values())
    coords_end = list(coordinates.iloc[-1].values())

    return distance.distance(coords_start, coords_end).km.real
