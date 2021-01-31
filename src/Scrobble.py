#  Copyright (c) 2020. Sharwin P. Bobde. Location: Delft, Netherlands.
#  Coded for final Thesis project for Masters in Computer Science at Delft university of Technology.
#  Contact s.p.bobde@student.tudelft.nl or bsharwin@gmail.com for any queries.

import numpy as np
import re
import src.arangodb_functions as af
import pandas as pd
import config
import gc

coding_kernal_matrix = np.array([[0b100], [0b010], [0b001]], dtype=np.uint8)
from collections import defaultdict


class ScrobbleProcessor:
    def __init__(self, filename: str, part: int):
        self.filename = filename
        self.part = part

        self.artists = set()
        self.recordings = set()

        self.users_to_artists = []
        self.users_to_recordings = []
        self.artists_to_recordings = []

    def collect_nodes_and_edges_from_df(self, user, df: pd.DataFrame):
        # user to artist; artist should exist
        df1 = df[df.fpc & 0b100 == 0b100]
        valid = df1 \
            [['timestamp', 'artist']] \
            .groupby(['artist']) \
            .count() \
            .reset_index()
        valid = valid[valid.timestamp > config.listen_lower_threshold].artist.tolist()
        df1 = df1[df1['artist'].isin(valid)] \
            [['timestamp', 'artist', 'year']] \
            .groupby(['artist', 'year']) \
            .count() \
            .reset_index()

        # dict of dict
        u_2_a_temp = defaultdict(dict)
        for index, x in df1.iterrows():
            if x[2] >= config.listen_lower_threshold / 2:
                u_2_a_temp[x[0]]["yr_" + str(x[1])] = x[2]
        for key in u_2_a_temp:
            self.users_to_artists.append({
                "_from": "users/" + user, "_to": "artists/" + key,
                "years": u_2_a_temp[key],
                "part": self.part

            })

        # ===========================================
        self.artists = df1['artist'] \
            .apply(lambda x: {"_key": x}) \
            .to_numpy().tolist()

        # ===========================================

        # user to recording; recording should exist
        df2 = df[df.fpc & 0b001 == 0b001]

        valid = df2 \
            [['timestamp', 'recording', 'year']] \
            .groupby(['recording', 'year']) \
            .count() \
            .reset_index()
        valid = valid[valid.timestamp > config.listen_lower_threshold].recording.tolist()
        df2 = df2[df2['recording'].isin(valid)] \
            [['timestamp', 'recording', 'year']] \
            .groupby(['recording', 'year']) \
            .count() \
            .reset_index()

        # dict of dict
        u_2_r_temp = defaultdict(dict)
        for index, x in df2.iterrows():
            u_2_r_temp[x[0]]["yr_" + str(x[1])] = x[2]
        for key in u_2_r_temp:
            self.users_to_recordings.append({
                "_from": "users/" + user, "_to": "recordings/" + key,
                "years": u_2_r_temp[key],
                "part": self.part

            })

        # ===========================================
        self.recordings = df2['recording'] \
            .apply(lambda x: {"_key": x}) \
            .to_numpy().tolist()

        # ===========================================

        # artist to recording; artist and recording should exist
        df3 = df[df.fpc & 0b101 == 0b101] \
            [['timestamp', 'artist', 'recording', 'year']] \
            .groupby(['artist', 'recording', 'year']).count() \
            .query('timestamp >= ' + str(config.listen_lower_threshold)) \
            .reset_index()

        self.artists_to_recordings = df3 \
            .apply(
            lambda x: {
                "_from": "artists/" + x[0], "_to": "recordings/" + x[1],
                "part": self.part
            },
            axis=1) \
            .to_numpy().tolist()

    def free_memory(self):
        del self.filename

        del self.artists
        del self.recordings

        del self.users_to_artists
        del self.users_to_recordings
        del self.artists_to_recordings

    def start(self):
        # print("here")
        user = re.search(r'(?!\/)[^\/\.]*(?=\.)', self.filename).group(0)
        df = pd.read_csv(self.filename, sep='\t', header=None, low_memory=False)

        df = df.rename(columns={"0": "timestamp", "1": "artist", "2": "release", "3": "recording"})
        scrobble = df.to_numpy()
        is_present = np.vectorize(lambda x: isinstance(x, str))(scrobble[:, 1:])
        fpc = np.matmul(is_present.astype(np.uint8), coding_kernal_matrix).reshape((-1,))
        mask = ~np.in1d(fpc, config.invalid_field_presence_codes)
        # selected = np.nonzero(mask)[0]

        df.columns = ["timestamp", "artist", "release", "recording"]
        df = df.assign(valid=mask)
        df = df.assign(fpc=fpc)
        df = df.drop(["release"], axis=1)
        df = df.loc[df["valid"]]  # faster filter

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df['year'] = df['timestamp'].dt.year

        self.collect_nodes_and_edges_from_df(user, df)

        af.add_nodes(user, self.artists, self.recordings)
        af.add_edges(self.users_to_artists, self.users_to_recordings, self.artists_to_recordings)

        # print("done")
