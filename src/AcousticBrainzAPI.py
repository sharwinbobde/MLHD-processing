#  Copyright (c) 2020. Sharwin P. Bobde. Location: Delft, Netherlands.
#  Coded for final Thesis project for Masters in Computer Science at Delft university of Technology.
#  Contact s.p.bobde@student.tudelft.nl or bsharwin@gmail.com for any queries.

import requests as req
from ratelimit import limits, RateLimitException, sleep_and_retry
from backoff import on_exception, expo
import config
import src.arangodb_functions as af

acousticBrainz_API_URL = "https://acousticbrainz.org"

test_recording_MBIDs = [
    '012f832d-bac1-4ff3-b195-d3ed4705cd58',
    '02010cef-9e2e-4314-99b2-4392745f48b7',
    '03898c3f-8c6e-413d-9f02-bd2d24a7b2e4',
    'a0f28cff-4cc9-4401-aa9e-db6fa478f767',
    '09c897c6-75ee-4574-8054-812b184c66eb',
]


# commented to use in future if high-level features will be used
# @on_exception(expo, RateLimitException, max_tries=config.AcousticBrainz_rate_limit_exponential_backoff_max_attempts)
# @limits(calls=config.AcousticBrainz_rate_limit_calls, period=config.AcousticBrainz_rate_limit_period)
# def get_high_level_features(self, mbids: list):
#     if len(mbids) > config.AcousticBrainz_max_MBIDs_per_request:
#         raise print("Cannot ask for more than 25 MBIDs at once")
#     query = {'recording_ids': mbids}
#     response = req.get(acousticBrainz_API_URL + "/api/v1/high-level",
#                        params=query)
#     if response.status_code != 200:
#         raise Exception('API response: {}'.format(response.status_code))
#     return response.json()

# @on_exception(expo, RateLimitException, max_tries=config.AcousticBrainz_rate_limit_exponential_backoff_max_attempts)
@sleep_and_retry
@limits(calls=config.AcousticBrainz_rate_limit_calls, period=config.AcousticBrainz_rate_limit_period)
def get_low_level_features(mbids: list):
    if len(mbids) > config.AcousticBrainz_max_MBIDs_per_request:
        raise Exception("Cannot ask for more than 25 MBIDs at once")
    query = {'recording_ids': mbids}
    response = req.get(acousticBrainz_API_URL + "/api/v1/low-level",
                       params=query)
    if response.status_code != 200:
        raise Exception('API response: {}'.format(response.status_code))
    return response.json()


if __name__ == '__main__':
    pass

    # Test scripts
    # af.get_batched_recordings()

    # res = af.get_count_recordings()
    # data = res.__next__()
    # print(data)


    # for tries in range(1000):
    #     res = get_low_level_features(test_recording_MBIDs)
    #     print("here")
        # for i in test_recording_MBIDs:
        #     try:
        #         print("\n\n")
        #         print(res[i])
        #     except KeyError:
        #         pass

    # print(res['mbid_mapping'])
