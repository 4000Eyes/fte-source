import datetime
from neotime import DateTime


def ft_serialize_datetime(dt_neo_date_time):
    print ("The year is", dt_neo_date_time.year)
    dtOutput = datetime.datetime(dt_neo_date_time.year, dt_neo_date_time.month, dt_neo_date_time.day,
                             dt_neo_date_time.hour, dt_neo_date_time.minute, int(dt_neo_date_time.second),
                             int(dt_neo_date_time.second * 1000000 % 1000000),
                             tzinfo=dt_neo_date_time.tzinfo)
    return dtOutput


def create_friend_circle_json(record, loutput):

    return True