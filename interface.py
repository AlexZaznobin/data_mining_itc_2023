
import datetime
def get_date_range () :
    """
    get input from user in format DDMMDDMM

    Returns:
        start: first day in suitable range of dates
        days_number: - number of days in suitable range
    """
    incorrect_date = True
    start_point = input("insert date range DDMMDDMM (for April 2023 - 01043004):")
    while incorrect_date :
        try :
            start_date = int(start_point[:2])
            start_month = int(start_point[2 :4])
            end_date = int(start_point[4 :6])
            end_month = int(start_point[6 :])
            start = datetime.date(year=2022, month=start_month, day=start_date)
            end = datetime.date(year=2022, month=end_month, day=end_date)
            days_number = (end - start).days + 1
            incorrect_date = False
        except :
            start_point = input("insert correct date range DDMMDDMM or stop:")
            if start_point == "stop" :
                incorrect_date = False

    return (start, days_number)


def get_airport (airport_df, start_or_end) :
    """
    Args:
        airport_df: A pandas DataFrame containing airport codes.

        start_or_end: Flag - start or end?

    takes input from the user to find out what from/ to what airport she wants to fly

    Returns:
        code if airport, or 'any' flag
    """

    if start_or_end == "end" :
        start_end_point = input(
            f"Type first letters (e.g 'tbil') of the airport you want to {start_or_end},\n \
            or type 'any' for all destinations in the world\n ")
        if start_end_point == "any" :
            return start_end_point
    else :
        start_end_point = input(f"Type first letters (e.g 'ben gu') of the airport you want to {start_or_end}:")
    while start_end_point != "stop" :
        if start_end_point == "" :
            country_code = input("enter country code (two letters capital):")
            see_airports(airport_df, country_code)
            start_end_point = input("input city one more time (or "'stop'"):")
            continue
        filtered_df = airport_df[airport_df['name'].str.contains(start_end_point, case=False)]

        if not filtered_df.empty :
            if filtered_df.shape[0] == 1 :
                print(filtered_df.iloc[0]['name'])
                return filtered_df.iloc[0]['code']
            else :
                print("too many options, choose one airport")
                for i in range(filtered_df.shape[0]) :
                    print('time zone:', filtered_df.iloc[i][['time_zone_id']][0],
                          ', airport', filtered_df.iloc[i][['name']][0])
        else :
            print("String not found. Please enter airport name")
        start_end_point = input("input city one more time (or "'stop'"):")


def see_country_id (airport_df) :
    """
    Print the count of airports per country.

    Args:
        airport_df: A pandas DataFrame containing airport codes.

    Returns:
        None.
    """
    grouped_airport_df = airport_df[['country_id', 'time_zone_id']].groupby('country_id').count()
    print(grouped_airport_df.to_string())


def see_airports (airport_df, country_id) :
    """
    Args:
        airport_df: A pandas DataFrame containing airport codes.

        country_id: country -Two capital letters

    print  all airport for country_id, or call see_country_id() function which print list of country_id

    Returns:
        None.
    """
    if country_id == "" :
        see_country_id(airport_df)
    else :
        print(airport_df[airport_df['country_id'] == country_id][['name']])



