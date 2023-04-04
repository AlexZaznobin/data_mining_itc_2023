import datetime
import argparse
import sys


def get_date_range (input_ddmmddmm=None) :
    """
    get input from user in format DDMMDDMM

    Returns:
        start: first day in suitable range of dates
        days_number: - number of days in suitable range
    """
    incorrect_date = True
    if input_ddmmddmm == None :
        start_point = input("insert date range DDMMDDMM (for May 2023 - 01053005):")
    else :
        start_point = input_ddmmddmm
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


def get_scraping_parameters_list (airport_df) :
    """
    Generates a list of parameters required for web scraping flight data.

    Args:
        airport_df (pd.DataFrame): A DataFrame containing airport information.

    Returns:
        list: A list containing the following parameters:
            - start_aero_code (str): The starting airport's code.
            - start_date (str): The start date of the date range for which the flight data is being scraped.
            - days_number (int): The number of days in the date range for which the flight data is being scraped.
            - end_point (list): A list containing the end airport codes. If 'any', it contains all available airport codes.

    Raises:
        Exception: If there is an error while generating the list of end points.
    """
    start_aero_code = get_airport(airport_df, "start")
    start_date, days_number = get_date_range()
    end_point = [get_airport(airport_df, "end")]
    try :
        if end_point == ["any"] :
            end_point = airport_df['code'].values
    except :
        pass
    return [start_aero_code, start_date, days_number, end_point]


def set_up_parser () :
    """
    Sets up an argument parser to handle command-line arguments for the script.

    Returns:
        tuple: A tuple containing a list with the following items: start airport code, start date, number of days, and a list of endpoint airport codes; and a boolean indicating if a database is needed.
    """

    parser = argparse.ArgumentParser(description="""You can run scrip with the following parameters:
                                             -sac --start_ariport_code 
                                             -eac --end_ariport_code 
                                             -dr --daterange
                                             """)
    parser.add_argument("-sac", "--start_ariport_code", type=str, required=False,
                        help="3 letters of arport code e.g TLV to start your flight")
    parser.add_argument("-eac", "--end_ariport_code", type=str, required=False,
                        help="3 letters of arport code e.g TBS  to end your flight")
    parser.add_argument("-dr", "--daterange", type=str, required=False,
                        help="date range DDMMDDMM (for September 2023 - 01093009")
    parser.add_argument("-db", "--database", action="store_true", help="do we need database (yes/no)")
    args = parser.parse_args()

    if len(sys.argv) == 1 :
        start_aero_code = 'TLV'
        start_date, days_number = get_date_range('17091709')
        end_point = ['SVO', 'TBS','EVN','ALA','BEG','GYD','TAS', 'PEK','JFK', 'SIN', 'HND', 'ICN', 'DOH', 'CDG', 'NRT', 'LHR', 'IST', 'DXB', 'MAD', 'MUC', 'ATL', 'AMS',
        'FCO', 'LGW', 'CPH']
        need_database = True
    else:
        start_aero_code = args.start_ariport_code
        start_date, days_number = get_date_range(args.daterange)
        end_point = args.end_ariport_code
        if type(end_point)==str:
            end_point=[end_point]
        need_database=args.database
        print(need_database)

    return [start_aero_code, start_date, days_number, end_point], need_database
